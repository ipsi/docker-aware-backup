use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::str::FromStr;
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;
use clap::Parser;
use docker_api::conn::TtyChunk;
use docker_api::opts::{ContainerFilter, ContainerListOpts, ContainerStopOpts, ExecCreateOpts, ExecStartOpts, VolumeListOpts};
use docker_api::{Docker, Exec};
use rustic_backend::BackendOptions;
use rustic_core::repofile::SnapshotFile;
use rustic_core::{BackupOptions, IndexedIds, KeepOptions, LocalSourceFilterOptions, NoProgressBars, PathList, PruneOptions, Repository, RepositoryOptions, SnapshotGroupCriterion, SnapshotOptions, StringList};
use serde::Deserialize;
use tokio::time::Instant;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, trace, warn};
use tracing_subscriber::EnvFilter;

const SQLITE_MAGIC_BYTES: &'static [u8; 16] = b"SQLite format 3\0";

#[derive(Debug, Parser)]
#[command(name="backup-rs", bin_name="backup-rs")]
struct Cli {
    /// Path to config file
    #[arg(short, long, env, default_value_os_t = default_config_file_path().into())]
    config_path: PathBuf,
}

fn default_config_file_path() -> OsString {
    let mut home = homedir::my_home()
        .expect("current user must have a home dir")
        .expect("current user must have a home dir");

    home.push(".config");
    home.push("backup-tooling");
    home.push("config.toml");

    home.into_os_string()
}

#[derive(Deserialize, Debug, Clone)]
struct ResticConfig {
    #[serde(default)]
    dry_run: bool,
    repository: String,
    repository_options: BTreeMap<String, String>,
    password_file: PathBuf,
    docker: DockerConfig,
    directories: Vec<DirectoryConfig>,
    volumes: Vec<VolumeConfig>,
}

#[derive(Deserialize, Debug, Clone)]
struct DockerConfig {
    #[serde(default)]
    docker_path: Option<String>,
    tag_volume_names: bool,
    #[serde(default)]
    skip_volumes: Vec<DockerSkipVolumes>,
    #[serde(default)]
    wait_until_running: bool,
    #[serde(default = "default_restart_timeout_secs")]
    restart_timeout_secs: u64,
}

fn default_restart_timeout_secs() -> u64 { 30 }

#[derive(Deserialize, Debug, Clone)]
enum DockerSkipVolumes {
    #[serde(rename = "name")]
    Name(String),
    #[serde(rename = "regex", with = "serde_regex")]
    Regex(regex::Regex),
}

// fn deserialise_regex<'de, D>(d: D) -> Result<regex::Regex, D::Error> where D: Deserializer<'de> {
//     let foo = d.deserialize_str()?;
// }

#[derive(Deserialize, Debug, Clone)]
struct DirectoryConfig {
    basedir: String,
    paths: Option<Vec<String>>,
    excludes: Option<Vec<String>>,
    tags: Vec<String>,
    pre_commands: Option<Vec<Vec<String>>>,
    post_commands: Option<Vec<Vec<String>>>,
}

impl DirectoryConfig {
    fn backup(&self) -> anyhow::Result<()> {
        info!(?self, "backing up directory");
        self.exec_pre_commands()?;

        let default_excludes = Vec::new();
        let excludes = self.excludes.as_ref().unwrap_or(&default_excludes);
        let backup_opts = BackupOptions::default()
            .ignore_filter_opts(LocalSourceFilterOptions::default().globs(excludes.as_slice()));
        let source = if self.paths.as_ref().unwrap_or(&Vec::new()).is_empty() {
            debug!("backing up basedir and subdirs, excluding explicit excludes");
            PathList::from_string(&self.basedir)?.sanitize()?
        } else {
            debug!("backing up explicitly-specified subdirs only, excluding explicit excludes");
            PathList::from_iter(
                self.paths.as_ref()
                    .unwrap_or(&Vec::new())
                    .iter()
                    .map(|p| format!("{}/{p}", self.basedir))
            ).sanitize()?
        };
        debug!("creating snapshot");
        if config().dry_run {
            info!("dry-run enabled, not doing backup");
        } else {
            let snap = SnapshotOptions::default()
                .tags((&self.tags).iter().map(|x| StringList::from_str(x.as_str()).unwrap()).collect::<Vec<StringList>>())
                .to_snapshot()?;

            repository().save_snapshot(backup_opts, source, snap)?;
        }
        debug!("created snapshot");

        self.exec_post_commands()?;

        Ok(())
    }

    fn exec_pre_commands(&self) -> anyhow::Result<()> {
        self.exec_commands(&self.pre_commands)
    }
    fn exec_post_commands(&self) -> anyhow::Result<()> {
        self.exec_commands(&self.post_commands)
    }
    fn exec_commands(&self, commands: &Option<Vec<Vec<String>>>) -> anyhow::Result<()> {
        if let Some(commands) = commands {
            for command_and_args in commands {
                info!(?command_and_args, "executing pre-/post-command");
                if config().dry_run {
                    info!("dry-run mode enabled, not executing command");
                    continue;
                }
                let mut command = Command::new(&command_and_args[0]);
                if command_and_args.len() > 1 {
                    command.args(&command_and_args[1..]);
                }
                command.stdout(Stdio::piped());
                command.stderr(Stdio::piped());
                let mut status = command.spawn()?;
                let mut join_handles = vec![];
                if let Some(child_stdout) = status.stdout.take() {
                    let external_command = command_and_args[0].clone();
                    join_handles.push(thread::spawn(move || {
                        let mut buf_reader = BufReader::new(child_stdout);
                        let mut line = String::new();
                        loop {
                            let size = buf_reader.read_line(&mut line).unwrap();
                            if size == 0 {
                                break;
                            }
                            info!(external_command, "{}", line.trim());
                            line.clear();
                        }
                    }));
                } else {
                    warn!("no stdout");
                }
                if let Some(child_stderr) = status.stderr.take() {
                    let external_command = command_and_args[0].clone();
                    join_handles.push(thread::spawn(move || {
                        let mut buf_reader = BufReader::new(child_stderr);
                        let mut line = String::new();
                        loop {
                            let size = buf_reader.read_line(&mut line).unwrap();
                            if size == 0 {
                                break;
                            }
                            warn!(external_command, "{}", line.trim());
                            line.clear();
                        }
                    }));
                } else {
                    warn!("no stderr");
                }
                if !status.wait()?.success() {
                    error!(?command_and_args, "Pre-/post-command failed");
                    continue;
                } else {
                    debug!("pre-/post-command executed successfully");
                }

                for join_handle in join_handles {
                    join_handle.join().unwrap();
                }
            }
        }
        Ok(())
    }
}

#[derive(Deserialize, Debug, Clone)]
struct VolumeConfig {
    name: String,
    container: String,
    #[serde(default)]
    stop_container: bool,
    command: Option<Vec<String>>,
    output_file: Option<String>,
}

impl VolumeConfig {
    // async fn backup(&self) -> anyhow::Result<Option<(BackupOptions, PathList, SnapshotFile)>> {
    async fn backup(&self) -> anyhow::Result<()> {
        info!(?self, "backing up Docker volume");
        let clo = ContainerListOpts::builder()
            .filter(vec![ContainerFilter::Name(format!("^/{}$", self.container))])
            .build();
        let mut container = docker().containers().list(&clo).await?;
        if container.len() != 1 {
            error!(container=self.container, count=container.len(), "Container not found, or multiple containers found with the same name");
            return Ok(());
        }

        let container = container.remove(0);
        let Some(container_id) = container.id else {
            error!(container=self.container, "Container has no id");
            return Ok(());
        };
        let Some(mounts) = container.mounts else {
            error!(container=self.container, "Container has no mounts");
            return Ok(());
        };
        let Some(volume) = mounts.into_iter().find(|m| matches!(&m.name, Some(name) if name == &self.name)) else {
            error!(container=self.container, volume_name=self.name, "Container has mounts but no matching volume by name");
            return Ok(());
        };
        let Some(source) = volume.source else {
            error!(container=self.container, volume_name=self.name, "Volume has no source");
            return Ok(());
        };

        let mut container_stopped = false;

        if self.stop_container || contains_sqlite_db(&Path::new(&source))? {
            if config().dry_run {
                info!(container_name=self.container, "dry-run mode enabled, not stopping container");
                container_stopped = true;
            } else {
                info!(container_name=self.container, "stopping container");
                let cso = ContainerStopOpts::builder().build();
                docker().containers().get(&container_id).stop(&cso).await?;

                // let start = Instant::now();
                // loop {
                //     if Instant::now().duration_since(start).as_secs() > config().docker.restart_timeout_secs {
                //         return Err(
                //             anyhow::Error::msg(
                //                 format!(
                //                     "container {} failed to stop after {} seconds",
                //                     self.container,
                //                     config().docker.restart_timeout_secs
                //                 )
                //             )
                //         )
                //     }
                // 
                //     if let Some(state) = docker().containers().get(&container_id).inspect().await?.state {
                //         if !state.running.unwrap_or(false) {
                //             info!(container_name=self.container, "container has stopped");
                //             break;
                //         }
                //     }
                // 
                //     tokio::time::sleep(Duration::from_millis(50)).await;
                // }

                container_stopped = true;
            }
        } else {
            info!(container_name=self.container, volume=self.name, "not configured to stop container and no Sqlite DB found");
        }

        let backup_opts = BackupOptions::default();
        let mut tags = Vec::new();
        if config().docker.tag_volume_names {
            tags.push(self.name.clone());
        }
        tags.push("docker".to_string());
        let snap = SnapshotOptions::default()
            .tags(tags.iter().map(|x| StringList::from_str(x.as_str()).unwrap()).collect::<Vec<StringList>>())
            .to_snapshot()?;

        if let Some(command) = &self.command {
            info!(container_name=self.container, ?command, "executing command on Docker volume to create data for snapshot");

            let Some(filename) = &self.output_file else {
                return Err(anyhow::Error::msg("filename must be provided when backing up with a command"));
            };

            if config().dry_run {
                info!(container_name=self.container, "dry-run mode enabled, not executing command on container");
                return Ok(());
            }

            let external_command = command[0].clone();
            let exec_create_opts = ExecCreateOpts::builder()
                .command(command)
                .attach_stderr(true)
                .attach_stdout(true)
                .tty(true)
                .build();
            let exec_start_opts = ExecStartOpts::builder().tty(true).build();
            let exec = Exec::create(docker().clone(), container_id.clone(), &exec_create_opts).await?;
            let mut multiplexer = exec.start(&exec_start_opts).await?;
            let external_command_c = (&external_command).clone();
            tokio::spawn(async move {
                loop {
                    let value = multiplexer.next().await;
                    if let Some(Ok(chunk)) = value {
                        match chunk {
                            TtyChunk::StdIn(_) => panic!("should not receive stdin chunks from exec"),
                            TtyChunk::StdOut(out) => info!(external_command=external_command_c, "{}", String::from_utf8_lossy(&out)),
                            TtyChunk::StdErr(err) => warn!(external_command=external_command_c, "{}", String::from_utf8_lossy(&err)),
                        };
                    } else if let None = value {
                        break;
                    } else {
                        error!(external_command_c, ?value, "unexpected error reading chunk from exec");
                        break;
                    }
                }
            });

            loop {
                trace!("fetching exec status");
                let exec_status = exec.inspect().await?;
                trace!(?exec_status, "checking exec status");
                if matches!(exec_status.running, Some(false)) {
                    if let Some(exit_code) = exec_status.exit_code {
                        if exit_code != 0 {
                            error!(external_command, exit_code, "command exited with non-zero exit code");
                        }
                    } else {
                        error!(external_command, "command exited without exit code");
                    }
                    break;
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            }

            let path = format!("{source}/{filename}");

            info!(container_name=self.container, path, "backing up data created by command on Docker volume to snapshot");

            if config().dry_run {
                info!(container_name=self.container, path, "dry-run enabled, not creating snapshot");
            } else {
                repository().save_snapshot(backup_opts, PathList::from_string(&path)?.sanitize()?, snap)?;
            }
        } else {
            info!(container_name=self.container, volume=self.name, "backing up entire Docker volume to snapshot");
            let source = PathList::from_string(&source)?.sanitize()?;

            if config().dry_run {
                info!(container_name=self.container, volume=self.name, "dry-run enabled, not creating snapshot");
            } else {
                repository().save_snapshot(backup_opts, source, snap)?;
            }
        }

        if container_stopped {
            if config().dry_run {
                info!(container_name=self.container, "dry-run mode enabled, not starting container as was never stopped");
            } else {
                info!(container_name=self.container, "starting container");
                docker().containers().get(&container_id).start().await?;
                if config().docker.wait_until_running {
                    info!(container_name=self.container, "waiting until container signals it is running");
                    let start = Instant::now();
                    loop {
                        if Instant::now().duration_since(start).as_secs() > config().docker.restart_timeout_secs {
                            return Err(
                                anyhow::Error::msg(
                                    format!(
                                        "container {} failed to start after {} seconds",
                                        self.container,
                                        config().docker.restart_timeout_secs
                                    )
                                )
                            )
                        }

                        if let Some(state) = docker().containers().get(&container_id).inspect().await?.state {
                            if state.running.unwrap_or(false) {
                                info!(container_name=self.container, "container is up and running");
                                break;
                            }
                        }

                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        }

        Ok(())
    }
}

fn config() -> &'static ResticConfig {
    static CONFIG: OnceLock<ResticConfig> = OnceLock::new();

    CONFIG.get_or_init(|| {
        let file = OpenOptions::new()
            .read(true)
            .open(Cli::parse().config_path)
            .expect("must be able to open config file for reading");
        let mut config_file_reader = BufReader::new(file);
        let mut config_file_str = String::new();

        config_file_reader
            .read_to_string(&mut config_file_str)
            .expect("must be able to read file to String");

        toml::from_str(&config_file_str)
            .expect("config must be valid TOML")
    })
}

#[cfg(windows)]
fn docker() -> &'static Docker {
    static DOCKER: OnceLock<Docker> = OnceLock::new();

    DOCKER.get_or_init(|| {
        Docker::tcp(
            config()
                .docker
                .docker_path
                .as_ref()
                .map(|s| s.as_str())
                .unwrap_or("tcp://127.0.0.1:80")
        ).expect("docker on windows should connect via TCP")
    })
}

#[cfg(not(windows))]
fn docker() -> &'static Docker {
    static DOCKER: OnceLock<Docker> = OnceLock::new();

    DOCKER.get_or_init(|| {
        Docker::unix(
            config()
                .docker
                .docker_path
                .as_ref()
                .map(|s| s.as_str())
                .unwrap_or("/var/run/docker.sock")
        )
    })
}

trait RepositoryHandlerTrait {
    fn save_snapshot(&self, backup_opts: BackupOptions, path_list: PathList, snapshot_file: SnapshotFile) -> anyhow::Result<()>;

    fn forget_and_prune(&self) -> anyhow::Result<()>;
}

struct RepositoryHandler2<T: IndexedIds> {
    repository: Repository<NoProgressBars, T>,
}

impl<T: IndexedIds> RepositoryHandlerTrait for RepositoryHandler2<T> {
    fn save_snapshot(&self, backup_opts: BackupOptions, path_list: PathList, snapshot_file: SnapshotFile) -> anyhow::Result<()> {
        info!(?path_list, "creating snapshot");
        let new_snap = self.repository.backup(&backup_opts, &path_list, snapshot_file)?;
        if let Some(summary) = new_snap.summary {
            info!(
                    files_new=summary.files_new,
                    files_changed=summary.files_changed,
                    files_unmodified=summary.files_unmodified,
                    total_files_processed=summary.total_files_processed,
                    total_bytes_processed=summary.total_bytes_processed,
                    dirs_new=summary.dirs_new,
                    dirs_changed=summary.dirs_changed,
                    dirs_unmodified=summary.dirs_unmodified,
                    total_dirs_processed=summary.total_dirs_processed,
                    total_dirsize_processed=summary.total_dirsize_processed,
                    data_blobs=summary.data_blobs,
                    tree_blobs=summary.tree_blobs,
                    data_added=summary.data_added,
                    data_added_packed=summary.data_added_packed,
                    data_added_files=summary.data_added_files,
                    data_added_files_packed=summary.data_added_files_packed,
                    data_added_trees=summary.data_added_trees,
                    data_added_trees_packed=summary.data_added_trees_packed,
                    command=summary.command,
                    backup_start=summary.backup_start.to_rfc3339(),
                    backup_end=summary.backup_end.to_rfc3339(),
                    backup_duration=summary.backup_duration,
                    total_duration=summary.total_duration,
                    parent=?new_snap.parent,
                    "snapshot created, sending ack"
                );
        } else {
            info!("created snapshot, sending ack");
        }

        Ok(())
    }

    fn forget_and_prune(&self) -> anyhow::Result<()> {
        let mut keep_plan = KeepOptions::default();
        keep_plan.keep_within = Some("6M".parse()?);
        let group_by = SnapshotGroupCriterion::default();
        info!(keep_plan=?keep_plan, group_by=?group_by, "forgetting about old snapshots");
        let snapshots = self.repository.get_forget_snapshots(&keep_plan, group_by, |_file| { true })?;
        let ids = snapshots.into_forget_ids();
        info!(snapshot_count=ids.len(), "deleting old snapshots from repository");

        if config().dry_run {
            info!("dry-run enabled, not deleting snapshots nor pruning data")
        } else {
            self.repository.delete_snapshots(&ids)?;
            info!("deleted all old snapshots from repository");

            let prune_opts = PruneOptions::default();
            let prune_plan = self.repository.prune_plan(&prune_opts)?;
            info!(prune_opts=?prune_opts, "pruning old data from repository");
            self.repository.prune(&prune_opts, prune_plan)?;
            info!("pruned old data from repository");
        }

        Ok(())
    }
}

fn repository() -> &'static dyn RepositoryHandlerTrait {
    static REPO_HANDLER: OnceLock<Box<dyn RepositoryHandlerTrait + Sync + Send>> = OnceLock::new();

    REPO_HANDLER.get_or_init(|| {
        info!(repository=config().repository, repository_options=?config().repository_options, "creating backends");
        let backends = BackendOptions::default()
            .repository(&config().repository)
            .options(config().repository_options.clone())
            .to_backends().unwrap();

        info!("creating repository");
        let repo_opts = RepositoryOptions::default().password_file(&config().password_file);
        let repo = Repository::new(&repo_opts, &backends).unwrap()
            .open().unwrap()
            .to_indexed_ids().unwrap();

        info!("created repository");

        Box::new(
            RepositoryHandler2 {
                repository: repo,
            }
        )
    }).as_ref()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .pretty()
        .init();

    let config = config();

    info!("backing up directories");
    for directory in &config.directories {
        directory.backup()?;
    }

    info!("backing up Docker volumes");
    let mut result = BTreeMap::<String, Vec<&VolumeConfig>>::new();
    for v in &config.volumes {
        result.entry(v.container.clone()).or_default().push(v);
    }
    // let mut result = BTreeMap::<String, Vec<&VolumeConfig>>::new();
    // for v in &config.volumes {
    //     result.entry(v.container.clone()).or_default().push(v);
    //     let volume_id = find_volume_id(&v.name).await?;
    //     result.entry(volume_id.clone()).or_default().push(VolumeDeets { id, name, container_ids });
    // }
    // let mut containers = BTreeMap::<String, Vec<&VolumeConfig>>::new();
    // for v in &config.volumes {
    //     let container_id = find_container_id(&v.container).await?;
    //     containers.entry(container_id.clone()).or_default().push(ContainerDeets { container_id, name, volume_ids })
    // }

    for (k, v) in result {
        info!(container_name=k, "backing up Docker volumes for container");
        let mut container_stopped = false;

        // if v.stop_container || contains_sqlite_db(&Path::new(&source))? {
        //     if crate::config().dry_run {
        //         info!(container_name=self.container, "dry-run mode enabled, not stopping container");
        //         container_stopped = true;
        //     } else {
        //         info!(container_name=self.container, "stopping container");
        //         let cso = ContainerStopOpts::builder().build();
        //         docker().containers().get(&container_id).stop(&cso).await?;
        //         container_stopped = true;
        //     }
        // } else {
        //     info!(container_name=self.container, volume=self.name, "not configured to stop container and no Sqlite DB found");
        // }

        for volume_config in v {
            volume_config.backup().await?;
        }

        // if container_stopped {
        //     if crate::config().dry_run {
        //         info!(container_name=k, "dry-run mode enabled, not starting container as was never stopped");
        //     } else {
        //         info!(container_name=k, "starting container");
        //         docker().containers().get(&container_id).start().await?;
        //     }
        // }
    }

    let configured_volumes: Vec<_> = config.volumes.iter().map(|v| &v.name).collect();
    if let Some(volumes) = docker().volumes().list(&VolumeListOpts::default()).await?.volumes {
        for v in volumes {
            if !configured_volumes.contains(&&v.name) && config.docker.skip_volumes.iter().find(|i| {
                match i {
                    DockerSkipVolumes::Name(n) => n == &v.name,
                    DockerSkipVolumes::Regex(r) => r.is_match(&v.name),
                }
            }).is_none() {
                warn!(name=v.name, "found volume not explicitly configured to be backed up");
            }
        }
    }

    repository().forget_and_prune()?;

    Ok(())
}

fn contains_sqlite_db(path: &Path) -> anyhow::Result<bool> {
    let dir = walkdir::WalkDir::new(path);
    let mut sqlite_magic = [0u8; 16];
    for entry in dir {
        let entry = entry?;
        let entry_name = entry.file_name().to_string_lossy();
        if entry.file_type().is_file() && entry_name.ends_with(".db") || entry_name.ends_with(".sqlite") {
            let mut reader = BufReader::new(File::open(entry.path())?);
            let bytes_read = reader.read(&mut sqlite_magic)?;
            if bytes_read == 16 && &sqlite_magic == SQLITE_MAGIC_BYTES {
                warn!(path=?entry.path(), "found a Sqlite DB file in volume");
                return Ok(true)
            } else {
                debug!(path=?entry.path(), "Found a .db or .sqlite file, but header does not match");
            }
        }
    }

    Ok(false)
}