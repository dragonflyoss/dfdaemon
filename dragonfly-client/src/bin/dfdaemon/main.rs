/*
 *     Copyright 2023 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use clap::Parser;
use dragonfly_client::announcer::{ManagerAnnouncer, SchedulerAnnouncer};
use dragonfly_client::dynconfig::Dynconfig;
use dragonfly_client::gc::GC;
use dragonfly_client::grpc::{
    dfdaemon_download::DfdaemonDownloadServer, dfdaemon_upload::DfdaemonUploadServer,
    manager::ManagerClient, scheduler::SchedulerClient,
};
use dragonfly_client::health::Health;
use dragonfly_client::metrics::Metrics;
use dragonfly_client::proxy::Proxy;
use dragonfly_client::resource::{persistent_cache_task::PersistentCacheTask, task::Task};
use dragonfly_client::shutdown;
use dragonfly_client::stats::Stats;
use dragonfly_client::tracing::init_tracing;
use dragonfly_client_backend::BackendFactory;
use dragonfly_client_config::dfdaemon;
use dragonfly_client_config::VersionValueParser;
use dragonfly_client_storage::Storage;
use dragonfly_client_util::id_generator::IDGenerator;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use termion::{color, style};
use tokio::sync::mpsc;
use tokio::sync::Barrier;
use tracing::{error, info, Level};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

#[derive(Debug, Parser)]
#[command(
    name = dfdaemon::NAME,
    author,
    version,
    about = "dfdaemon is a high performance P2P download daemon",
    long_about = "A high performance P2P download daemon in Dragonfly that can download resources of different protocols. \
    When user triggers a file downloading task, dfdaemon will download the pieces of file from other peers. \
    Meanwhile, it will act as an uploader to support other peers to download pieces from it if it owns them.",
    disable_version_flag = true
)]
struct Args {
    #[arg(
        short = 'c',
        long = "config",
        default_value_os_t = dfdaemon::default_dfdaemon_config_path(),
        help = "Specify config file to use")
    ]
    config: PathBuf,

    #[arg(
        short = 'l',
        long,
        default_value = "info",
        help = "Specify the logging level [trace, debug, info, warn, error]"
    )]
    log_level: Level,

    #[arg(
        long,
        default_value_os_t = dfdaemon::default_dfdaemon_log_dir(),
        help = "Specify the log directory"
    )]
    log_dir: PathBuf,

    #[arg(
        long,
        default_value_t = 6,
        help = "Specify the max number of log files"
    )]
    log_max_files: usize,

    #[arg(
        long = "verbose",
        default_value_t = true,
        help = "Specify whether to print log"
    )]
    verbose: bool,

    #[arg(
        short = 'V',
        long = "version",
        help = "Print version information",
        default_value_t = false,
        action = clap::ArgAction::SetTrue,
        value_parser = VersionValueParser
    )]
    version: bool,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Parse command line arguments.
    let args = Args::parse();

    // Load config.
    let config = match dfdaemon::Config::load(&args.config).await {
        Ok(config) => config,
        Err(err) => {
            println!(
                "{}{}Load config {} error: {}{}\n",
                color::Fg(color::Red),
                style::Bold,
                args.config.display(),
                err,
                style::Reset
            );

            println!(
                "{}{}If the file does not exist, you need to new a default config file refer to: {}{}{}{}https://d7y.io/docs/next/reference/configuration/client/dfdaemon/{}",
                color::Fg(color::Yellow),
                style::Bold,
                style::Reset,
                color::Fg(color::Cyan),
                style::Underline,
                style::Italic,
                style::Reset,
            );
            std::process::exit(1);
        }
    };

    let config = Arc::new(config);

    // Initialize tracing.
    let _guards = init_tracing(
        dfdaemon::NAME,
        args.log_dir.clone(),
        args.log_level,
        args.log_max_files,
        config.tracing.addr.to_owned(),
        config.tracing.flamegraph,
        args.verbose,
    );

    // Initialize storage.
    let storage = Storage::new(config.clone(), config.storage.dir.as_path(), args.log_dir)
        .await
        .inspect_err(|err| {
            error!("initialize storage failed: {}", err);
        })?;
    let storage = Arc::new(storage);

    // Initialize id generator.
    let id_generator = IDGenerator::new(
        config.host.ip.unwrap().to_string(),
        config.host.hostname.clone(),
        config.seed_peer.enable,
    );
    let id_generator = Arc::new(id_generator);

    // Initialize manager client.
    let manager_client = ManagerClient::new(config.clone(), config.manager.addr.clone())
        .await
        .inspect_err(|err| {
            error!("initialize manager client failed: {}", err);
        })?;
    let manager_client = Arc::new(manager_client);

    // Initialize channel for graceful shutdown.
    let shutdown = shutdown::Shutdown::default();
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::unbounded_channel();

    // Initialize dynconfig server.
    let dynconfig = Dynconfig::new(
        config.clone(),
        manager_client.clone(),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    )
    .await
    .inspect_err(|err| {
        error!("initialize dynconfig server failed: {}", err);
    })?;
    let dynconfig = Arc::new(dynconfig);

    // Initialize scheduler client.
    let scheduler_client = SchedulerClient::new(config.clone(), dynconfig.clone())
        .await
        .inspect_err(|err| {
            error!("initialize scheduler client failed: {}", err);
        })?;
    let scheduler_client = Arc::new(scheduler_client);

    let backend_factory = BackendFactory::new(Some(config.server.plugin_dir.as_path()))
        .inspect_err(|err| {
            error!("initialize backend factory failed: {}", err);
        })?;
    let backend_factory = Arc::new(backend_factory);

    // Initialize task manager.
    let task = Task::new(
        config.clone(),
        id_generator.clone(),
        storage.clone(),
        scheduler_client.clone(),
        backend_factory.clone(),
    )?;
    let task = Arc::new(task);

    // Initialize persistent cache task manager.
    let persistent_cache_task = PersistentCacheTask::new(
        config.clone(),
        id_generator.clone(),
        storage.clone(),
        scheduler_client.clone(),
        backend_factory.clone(),
    )?;
    let persistent_cache_task = Arc::new(persistent_cache_task);

    // Initialize health server.
    let health = Health::new(
        SocketAddr::new(config.health.server.ip.unwrap(), config.health.server.port),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    // Initialize metrics server.
    let metrics = Metrics::new(
        config.clone(),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    // Initialize stats server.
    let stats = Stats::new(
        SocketAddr::new(config.stats.server.ip.unwrap(), config.stats.server.port),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    // Initialize proxy server.
    let proxy = Proxy::new(
        config.clone(),
        task.clone(),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    // Initialize manager announcer.
    let manager_announcer = ManagerAnnouncer::new(
        config.clone(),
        manager_client.clone(),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    // Initialize scheduler announcer.
    let scheduler_announcer = SchedulerAnnouncer::new(
        config.clone(),
        id_generator.host_id(),
        scheduler_client.clone(),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    )
    .await
    .inspect_err(|err| {
        error!("initialize scheduler announcer failed: {}", err);
    })?;

    // Initialize upload grpc server.
    let mut dfdaemon_upload_grpc = DfdaemonUploadServer::new(
        config.clone(),
        SocketAddr::new(config.upload.server.ip.unwrap(), config.upload.server.port),
        task.clone(),
        persistent_cache_task.clone(),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    // Initialize download grpc server.
    let mut dfdaemon_download_grpc = DfdaemonDownloadServer::new(
        config.download.server.socket_path.clone(),
        task.clone(),
        persistent_cache_task.clone(),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    // Initialize garbage collector.
    let gc = GC::new(
        config.clone(),
        id_generator.host_id(),
        storage.clone(),
        scheduler_client.clone(),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    // Log dfdaemon started pid.
    info!("dfdaemon started at pid {}", std::process::id());

    // grpc server started barrier.
    let grpc_server_started_barrier = Arc::new(Barrier::new(3));

    // Wait for servers to exit or shutdown signal.
    tokio::select! {
        _ = tokio::spawn(async move { dynconfig.run().await }) => {
            info!("dynconfig manager exited");
        },

        _ = tokio::spawn(async move { health.run().await }) => {
            info!("health server exited");
        },

        _ = tokio::spawn(async move { metrics.run().await }) => {
            info!("metrics server exited");
        },

        _ = tokio::spawn(async move { stats.run().await }) => {
            info!("stats server exited");
        },

        _ = tokio::spawn(async move { manager_announcer.run().await.unwrap_or_else(|err| error!("announcer manager failed: {}", err))} ) => {
            info!("announcer manager exited");
        },

        _ = tokio::spawn(async move { scheduler_announcer.run().await }) => {
            info!("announcer scheduler exited");
        },

        _ = tokio::spawn(async move { gc.run().await }) => {
            info!("garbage collector exited");
        },

        _ = {
            let barrier = grpc_server_started_barrier.clone();
            tokio::spawn(async move {
                dfdaemon_upload_grpc.run(barrier).await.unwrap_or_else(|err| error!("dfdaemon upload grpc server failed: {}", err));
            })
        } => {
            info!("dfdaemon upload grpc server exited");
        },

        _ = {
            let barrier = grpc_server_started_barrier.clone();
            tokio::spawn(async move {
                dfdaemon_download_grpc.run(barrier).await.unwrap_or_else(|err| error!("dfdaemon download grpc server failed: {}", err));
            })
        } => {
            info!("dfdaemon download grpc unix server exited");
        },

        _ = {
            let barrier = grpc_server_started_barrier.clone();
            tokio::spawn(async move {
                proxy.run(barrier).await.unwrap_or_else(|err| error!("proxy server failed: {}", err));
            })
        } => {
            info!("proxy server exited");
        },

        _ = shutdown::shutdown_signal() => {},
    }

    // Trigger shutdown signal to other servers.
    shutdown.trigger();

    // Drop task to release scheduler_client. when drop the task, it will release the Arc reference
    // of scheduler_client, so scheduler_client can be released normally.
    drop(task);

    // Drop persistent cache task to release scheduler_client. when drop the persistent cache task, it will release the Arc reference
    // of scheduler_client, so scheduler_client can be released normally.
    drop(persistent_cache_task);

    // Drop scheduler_client to release dynconfig. when drop the scheduler_client, it will release the
    // Arc reference of dynconfig, so dynconfig can be released normally.
    drop(scheduler_client);

    // Drop shutdown_complete_rx to wait for the other server to exit.
    drop(shutdown_complete_tx);

    // Wait for the other server to exit.
    let _ = shutdown_complete_rx.recv().await;

    Ok(())
}
