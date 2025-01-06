/*
 *     Copyright 2025 The Dragonfly Authors
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

use bytesize::ByteSize;
use dashmap::DashMap;
use dragonfly_api::common::v2::{Host, Network};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::Result as ClientResult;
use pnet::datalink;
use pnet::ipnetwork::IpNetwork;
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use sysinfo::Networks;
use tokio::time::sleep;
use tracing::{debug, instrument};

pub const DEFAULT_NETWORK_BANDWIDTH: ByteSize = ByteSize::gb(10);

pub struct NetworkInfo {
    /// ip for interface
    ip_segments: DashMap<String, Vec<IpNetwork>>,

    /// bandwidth for interface
    bandwidth: DashMap<String, ByteSize>,

    /// available capacity for interface
    available_bandwidth: DashMap<String, ByteSize>,
}

/// HostInfo represents a local host info manager.
pub struct HostInfo {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// network is the network info of the host.
    network: Arc<NetworkInfo>,
}

/// HostInfo implements the local host info manager.
impl HostInfo {
    /// new returns a new HostInfo.
    pub fn new(config: Arc<Config>) -> ClientResult<Self> {
        Ok(Self {
            config: config.clone(),
            network: Arc::new(NetworkInfo {
                ip_segments: Default::default(),
                bandwidth: Default::default(),
                available_bandwidth: Default::default(),
            }),
        })
    }

    /// run start a loop to get local host info.
    #[instrument(skip_all)]
    pub async fn run(&self) {
        // Get network basic information.
        self.init_network_info();

        // Clone the sync_interval.
        let sync_interval = self.config.clone().download.parent_selector.sync_interval;

        // Clone network info.
        let network = self.network.clone();
        let bandwidth = network.bandwidth.clone();

        // Start the host info update loop.
        let mut networks = Networks::new_with_refreshed_list();
        let mut last_refresh_time = SystemTime::now();
        loop {
            // Sleep
            sleep(sync_interval).await;

            // Refresh network information.
            networks.refresh();
            let new_time = SystemTime::now();
            let interval = new_time
                .duration_since(last_refresh_time)
                .unwrap()
                .as_millis() as u64;
            last_refresh_time = new_time;

            // Insert into network info.
            for (interface, data) in &networks {
                let capacity = match bandwidth.get(interface) {
                    None => DEFAULT_NETWORK_BANDWIDTH.as_u64(),
                    Some(cap) => cap.as_u64(),
                };
                network.available_bandwidth.insert(
                    interface.clone(),
                    ByteSize(capacity - data.transmitted() * 1000 / interval),
                );
                debug!(
                    "refresh interface {} available bandwidth to {}",
                    interface,
                    ByteSize(capacity - data.transmitted() * 1000 / interval)
                );
            }
        }
    }

    /// init_network get basic information of local network.
    #[instrument(skip_all)]
    fn init_network_info(&self) {
        // Clone network info.
        let network = self.network.clone();

        // Get network bandwidth for all interfaces.
        let path = "/sys/class/net";
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                // Get file name.
                if let Ok(name) = entry.file_name().into_string() {
                    // Read interface speed from `/sys/class/net/{interface}/speed`
                    let speed_path = format!("/sys/class/net/{}/speed", name);
                    let content = fs::read_to_string(speed_path).unwrap_or("10000".to_string());

                    if let Ok(speed) = content.trim().parse::<u64>() {
                        // Convert Mib/Sec to bit/Sec & Insert into network info.
                        network.bandwidth.insert(name.clone(), ByteSize::mb(speed));
                        debug!("interface {} bandwidth is {}", &name, ByteSize::mb(speed));
                    };
                }
            }
        }

        // Get network ip for all interfaces.
        let interfaces = datalink::interfaces();
        for interface in interfaces {
            network
                .ip_segments
                .insert(interface.name.clone(), interface.ips.clone());
        }
    }

    /// get_host_info get information of host.
    #[instrument(skip_all)]
    pub fn get_host_info(&self, ip: Option<SocketAddr>) -> Result<Host, String> {
        // Init host.
        let mut host = Host::default();

        // Get network information by request ip
        let mut network = Network::default();
        let network_info = self.network.clone();
        if let Some(ip) = ip {
            for interface in network_info.ip_segments.iter() {
                // Check if segments contain this ip.
                let segment = interface.iter().find(|&&x| x.contains(ip.ip()));
                if segment.is_some() {
                    if let Some(ab) = network_info.available_bandwidth.get(interface.key()) {
                        network.upload_rate = ab.value().as_u64();
                    }
                    break;
                }
            }
        };

        // Return host
        host.network = Some(network);
        Ok(host)
    }

    /// get_interval return parent selector sync_interval.
    #[instrument(skip_all)]
    pub fn get_interval(&self) -> Duration {
        self.config.download.parent_selector.sync_interval
    }
}
