use crate::models::{AppState, Peer, PROTOCOL_VERSION};
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use std::{sync::Arc, time::Duration};
use tauri::{AppHandle, Emitter};

const SERVICE_TYPE: &str = "_dead-drop._tcp.local.";

pub fn start(state: Arc<AppState>, app: AppHandle) {
    std::thread::spawn(move || {
        let daemon = match ServiceDaemon::new() {
            Ok(daemon) => daemon,
            Err(error) => {
                eprintln!("Dead Drop discovery could not start: {error}");
                return;
            }
        };

        let device = state.device();
        let short_id = &device.id[..8];
        let instance_name = format!("Dead Drop {short_id}");
        let host_name = format!("dead-drop-{short_id}.local.");
        let properties = [
            ("id", device.id.as_str()),
            ("name", device.name.as_str()),
            ("os", device.os.as_str()),
            ("protocol", "1"),
        ];
        let service = match ServiceInfo::new(
            SERVICE_TYPE,
            &instance_name,
            &host_name,
            "",
            state.listener_port(),
            &properties[..],
        ) {
            Ok(service) => service.enable_addr_auto(),
            Err(error) => {
                eprintln!("Dead Drop discovery could not describe this device: {error}");
                return;
            }
        };

        if let Err(error) = daemon.register(service) {
            eprintln!("Dead Drop discovery could not advertise this device: {error}");
            return;
        }

        let receiver = match daemon.browse(SERVICE_TYPE) {
            Ok(receiver) => receiver,
            Err(error) => {
                eprintln!("Dead Drop discovery could not browse for peers: {error}");
                return;
            }
        };

        while let Ok(event) = receiver.recv_timeout(Duration::from_secs(30)) {
            match event {
                ServiceEvent::ServiceResolved(service) => {
                    let Some(id) = service.get_property_val_str("id") else {
                        continue;
                    };
                    if id == state.device().id {
                        continue;
                    }
                    let addresses = service.get_addresses_v4();
                    let Some(address) = addresses.iter().next() else {
                        continue;
                    };
                    let peer = Peer {
                        id: id.to_string(),
                        name: service
                            .get_property_val_str("name")
                            .unwrap_or("Unnamed device")
                            .to_string(),
                        os: service
                            .get_property_val_str("os")
                            .unwrap_or("Unknown OS")
                            .to_string(),
                        endpoint: format!("{}:{}", address, service.get_port()),
                        protocol_version: service
                            .get_property_val_str("protocol")
                            .and_then(|value| value.parse().ok())
                            .unwrap_or(PROTOCOL_VERSION),
                        online: true,
                        service_fullname: service.get_fullname().to_string(),
                    };
                    state.upsert_peer(peer);
                    let _ = app.emit("peers-updated", state.peers());
                }
                ServiceEvent::ServiceRemoved(_, service_fullname) => {
                    state.remove_peer_by_service(&service_fullname);
                    let _ = app.emit("peers-updated", state.peers());
                }
                _ => {}
            }
        }
    });
}
