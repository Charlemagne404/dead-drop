mod connectivity;
mod diagnostics;
mod discovery;
mod models;
mod peer;
mod platform;
mod protocol;
mod routing;
#[cfg(any(test, feature = "integration-tests"))]
pub mod test_support;
mod transfer;

use diagnostics::{LogCategory, LogLevel, SupportLogger};
use models::{AppState, PeerSnapshot, Preferences, PreferencesDraft, StartupSnapshot};
use std::{error::Error, net::TcpListener as StdTcpListener, sync::Arc};
use tauri::{Emitter, State};

#[tauri::command]
fn initial_state(state: State<'_, Arc<AppState>>) -> StartupSnapshot {
    state.startup_snapshot()
}

#[tauri::command]
fn diagnostics_report(state: State<'_, Arc<AppState>>) -> String {
    state.diagnostics_report()
}

#[tauri::command]
fn update_preferences(
    state: State<'_, Arc<AppState>>,
    draft: PreferencesDraft,
) -> Result<Preferences, String> {
    state.update_preferences(draft)
}

#[tauri::command]
fn respond_to_incoming(
    state: State<'_, Arc<AppState>>,
    transfer_id: String,
    accepted: bool,
) -> Result<(), String> {
    state.resolve_pending_request(&transfer_id, accepted)
}

#[tauri::command]
async fn send_files(
    app: tauri::AppHandle,
    state: State<'_, Arc<AppState>>,
    peer_id: String,
    paths: Vec<String>,
) -> Result<String, String> {
    let peer = state
        .peer(&peer_id)
        .ok_or_else(|| "Device went offline.".to_string())?;
    if !peer.is_online() {
        return Err("Device went offline.".to_string());
    }
    if peer.protocol_version != models::PROTOCOL_VERSION {
        return Err("That device uses a different Drop protocol version.".to_string());
    }
    if paths.is_empty() {
        return Err("Choose at least one file to send.".to_string());
    }
    if paths.len() > models::MAX_TRANSFER_FILES {
        return Err(format!(
            "Choose no more than {} files at a time.",
            models::MAX_TRANSFER_FILES
        ));
    }
    let transfer_id = uuid::Uuid::new_v4().to_string();
    state.try_begin_transfer(&transfer_id)?;
    let cancellation = state.register_cancellation(transfer_id.clone());
    tauri::async_runtime::spawn(transfer::run_outgoing(
        app,
        state.inner().clone(),
        transfer_id.clone(),
        peer,
        paths,
        cancellation,
    ));
    Ok(transfer_id)
}

#[tauri::command]
async fn connect_by_address(
    app: tauri::AppHandle,
    state: State<'_, Arc<AppState>>,
    address: String,
) -> Result<PeerSnapshot, String> {
    let endpoints = match connectivity::resolve_manual_target(&address).await {
        Ok(endpoints) => endpoints,
        Err(error) => {
            state.log(
                LogLevel::Warn,
                LogCategory::Connection,
                "manual_target_rejected",
                Some(&error),
            );
            return Err(error);
        }
    };
    let local = state.device();
    let shutdown = state.shutdown_token();
    let cancellation = models::Cancellation::new();
    let mut last_error = None;
    for endpoint in endpoints {
        match connectivity::connect_and_identify(
            endpoint,
            &local,
            None,
            &cancellation,
            shutdown.as_ref(),
        )
        .await
        {
            Ok(connection) => {
                let mut discovered = peer::Endpoint::new(
                    endpoint,
                    peer::EndpointSource::new("manual", "tcp", endpoint.to_string()),
                    peer::RouteClass::Other,
                    std::time::Instant::now(),
                );
                discovered.reachability = peer::EndpointReachability::Reachable;
                let peer_id = connection.identity.id.clone();
                state.apply_discovery_observation(peer::DiscoveryObservation {
                    identity: connection.identity.clone(),
                    source: discovered.source.clone(),
                    endpoints: vec![discovered],
                });
                state.record_route_success(&peer_id, endpoint);
                state.remember_peer(&connection.identity, endpoint);
                let peer = state
                    .peers()
                    .into_iter()
                    .find(|peer| peer.id == peer_id)
                    .ok_or_else(|| "Couldn't add that device.".to_string())?;
                let _ = app.emit("peers-updated", state.peers());
                let _ = app.emit("connectivity-diagnostics", state.runtime_diagnostics());
                return Ok(peer);
            }
            Err(error) => {
                state.log(
                    LogLevel::Warn,
                    LogCategory::Connection,
                    "manual_connection_failed",
                    Some(&format!(
                        "endpoint={} reason={}",
                        endpoint,
                        error.diagnostic_message()
                    )),
                );
                last_error = Some(error);
            }
        }
    }
    Err(last_error
        .map(|error| error.user_message().to_string())
        .unwrap_or_else(|| "Couldn't connect to that device.".to_string()))
}

#[tauri::command]
fn cancel_transfer(state: State<'_, Arc<AppState>>, transfer_id: String) -> Result<(), String> {
    state.cancel_transfer(&transfer_id)
}

pub fn run() {
    if let Err(error) = run_inner() {
        let logger = SupportLogger::persistent(platform::log_path());
        logger.record(
            LogLevel::Error,
            LogCategory::Startup,
            "application_start_failed",
            Some(&error.to_string()),
        );
        eprintln!("[dead-drop] Drop could not start.");
    }
}

fn run_inner() -> Result<(), Box<dyn Error>> {
    let std_listener = StdTcpListener::bind(("0.0.0.0", connectivity::DROP_SERVICE_PORT))?;
    std_listener.set_nonblocking(true)?;
    let listener_port = std_listener.local_addr()?.port();
    let state = Arc::new(AppState::load(listener_port));
    let setup_state = state.clone();
    let shutdown_state = state.clone();

    let app = tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .manage(state)
        .setup(move |app| {
            let app_handle = app.handle().clone();
            let state = setup_state.clone();
            transfer::start_listener(std_listener, state.clone(), app_handle.clone());
            discovery::start(state, app_handle);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            initial_state,
            update_preferences,
            respond_to_incoming,
            send_files,
            connect_by_address,
            cancel_transfer,
            diagnostics_report
        ])
        .build(tauri::generate_context!())?;
    app.run(move |_app_handle, event| {
        if matches!(
            event,
            tauri::RunEvent::ExitRequested { .. } | tauri::RunEvent::Exit
        ) {
            shutdown_state.shutdown();
        }
    });
    Ok(())
}
