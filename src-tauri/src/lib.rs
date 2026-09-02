mod discovery;
mod models;
mod protocol;
mod transfer;

use models::{AppState, Preferences, PreferencesDraft, StartupSnapshot};
use std::{net::TcpListener as StdTcpListener, sync::Arc};
use tauri::{Manager, State};

#[tauri::command]
fn initial_state(state: State<'_, Arc<AppState>>) -> StartupSnapshot {
    state.startup_snapshot()
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
        .ok_or_else(|| "That device is no longer available.".to_string())?;
    if peer.protocol_version != models::PROTOCOL_VERSION {
        return Err("That device uses a different Dead Drop protocol version.".to_string());
    }
    if paths.is_empty() {
        return Err("Choose at least one file to send.".to_string());
    }
    let transfer_id = uuid::Uuid::new_v4().to_string();
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
fn cancel_transfer(state: State<'_, Arc<AppState>>, transfer_id: String) -> Result<(), String> {
    state.cancel_transfer(&transfer_id)
}

pub fn run() {
    let std_listener = StdTcpListener::bind(("0.0.0.0", 0))
        .expect("Dead Drop could not reserve a local transfer port");
    std_listener
        .set_nonblocking(true)
        .expect("Dead Drop could not configure its local transfer port");
    let listener_port = std_listener
        .local_addr()
        .expect("Dead Drop could not read its local transfer port")
        .port();
    let state = Arc::new(AppState::load(listener_port));

    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .manage(state)
        .setup(move |app| {
            let app_handle = app.handle().clone();
            let state = app.state::<Arc<AppState>>().inner().clone();
            let listener = tokio::net::TcpListener::from_std(std_listener)
                .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })?;
            transfer::start_listener(listener, state.clone(), app_handle.clone());
            discovery::start(state, app_handle);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            initial_state,
            update_preferences,
            respond_to_incoming,
            send_files,
            cancel_transfer
        ])
        .run(tauri::generate_context!())
        .expect("error while running Dead Drop");
}
