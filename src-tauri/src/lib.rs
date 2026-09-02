mod discovery;
mod models;
mod protocol;
mod transfer;

use models::{AppState, Preferences, PreferencesDraft, StartupSnapshot};
use std::{error::Error, net::TcpListener as StdTcpListener, sync::Arc};
use tauri::State;

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
    if !peer.online {
        return Err("That device is no longer available.".to_string());
    }
    if peer.protocol_version != models::PROTOCOL_VERSION {
        return Err("That device uses a different Dead Drop protocol version.".to_string());
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
fn cancel_transfer(state: State<'_, Arc<AppState>>, transfer_id: String) -> Result<(), String> {
    state.cancel_transfer(&transfer_id)
}

pub fn run() {
    if let Err(error) = run_inner() {
        eprintln!("[dead-drop] could not start: {error}");
    }
}

fn run_inner() -> Result<(), Box<dyn Error>> {
    let std_listener = StdTcpListener::bind(("0.0.0.0", 0))?;
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
            cancel_transfer
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
