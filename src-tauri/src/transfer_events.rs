//! Transfer lifecycle tracking and the Tauri event adapter.
//!
//! The transfer engine emits domain snapshots through `EventSink`. Only this
//! module knows how the production sink maps those snapshots to Tauri events;
//! tests can provide a recording sink without a window or WebView.

use crate::{
    config::TRANSFER_PROGRESS_INTERVAL,
    diagnostics::{LogCategory, LogLevel, SupportLogger},
    events::{CONNECTIVITY_DIAGNOSTICS, INCOMING_TRANSFER, TRANSFER_UPDATE},
    models::{
        IncomingTransfer, RuntimeDiagnostics, TransferFile, TransferLifecycle, TransferPhase,
        TransferSnapshot, TrustRequest,
    },
    transfer::TransferError,
};
use std::{sync::Arc, time::Instant};
use tauri::{AppHandle, Emitter};

pub(crate) trait EventSink: Send + Sync {
    fn emit_transfer_update(&self, snapshot: &TransferSnapshot) -> Result<(), String>;
    fn emit_incoming_transfer(&self, transfer: &IncomingTransfer) -> Result<(), String>;

    fn emit_trust_request(&self, _request: &TrustRequest) -> Result<(), String> {
        Ok(())
    }

    fn record_log(
        &self,
        _level: LogLevel,
        _category: LogCategory,
        _event: &str,
        _detail: Option<&str>,
    ) {
    }

    fn emit_connectivity_diagnostics(
        &self,
        _diagnostics: &RuntimeDiagnostics,
    ) -> Result<(), String> {
        Ok(())
    }
}

pub(crate) struct TauriEventSink {
    pub(crate) app: AppHandle,
    pub(crate) logger: Arc<SupportLogger>,
}

impl EventSink for TauriEventSink {
    fn emit_transfer_update(&self, snapshot: &TransferSnapshot) -> Result<(), String> {
        let result = self
            .app
            .emit(TRANSFER_UPDATE, snapshot)
            .map_err(|error| error.to_string());
        if let Err(error) = &result {
            self.record_log(
                LogLevel::Warn,
                LogCategory::Errors,
                "transfer_update_emit_failed",
                Some(error),
            );
        }
        result
    }

    fn emit_incoming_transfer(&self, transfer: &IncomingTransfer) -> Result<(), String> {
        let result = self
            .app
            .emit(INCOMING_TRANSFER, transfer)
            .map_err(|error| error.to_string());
        if let Err(error) = &result {
            self.record_log(
                LogLevel::Warn,
                LogCategory::Errors,
                "incoming_update_emit_failed",
                Some(error),
            );
        }
        result
    }

    fn emit_trust_request(&self, request: &TrustRequest) -> Result<(), String> {
        let result = self
            .app
            .emit(crate::events::TRUST_REQUEST, request)
            .map_err(|error| error.to_string());
        if let Err(error) = &result {
            self.record_log(
                LogLevel::Warn,
                LogCategory::Errors,
                "trust_request_emit_failed",
                Some(error),
            );
        }
        result
    }

    fn record_log(
        &self,
        level: LogLevel,
        category: LogCategory,
        event: &str,
        detail: Option<&str>,
    ) {
        self.logger.record(level, category, event, detail);
    }

    fn emit_connectivity_diagnostics(
        &self,
        diagnostics: &RuntimeDiagnostics,
    ) -> Result<(), String> {
        let result = self
            .app
            .emit(CONNECTIVITY_DIAGNOSTICS, diagnostics)
            .map_err(|error| error.to_string());
        if let Err(error) = &result {
            self.record_log(
                LogLevel::Warn,
                LogCategory::Errors,
                "diagnostics_update_emit_failed",
                Some(error),
            );
        }
        result
    }
}

pub(crate) struct TransferTracker {
    pub(crate) events: Arc<dyn EventSink>,
    lifecycle: TransferLifecycle,
    snapshot: TransferSnapshot,
    last_progress_emit: Option<Instant>,
}

impl TransferTracker {
    pub(crate) fn new(
        events: Arc<dyn EventSink>,
        id: &str,
        direction: &str,
        phase: TransferPhase,
        device_name: &str,
    ) -> Self {
        Self {
            events,
            lifecycle: TransferLifecycle::new(phase),
            snapshot: TransferSnapshot {
                id: id.to_string(),
                direction: direction.to_string(),
                phase,
                device_name: device_name.to_string(),
                files: Vec::new(),
                total_bytes: 0,
                transferred_bytes: 0,
                bytes_per_second: 0,
                eta_seconds: None,
                message: None,
            },
            last_progress_emit: None,
        }
    }

    pub(crate) fn emit(&self) {
        if let Err(error) = self.events.emit_transfer_update(&self.snapshot) {
            self.events.record_log(
                LogLevel::Warn,
                LogCategory::Errors,
                "transfer_update_failed",
                Some(&error),
            );
        }
    }

    pub(crate) fn set_files(&mut self, files: Vec<TransferFile>, total_bytes: u64) {
        self.snapshot.files = files;
        self.snapshot.total_bytes = total_bytes;
    }

    pub(crate) fn transition(&mut self, next: TransferPhase, message: Option<String>) {
        if self.lifecycle.phase() == next {
            self.snapshot.message = message;
            self.emit();
            return;
        }
        if let Err(previous) = self.lifecycle.transition(next) {
            self.events.record_log(
                LogLevel::Warn,
                LogCategory::Errors,
                "invalid_transfer_transition",
                Some(&format!("from={previous:?} to={next:?}")),
            );
            return;
        }
        self.snapshot.phase = next;
        self.snapshot.message = message;
        self.emit();
    }

    pub(crate) fn progress(&mut self, transferred_bytes: u64, started_at: Instant, force: bool) {
        let now = Instant::now();
        if !force
            && self
                .last_progress_emit
                .map(|last| now.saturating_duration_since(last) < TRANSFER_PROGRESS_INTERVAL)
                .unwrap_or(false)
        {
            return;
        }
        let transferred_bytes = self
            .snapshot
            .transferred_bytes
            .max(transferred_bytes)
            .min(self.snapshot.total_bytes);
        let bytes_per_second = speed_for(transferred_bytes, started_at);
        let eta_seconds = if bytes_per_second > 0 && self.snapshot.total_bytes > transferred_bytes {
            Some((self.snapshot.total_bytes - transferred_bytes).div_ceil(bytes_per_second))
        } else {
            None
        };
        if force
            && self.snapshot.transferred_bytes == transferred_bytes
            && self.snapshot.eta_seconds == eta_seconds
        {
            return;
        }
        self.snapshot.transferred_bytes = transferred_bytes;
        self.snapshot.bytes_per_second = bytes_per_second;
        self.snapshot.eta_seconds = eta_seconds;
        self.last_progress_emit = Some(now);
        self.emit();
    }

    pub(crate) fn finish_error(&mut self, error: &TransferError) {
        if self.lifecycle.phase().is_terminal() {
            return;
        }
        let phase = if error.is_cancelled() {
            TransferPhase::Canceled
        } else {
            TransferPhase::Failed
        };
        self.transition(phase, Some(error.user_message()));
    }
}

fn speed_for(transferred: u64, started_at: Instant) -> u64 {
    let elapsed = started_at.elapsed().as_secs_f64().max(0.001);
    (transferred as f64 / elapsed) as u64
}
