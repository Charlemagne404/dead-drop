//! Bounded structured logging and redacted support-report generation.
//!
//! Diagnostics are deliberately separate from transfer and discovery policy:
//! subsystems record events here, while the frontend only receives the safe
//! report DTO and runtime snapshot.

use crate::models::RuntimeDiagnostics;
use parking_lot::Mutex;
use serde::Serialize;
use std::{
    collections::VecDeque,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

/// The log is intentionally small. Diagnostics are for answering the last
/// "why did this fail?" question, not for acting as an application history.
pub const MAX_LOG_ENTRIES: usize = 256;
pub const MAX_LOG_LINE_BYTES: usize = 2 * 1024;
pub const MAX_LOG_FILE_BYTES: u64 = 128 * 1024;
pub const MAX_ROTATED_LOG_FILES: usize = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LogLevel {
    Info,
    Warn,
    Error,
}

impl LogLevel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

/// Keep categories stable so a support report can be searched without
/// coupling it to Rust module names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LogCategory {
    Startup,
    Shutdown,
    Discovery,
    PeerRegistry,
    RouteSelection,
    Connection,
    Transfer,
    Filesystem,
    Settings,
    Errors,
}

impl LogCategory {
    fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Shutdown => "shutdown",
            Self::Discovery => "discovery",
            Self::PeerRegistry => "peer_registry",
            Self::RouteSelection => "route_selection",
            Self::Connection => "connection",
            Self::Transfer => "transfer",
            Self::Filesystem => "filesystem",
            Self::Settings => "settings",
            Self::Errors => "errors",
        }
    }
}

#[derive(Serialize)]
struct LogRecord {
    timestamp: u64,
    level: &'static str,
    category: &'static str,
    event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

#[derive(Default)]
struct LoggerState {
    lines: VecDeque<String>,
    bytes: usize,
}

/// A small structured logger with an optional persistent sink. The in-memory
/// form is used by unit/integration tests so tests never write to a user's
/// application-data directory.
pub(crate) struct SupportLogger {
    path: Option<PathBuf>,
    max_file_bytes: u64,
    max_rotated_files: usize,
    state: Mutex<LoggerState>,
    persistent_lock: Mutex<()>,
}

impl SupportLogger {
    pub(crate) fn persistent(path: Option<PathBuf>) -> Self {
        Self::with_limits(path, MAX_LOG_FILE_BYTES, MAX_ROTATED_LOG_FILES)
    }

    pub(crate) fn in_memory() -> Self {
        Self::with_limits(None, MAX_LOG_FILE_BYTES, MAX_ROTATED_LOG_FILES)
    }

    #[cfg(test)]
    fn with_test_limits(path: PathBuf, max_file_bytes: u64, max_rotated_files: usize) -> Self {
        Self::with_limits(Some(path), max_file_bytes, max_rotated_files)
    }

    fn with_limits(path: Option<PathBuf>, max_file_bytes: u64, max_rotated_files: usize) -> Self {
        Self {
            path,
            max_file_bytes: max_file_bytes.max(1),
            max_rotated_files,
            state: Mutex::new(LoggerState::default()),
            persistent_lock: Mutex::new(()),
        }
    }

    pub(crate) fn record(
        &self,
        level: LogLevel,
        category: LogCategory,
        event: &str,
        detail: Option<&str>,
    ) {
        let detail = detail.map(|value| {
            let mut value = redact_text(value);
            // Leave headroom for JSON escaping and the stable record fields so
            // an oversized detail is omitted rather than producing a clipped,
            // invalid structured record.
            truncate_utf8(&mut value, MAX_LOG_LINE_BYTES / 4);
            value
        });
        let mut record = LogRecord {
            timestamp: unix_now(),
            level: level.as_str(),
            category: category.as_str(),
            event: sanitize_token(event, 80),
            detail,
        };
        let mut line = serde_json::to_string(&record).unwrap_or_else(|_| {
            "{\"level\":\"error\",\"category\":\"errors\",\"event\":\"log_encode_failed\"}"
                .to_string()
        });
        if line.len() > MAX_LOG_LINE_BYTES {
            record.detail = None;
            line = serde_json::to_string(&record).unwrap_or_else(|_| {
                "{\"level\":\"error\",\"category\":\"errors\",\"event\":\"log_encode_failed\"}"
                    .to_string()
            });
        }

        let mut state = self.state.lock();
        state.bytes = state.bytes.saturating_add(line.len() + 1);
        state.lines.push_back(line.clone());
        while state.lines.len() > MAX_LOG_ENTRIES
            || state.bytes > MAX_LOG_ENTRIES * MAX_LOG_LINE_BYTES
        {
            if let Some(removed) = state.lines.pop_front() {
                state.bytes = state.bytes.saturating_sub(removed.len() + 1);
            } else {
                break;
            }
        }
        drop(state);

        self.append_persistent(&line);
    }

    pub(crate) fn recent_lines(&self) -> Vec<String> {
        self.state.lock().lines.iter().cloned().collect()
    }

    pub(crate) fn current_entry_count(&self) -> usize {
        self.state.lock().lines.len()
    }

    pub(crate) fn storage_status(&self) -> &'static str {
        if self.path.is_some() {
            "bounded rolling file"
        } else {
            "current session only"
        }
    }

    fn append_persistent(&self, line: &str) {
        let Some(path) = &self.path else {
            return;
        };
        let _persistent_lock = self.persistent_lock.lock();
        if self.max_file_bytes <= 1 {
            return;
        }
        let line_bytes = line.len() as u64 + 1;
        if line_bytes > self.max_file_bytes {
            return;
        }
        if let Some(parent) = path.parent() {
            if fs::create_dir_all(parent).is_err() {
                return;
            }
        }
        let current_size = fs::metadata(path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        if current_size > self.max_file_bytes
            || current_size.saturating_add(line_bytes) > self.max_file_bytes
        {
            self.rotate(path);
        }
        let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) else {
            return;
        };
        let _ = writeln!(file, "{line}");
    }

    fn rotate(&self, path: &Path) {
        if self.max_rotated_files == 0 {
            let _ = fs::remove_file(path);
            return;
        }
        for index in (1..=self.max_rotated_files).rev() {
            let target = rotated_path(path, index);
            if index == self.max_rotated_files {
                let _ = fs::remove_file(&target);
            }
            let source = if index == 1 {
                path.to_path_buf()
            } else {
                rotated_path(path, index - 1)
            };
            if source.exists() {
                let _ = fs::remove_file(&target);
                let _ = fs::rename(source, target);
            }
        }
    }
}

fn truncate_utf8(value: &mut String, maximum: usize) {
    if value.len() <= maximum {
        return;
    }
    let mut end = maximum;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
}

fn rotated_path(path: &Path, index: usize) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("drop.log");
    path.with_file_name(format!("{name}.{index}"))
}

/// Redact common credential-shaped values before a detail is ever written to
/// disk or returned in a report. Callers still avoid logging paths, filenames,
/// file contents, and command output wherever possible.
pub(crate) fn redact_text(value: &str) -> String {
    let cleaned = value
        .chars()
        .filter(|character| !character.is_control() || *character == '\t')
        .collect::<String>();
    let mut result = Vec::new();
    let mut redact_next = false;
    for token in cleaned.split_whitespace() {
        if redact_next {
            result.push("[redacted]".to_string());
            let next = boundary_trim(token);
            redact_next = next.is_empty() || next.eq_ignore_ascii_case("bearer");
            continue;
        }
        if is_path_like(boundary_trim(token)) {
            result.push("[path redacted]".to_string());
            continue;
        }
        let lower = token.to_ascii_lowercase();
        let trimmed = boundary_trim(&lower);
        if trimmed.starts_with("tskey-") || trimmed.starts_with("bearer=") {
            result.push("[redacted]".to_string());
            continue;
        }
        if let Some(separator) = token.find(['=', ':']) {
            let key = boundary_trim(&lower[..separator]);
            let assignment_value = boundary_trim(&token[separator + 1..]);
            if is_path_like(assignment_value) {
                result.push(format!("{}[path redacted]", &token[..=separator]));
                continue;
            }
            if is_secret_prefix(assignment_value) {
                result.push(format!("{}[redacted]", &token[..=separator]));
                redact_next = assignment_value.eq_ignore_ascii_case("bearer");
                continue;
            }
            if is_sensitive_key(key) {
                result.push(format!("{}[redacted]", &token[..=separator]));
                redact_next = token[separator + 1..]
                    .chars()
                    .all(|character| !character.is_ascii_alphanumeric());
                continue;
            }
        }
        if is_sensitive_key(trimmed.trim_end_matches(['=', ':'])) {
            result.push(token.to_string());
            redact_next = true;
            continue;
        }
        result.push(token.to_string());
    }
    let mut redacted = result.join(" ");
    truncate_utf8(&mut redacted, MAX_LOG_LINE_BYTES / 2);
    redacted
}

fn is_sensitive_key(value: &str) -> bool {
    matches!(
        value,
        "password"
            | "passwd"
            | "token"
            | "secret"
            | "authorization"
            | "bearer"
            | "api_key"
            | "apikey"
            | "private_key"
            | "tailscale_key"
    )
}

fn is_secret_prefix(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    lower.starts_with("tskey-") || lower.starts_with("bearer=") || lower == "bearer"
}

fn is_path_like(value: &str) -> bool {
    value.starts_with('/')
        || value.starts_with("~/")
        || value.starts_with("./")
        || value.starts_with("../")
        || value.starts_with("\\\\")
        || value.as_bytes().get(1) == Some(&b':')
            && value
                .as_bytes()
                .get(2)
                .is_some_and(|character| *character == b'\\' || *character == b'/')
}

fn boundary_trim(value: &str) -> &str {
    value.trim_matches(|character: char| {
        matches!(
            character,
            '"' | '\'' | ',' | ';' | ':' | '=' | '(' | ')' | '[' | ']' | '{' | '}'
        )
    })
}

fn sanitize_token(value: &str, maximum: usize) -> String {
    let mut token = value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
        .collect::<String>();
    if token.len() > maximum {
        token.truncate(maximum);
    }
    if token.is_empty() {
        "unknown".to_string()
    } else {
        token
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

/// Render a support-safe report from the current runtime snapshot. The
/// report deliberately contains endpoint addresses and stable Drop IDs, but
/// never the receive path, filenames, file contents, command output, or
/// discovery source keys such as Tailscale public keys.
pub(crate) fn render_report(diagnostics: &RuntimeDiagnostics, logger: &SupportLogger) -> String {
    let mut report = String::new();
    report.push_str("Drop diagnostics\n================\n\n");
    report.push_str("Application\n");
    report.push_str(&format!("Version: {}\n", diagnostics.application.version));
    report.push_str(&format!("OS: {}\n", diagnostics.application.os));
    report.push_str(&format!(
        "Architecture: {}\n",
        diagnostics.application.architecture
    ));
    report.push_str(&format!(
        "Protocol: v{}\n\n",
        diagnostics.application.protocol_version
    ));

    report.push_str("Local Drop instance\n");
    report.push_str(&format!("Device UUID: {}\n", diagnostics.local.device_id));
    report.push_str(&format!(
        "Device name: {}\n",
        redact_text(&diagnostics.local.device_name)
    ));
    report.push_str(&format!(
        "Identity fingerprint: {}\n",
        diagnostics.local.identity_fingerprint
    ));
    report.push_str(&format!(
        "Identity storage: {}\n",
        diagnostics.local.identity_storage_status
    ));
    report.push_str(&format!(
        "Receive directory: {}\n",
        availability_label(diagnostics.local.receive_directory_available)
    ));
    report.push_str(&format!(
        "Listener/service: {}{}\n",
        diagnostics.local.service_status,
        diagnostics
            .local
            .service_detail
            .as_deref()
            .map(|detail| format!(" ({})", redact_text(detail)))
            .unwrap_or_default()
    ));
    report.push_str(&format!(
        "Service port: TCP/UDP {}\n",
        diagnostics.local.service_port
    ));
    report.push_str(&format!("Transport: {}\n", diagnostics.local.transport));
    report.push_str(&format!(
        "Interface scope: {}\n",
        diagnostics.local.interface_status
    ));
    report.push_str("Transport limitations:\n");
    for limitation in &diagnostics.local.transport_limitations {
        report.push_str(&format!("- {}\n", redact_text(limitation)));
    }
    report.push('\n');

    report.push_str("Discovery / connectivity\n");
    report.push_str(&format!(
        "Logical peers: {}\n",
        diagnostics.logical_peer_count
    ));
    report.push_str(&format_source_status("mDNS", &diagnostics.discovery.mdns));
    report.push_str(&format_source_status(
        "Local fallback",
        &diagnostics.discovery.local_fallback,
    ));
    report.push_str(&format_source_status(
        "Tailscale",
        &diagnostics.discovery.tailscale,
    ));
    report.push_str(&format!(
        "Remembered peers: {}\n\n",
        diagnostics.discovery.remembered_peers
    ));

    report.push_str("Trusted devices\n");
    if diagnostics.trusted_devices.is_empty() {
        report.push_str("No trusted devices.\n");
    } else {
        for device in &diagnostics.trusted_devices {
            report.push_str(&format!(
                "- {} ({}, fingerprint {})\n",
                redact_text(&device.name),
                device.short_fingerprint,
                device.fingerprint
            ));
        }
    }
    report.push('\n');

    report.push_str("Peer diagnostics\n");
    if diagnostics.peers.is_empty() {
        report.push_str("No current Drop peers.\n");
    } else {
        for peer in &diagnostics.peers {
            report.push_str(&format!("- {}\n", redact_text(&peer.name)));
            report.push_str(&format!("  UUID: {}\n", peer.id));
            report.push_str(&format!("  OS: {}\n", redact_text(&peer.os)));
            report.push_str(&format!(
                "  Protocol: v{} ({})\n",
                peer.protocol_version,
                if peer.protocol_compatible {
                    "compatible"
                } else {
                    "incompatible"
                }
            ));
            report.push_str(&format!(
                "  Preferred route: {}\n",
                peer.selected_route.as_deref().unwrap_or("none")
            ));
            if let Some(route) = &peer.last_successful_route {
                report.push_str(&format!(
                    "  Last successful route: {} ({}, {} ago)\n",
                    route.endpoint,
                    route.route_class,
                    format_age(route.seconds_ago)
                ));
            } else {
                report.push_str("  Last successful route: unknown\n");
            }
            report.push_str("  Endpoints:\n");
            if peer.endpoints.is_empty() {
                report.push_str("  - none\n");
            } else {
                for endpoint in &peer.endpoints {
                    report.push_str(&format!(
                        "  - {} [{}; {}; {}; {}]\n",
                        endpoint.address,
                        endpoint.address_family,
                        endpoint.sources.join(", "),
                        endpoint.route_class,
                        endpoint.reachability
                    ));
                    report.push_str(&format!(
                        "    last seen: {} ago\n",
                        format_age(endpoint.last_seen_seconds_ago)
                    ));
                }
            }
            if peer.recent_route_failures.is_empty() {
                report.push_str("  Recent route failures: none\n");
            } else {
                report.push_str("  Recent route failures:\n");
                for failure in &peer.recent_route_failures {
                    report.push_str(&format!(
                        "  - {} ({}, {}) {} ago\n",
                        failure.endpoint,
                        failure.route_class,
                        redact_text(&failure.reason),
                        format_age(failure.seconds_ago)
                    ));
                }
            }
        }
    }

    report.push_str("\nLogging\n");
    report.push_str(&format!(
        "Storage: {}\n",
        diagnostics.logging.storage_status
    ));
    report.push_str(&format!("Retention: {}\n", diagnostics.logging.retention));
    report.push_str(&format!(
        "Current session entries: {}\n",
        diagnostics.logging.current_entries
    ));
    report.push_str("Recent structured entries:\n");
    let lines = logger.recent_lines();
    if lines.is_empty() {
        report.push_str("- none\n");
    } else {
        for line in lines {
            report.push_str("- ");
            report.push_str(&redact_text(&line));
            report.push('\n');
        }
    }
    report.push_str(
        "\nPrivacy: this report excludes file contents, filenames, passwords, auth tokens, ",
    );
    report.push_str("Tailscale keys, full receive paths, and unrelated system information.\n");
    report
}

fn availability_label(available: bool) -> &'static str {
    if available {
        "available"
    } else {
        "unavailable"
    }
}

fn format_source_status(label: &str, source: &crate::models::DiscoverySourceDiagnostics) -> String {
    let detail = source
        .detail
        .as_deref()
        .map(|value| format!(" — {}", redact_text(value)))
        .unwrap_or_default();
    format!("{label}: {}{detail}\n", source.status)
}

fn format_age(seconds: u64) -> String {
    if seconds < 5 {
        "just now".to_string()
    } else if seconds < 60 {
        format!("{seconds}s")
    } else {
        format!("{}m", seconds / 60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn redaction_removes_credentials_and_paths() {
        let redacted =
            redact_text(
                "password=hunter2 token=abc123 tskey-auth-123 key=tskey-auth-456 /Users/alice/secret.txt path=/Volumes/private/Drop",
            );
        assert!(!redacted.contains("hunter2"));
        assert!(!redacted.contains("abc123"));
        assert!(!redacted.contains("tskey-auth-123"));
        assert!(!redacted.contains("tskey-auth-456"));
        assert!(!redacted.contains("/Users/alice"));
        assert!(!redacted.contains("/Volumes/private"));
    }

    #[test]
    fn redaction_handles_header_and_json_shaped_credentials() {
        let redacted = redact_text(
            "Authorization: Bearer hunter2 token = abc123 {\"token\":\"def456\"} \"password\": \"secret\"",
        );
        assert!(!redacted.contains("hunter2"));
        assert!(!redacted.contains("abc123"));
        assert!(!redacted.contains("def456"));
        assert!(!redacted.contains("secret"));
    }

    #[test]
    fn structured_records_are_parseable_and_classified() {
        let logger = SupportLogger::in_memory();
        logger.record(
            LogLevel::Warn,
            LogCategory::RouteSelection,
            "route_failed",
            Some("endpoint=192.168.1.40:39821 token=hunter2"),
        );
        let oversized = "\"".repeat(MAX_LOG_LINE_BYTES);
        logger.record(
            LogLevel::Error,
            LogCategory::Errors,
            "oversized_detail",
            Some(&oversized),
        );
        let lines = logger.recent_lines();
        assert_eq!(lines.len(), 2);
        let record: serde_json::Value = serde_json::from_str(&lines[0]).expect("valid log JSON");
        assert_eq!(record["level"], "warn");
        assert_eq!(record["category"], "route_selection");
        assert_eq!(record["event"], "route_failed");
        assert!(!lines[0].contains("hunter2"));
        for line in lines {
            assert!(line.len() <= MAX_LOG_LINE_BYTES);
            serde_json::from_str::<serde_json::Value>(&line).expect("bounded log JSON");
        }
    }

    #[test]
    fn redaction_removes_control_characters_and_bounds_detail() {
        let value = format!("ok\nsecret={} tail", "x".repeat(MAX_LOG_LINE_BYTES));
        let redacted = redact_text(&value);
        assert!(!redacted.chars().any(|character| character == '\n'));
        assert!(redacted.len() <= MAX_LOG_LINE_BYTES / 2);
    }

    #[test]
    fn persistent_logger_rotates_and_keeps_memory_bounded() {
        let root = std::env::temp_dir().join(format!("drop-log-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("test log directory should be created");
        let path = root.join("drop.log");
        let logger = SupportLogger::with_test_limits(path.clone(), 180, 2);
        for index in 0..100 {
            logger.record(
                LogLevel::Info,
                LogCategory::Transfer,
                "bounded_event",
                Some(&format!("index={index}")),
            );
        }
        assert!(logger.current_entry_count() <= MAX_LOG_ENTRIES);
        assert!(fs::metadata(&path).expect("current log should exist").len() <= 180);
        assert!(
            fs::metadata(root.join("drop.log.1"))
                .expect("first rotated log should exist")
                .len()
                <= 180
        );
        let _ = fs::remove_dir_all(root);
    }
}
