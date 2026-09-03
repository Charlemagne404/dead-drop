//! Destination-side file lifecycle for transfers.
//!
//! The transfer protocol decides what bytes should arrive. This module owns
//! the filesystem policy for those bytes: choose collision-safe names, write
//! hidden staging files, finalize without replacing an existing file, and
//! clean up after a failed batch.

#[cfg(any(test, feature = "integration-tests"))]
use crate::models::FaultPoint;
use crate::{models::AppState, platform, transfer::TransferError};
use std::{
    collections::HashSet,
    io::ErrorKind,
    path::{Path, PathBuf},
};
use tokio::fs;
use unicode_normalization::UnicodeNormalization;
use uuid::Uuid;

use crate::config::MAX_FILENAME_BYTES;

pub(crate) const MAX_COLLISION_ATTEMPTS: u32 = 100_000;

pub(crate) struct StagedFile {
    pub(crate) name: String,
    pub(crate) temporary: PathBuf,
    pub(crate) final_path: PathBuf,
}

pub(crate) fn temporary_staging_path(directory: &Path, transfer_id: &str, index: usize) -> PathBuf {
    let transfer_component = Uuid::parse_str(transfer_id)
        .map(|id| id.to_string())
        .unwrap_or_else(|_| "invalid".to_string());
    directory.join(format!(".dead-drop-{transfer_component}-{index}.part"))
}

pub(crate) fn available_destination_path(
    directory: &Path,
    name: &str,
    used_names: &mut HashSet<String>,
) -> Result<PathBuf, TransferError> {
    let source = Path::new(name);
    let stem = source
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or(name);
    let extension = source.extension().and_then(|value| value.to_str());
    for index in 0..=MAX_COLLISION_ATTEMPTS {
        let candidate_name = match (index, extension) {
            (0, _) => name.to_string(),
            (_, Some(extension)) => collision_name(stem, extension, index),
            (_, None) => collision_name(stem, "", index),
        };
        let candidate = directory.join(candidate_name);
        let key = path_key(&candidate);
        if !candidate.exists() && used_names.insert(key) {
            return Ok(candidate);
        }
    }
    Err(TransferError::Destination {
        detail: "too many duplicate file names".to_string(),
    })
}

pub(crate) fn collision_name(stem: &str, extension: &str, index: u32) -> String {
    let suffix = format!(" ({index})");
    let extension = extension.strip_prefix('.').unwrap_or(extension);
    if extension.is_empty() {
        let mut truncated_stem = stem.to_string();
        truncate_utf8(
            &mut truncated_stem,
            MAX_FILENAME_BYTES.saturating_sub(suffix.len()),
        );
        return format!("{truncated_stem}{suffix}");
    }

    let extension_budget = MAX_FILENAME_BYTES
        .saturating_sub(suffix.len())
        .saturating_sub(1);
    let extension = truncate_utf8_copy(extension, extension_budget);
    if extension.is_empty() {
        let mut truncated_stem = stem.to_string();
        truncate_utf8(
            &mut truncated_stem,
            MAX_FILENAME_BYTES.saturating_sub(suffix.len()),
        );
        return format!("{truncated_stem}{suffix}");
    }
    let stem_budget = MAX_FILENAME_BYTES
        .saturating_sub(suffix.len())
        .saturating_sub(extension.len())
        .saturating_sub(1);
    let mut truncated_stem = stem.to_string();
    truncate_utf8(&mut truncated_stem, stem_budget);
    format!("{truncated_stem}{suffix}.{extension}")
}

fn truncate_utf8(value: &mut String, maximum_bytes: usize) {
    while value.len() > maximum_bytes {
        value.pop();
    }
}

fn truncate_utf8_copy(value: &str, maximum_bytes: usize) -> String {
    let mut copy = value.to_string();
    truncate_utf8(&mut copy, maximum_bytes);
    copy
}

pub(crate) fn path_key(path: &Path) -> String {
    let value: String = path.to_string_lossy().nfc().collect();
    if platform::default_case_insensitive_filesystem() {
        value.to_lowercase()
    } else {
        value
    }
}

pub(crate) async fn finalize_staged_file(
    staged: &mut StagedFile,
    directory: &Path,
    used_names: &mut HashSet<String>,
) -> Result<(), TransferError> {
    let mut final_path = staged.final_path.clone();
    for index in 0..=MAX_COLLISION_ATTEMPTS {
        match move_staged_file(&staged.temporary, &final_path).await {
            Ok(()) => {
                staged.final_path = final_path;
                return Ok(());
            }
            Err(error) if is_already_exists(&error) => {
                if index == MAX_COLLISION_ATTEMPTS {
                    break;
                }
                final_path = available_destination_path(directory, &staged.name, used_names)?;
            }
            Err(error) => return Err(destination_error(error)),
        }
    }
    Err(TransferError::Destination {
        detail: "could not reserve a unique destination name".to_string(),
    })
}

async fn move_staged_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    match platform::move_file_without_overwrite(source, destination) {
        Ok(()) => Ok(()),
        Err(error) if can_fallback_to_hard_link(&error) => {
            fs::hard_link(source, destination).await?;
            if let Err(remove_error) = fs::remove_file(source).await {
                // Do not report success while the staging file still exists.
                // The destination was created here, so remove it before the
                // batch rollback path handles any remaining files.
                let _ = fs::remove_file(destination).await;
                return Err(remove_error);
            }
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn is_already_exists(error: &std::io::Error) -> bool {
    error.kind() == ErrorKind::AlreadyExists
        || matches!(error.raw_os_error(), Some(17) | Some(80) | Some(183))
}

fn can_fallback_to_hard_link(error: &std::io::Error) -> bool {
    error.kind() == ErrorKind::Unsupported
        || matches!(
            error.raw_os_error(),
            Some(1) | Some(22) | Some(38) | Some(45) | Some(50) | Some(87) | Some(95) | Some(524)
        )
}

pub(crate) async fn cleanup_staged(staged: &[StagedFile], state: &AppState) {
    #[cfg(not(any(test, feature = "integration-tests")))]
    let _ = state;
    for file in staged {
        for attempt in 0..3 {
            let result = {
                #[cfg(any(test, feature = "integration-tests"))]
                if let Some(error) = state.take_fault(FaultPoint::Cleanup) {
                    Err(error)
                } else {
                    fs::remove_file(&file.temporary).await
                }
                #[cfg(not(any(test, feature = "integration-tests")))]
                {
                    fs::remove_file(&file.temporary).await
                }
            };
            match result {
                Ok(()) => break,
                Err(error) if error.kind() == ErrorKind::NotFound => break,
                Err(error) if attempt == 2 => {
                    state.log(
                        crate::diagnostics::LogLevel::Warn,
                        crate::diagnostics::LogCategory::Filesystem,
                        "staged_file_cleanup_failed",
                        Some(&error.to_string()),
                    );
                }
                Err(_) => tokio::task::yield_now().await,
            }
        }
    }
}

pub(crate) async fn rollback_finalized(finalized: &[PathBuf], state: &AppState) {
    for path in finalized.iter().rev() {
        if let Err(error) = fs::remove_file(path).await {
            if error.kind() != ErrorKind::NotFound {
                state.log(
                    crate::diagnostics::LogLevel::Warn,
                    crate::diagnostics::LogCategory::Filesystem,
                    "finalized_file_rollback_failed",
                    Some(&error.to_string()),
                );
            }
        }
    }
}

pub(crate) fn destination_error(error: std::io::Error) -> TransferError {
    if matches!(
        error.kind(),
        ErrorKind::StorageFull | ErrorKind::QuotaExceeded
    ) || matches!(error.raw_os_error(), Some(28) | Some(39) | Some(112))
    {
        TransferError::DiskFull
    } else {
        TransferError::Destination {
            detail: error.to_string(),
        }
    }
}
