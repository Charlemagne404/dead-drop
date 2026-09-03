//! Small operating-system adapters for paths, app metadata, and file moves.
//!
//! Platform-specific behavior stays behind these helpers so transfer policy
//! can express destination semantics without knowing OS APIs.

use std::{
    io,
    path::{Path, PathBuf},
};

const QUALIFIER: &str = "com";
const ORGANIZATION: &str = "Continental";
const APPLICATION: &str = "Drop";
const LEGACY_APPLICATION: &str = "Dead Drop";

fn project_dirs(application: &str) -> Option<directories::ProjectDirs> {
    directories::ProjectDirs::from(QUALIFIER, ORGANIZATION, application)
}

/// v1 deliberately keeps the transport IPv4-only. Discovery disables IPv6
/// advertisements as well, so an IPv6 address can never be handed to the
/// current TCP transport by accident.
pub const TRANSPORT_NAME: &str = "IPv4";

pub fn platform_name() -> String {
    match std::env::consts::OS {
        "macos" => "macOS".to_string(),
        "windows" => "Windows".to_string(),
        "linux" => "Linux".to_string(),
        other => other.to_string(),
    }
}

pub fn default_destination() -> PathBuf {
    if let Some(downloads) = directories::UserDirs::new()
        .and_then(|dirs| dirs.download_dir().map(Path::to_path_buf))
        .filter(|path| path.is_absolute())
    {
        return downloads.join("Drop");
    }

    // A missing XDG/known Downloads directory should prefer a persistent,
    // OS-managed app-data location. The temporary fallback is only used when
    // the platform cannot provide an application-data directory at all.
    if let Some(dirs) = project_dirs(APPLICATION) {
        return dirs.data_local_dir().join("Received");
    }

    std::env::temp_dir().join("Drop")
}

pub fn settings_path() -> Option<PathBuf> {
    project_dirs(APPLICATION).map(|dirs| dirs.config_local_dir().join("settings.json"))
}

/// The device private key is kept separate from user-editable preferences so
/// its restrictive file permissions are easy to audit and a settings export
/// cannot accidentally include it.
pub fn identity_path() -> Option<PathBuf> {
    project_dirs(APPLICATION).map(|dirs| dirs.config_local_dir().join("identity.json"))
}

pub fn log_path() -> Option<PathBuf> {
    project_dirs(APPLICATION).map(|dirs| dirs.data_local_dir().join("drop.log"))
}

/// The pre-Plain release stored settings in the application data directory.
/// Keep this path readable during the display-name migration.
pub fn legacy_settings_path() -> Option<PathBuf> {
    project_dirs(LEGACY_APPLICATION).map(|dirs| dirs.data_local_dir().join("settings.json"))
}

/// Releases before the settings hardening pass used the old display name for
/// the normal per-user configuration directory.
pub fn previous_settings_path() -> Option<PathBuf> {
    project_dirs(LEGACY_APPLICATION).map(|dirs| dirs.config_local_dir().join("settings.json"))
}

pub fn default_case_insensitive_filesystem() -> bool {
    cfg!(any(windows, target_os = "macos"))
}

/// Replace a completed temporary settings file with the current settings.
/// Unix rename is atomic replacement; Windows needs the native replace flag
/// because std::fs::rename refuses to replace an existing file there.
#[cfg(not(windows))]
pub fn replace_file(source: &Path, destination: &Path) -> io::Result<()> {
    std::fs::rename(source, destination)
}

#[cfg(windows)]
pub fn replace_file(source: &Path, destination: &Path) -> io::Result<()> {
    const MOVEFILE_REPLACE_EXISTING: u32 = 0x00000001;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x00000008;
    let source = wide_path(source);
    let destination = wide_path(destination);
    let result = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result != 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Move a completed staged file into its final name without replacing an
/// existing path. Native no-replace rename APIs preserve the hardening
/// guarantee on the common filesystems where hard links are unavailable.
#[cfg(target_os = "linux")]
pub fn move_file_without_overwrite(source: &Path, destination: &Path) -> io::Result<()> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};

    let source = CString::new(source.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "path contains an embedded NUL")
    })?;
    let destination = CString::new(destination.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "path contains an embedded NUL")
    })?;
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "macos")]
pub fn move_file_without_overwrite(source: &Path, destination: &Path) -> io::Result<()> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};

    let source = CString::new(source.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "path contains an embedded NUL")
    })?;
    let destination = CString::new(destination.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "path contains an embedded NUL")
    })?;
    let result = unsafe {
        libc::renameatx_np(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(windows)]
pub fn move_file_without_overwrite(source: &Path, destination: &Path) -> io::Result<()> {
    const MOVEFILE_WRITE_THROUGH: u32 = 0x00000008;
    let source = wide_path(source);
    let destination = wide_path(destination);
    // Without MOVEFILE_REPLACE_EXISTING, Windows fails if the destination
    // already exists. Both paths are in the same receive directory, so the
    // move does not need cross-volume copying semantics.
    let result = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_WRITE_THROUGH,
        )
    };
    if result != 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub fn move_file_without_overwrite(source: &Path, destination: &Path) -> io::Result<()> {
    // A check followed by rename is a TOCTOU window, and rename may replace
    // an existing destination on Unix-like platforms. Hard-linking first is
    // the portable no-replace primitive; both paths are in the receive
    // directory, so they are expected to share a filesystem.
    std::fs::hard_link(source, destination)?;
    if let Err(error) = std::fs::remove_file(source) {
        let _ = std::fs::remove_file(destination);
        return Err(error);
    }
    Ok(())
}

#[cfg(windows)]
fn wide_path(path: &Path) -> Vec<u16> {
    use std::os::windows::ffi::OsStrExt;

    let raw: Vec<u16> = path.as_os_str().encode_wide().collect();
    let mut wide = if path.is_absolute()
        && !raw.starts_with(&"\\\\?\\".encode_utf16().collect::<Vec<_>>())
        && !raw.starts_with(&"\\\\.\\".encode_utf16().collect::<Vec<_>>())
    {
        if raw.starts_with(&"\\\\".encode_utf16().collect::<Vec<_>>()) {
            let mut prefixed = "\\\\?\\UNC\\".encode_utf16().collect::<Vec<_>>();
            prefixed.extend(raw.into_iter().skip(2));
            prefixed
        } else {
            let mut prefixed = "\\\\?\\".encode_utf16().collect::<Vec<_>>();
            prefixed.extend(raw);
            prefixed
        }
    } else {
        raw
    };
    wide.push(0);
    wide
}

#[cfg(windows)]
#[link(name = "kernel32")]
unsafe extern "system" {
    fn MoveFileExW(existing_file_name: *const u16, new_file_name: *const u16, flags: u32) -> i32;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_default_destination_is_absolute() {
        assert!(default_destination().is_absolute());
    }

    #[test]
    fn managed_settings_paths_are_absolute_when_available() {
        if let Some(path) = settings_path() {
            assert!(path.is_absolute());
        }
        if let Some(path) = legacy_settings_path() {
            assert!(path.is_absolute());
        }
        if let Some(path) = previous_settings_path() {
            assert!(path.is_absolute());
        }
    }
}
