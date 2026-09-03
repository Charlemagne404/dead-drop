use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine as _};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use snow::{params::NoiseParams, Builder, HandshakeState};
use std::{
    fs::{self, OpenOptions},
    io,
    path::{Path, PathBuf},
};
use uuid::Uuid;
use x25519_dalek::{PublicKey, StaticSecret};

pub(crate) const NOISE_PATTERN: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";
pub(crate) const NOISE_PROLOGUE: &[u8] = b"Drop secure sessions v2";
const IDENTITY_FILE_VERSION: u16 = 1;
const PRIVATE_KEY_BYTES: usize = 32;
const FINGERPRINT_BYTES: usize = 32;
const MAX_IDENTITY_FILE_BYTES: usize = 4 * 1024;

/// The process-local identity. The private key is intentionally not exposed
/// through serde, diagnostics, logs, or the frontend model.
pub(crate) struct LocalIdentity {
    private_key: [u8; PRIVATE_KEY_BYTES],
    public_key: [u8; PRIVATE_KEY_BYTES],
    fingerprint: String,
}

impl Clone for LocalIdentity {
    fn clone(&self) -> Self {
        Self {
            private_key: self.private_key,
            public_key: self.public_key,
            fingerprint: self.fingerprint.clone(),
        }
    }
}

impl LocalIdentity {
    pub(crate) fn generate() -> Result<Self, String> {
        let keypair = Builder::new(noise_params())
            .generate_keypair()
            .map_err(|_| "Drop could not generate a device identity.".to_string())?;
        let private_key: [u8; PRIVATE_KEY_BYTES] = keypair
            .private
            .try_into()
            .map_err(|_| "Drop generated an invalid device identity.".to_string())?;
        Ok(Self::from_private_key(private_key))
    }

    pub(crate) fn from_private_key(private_key: [u8; PRIVATE_KEY_BYTES]) -> Self {
        let secret = StaticSecret::from(private_key);
        let public_key = PublicKey::from(&secret).to_bytes();
        let fingerprint = fingerprint_for_public_key(&public_key)
            .expect("a fixed-size X25519 public key always has a fingerprint");
        Self {
            private_key,
            public_key,
            fingerprint,
        }
    }

    pub(crate) fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    #[cfg(test)]
    pub(crate) fn public_key(&self) -> &[u8; PRIVATE_KEY_BYTES] {
        &self.public_key
    }

    pub(crate) fn initiator(&self) -> Result<HandshakeState, String> {
        Builder::new(noise_params())
            .local_private_key(&self.private_key)
            .prologue(NOISE_PROLOGUE)
            .build_initiator()
            .map_err(|_| "Drop could not start a secure session.".to_string())
    }

    pub(crate) fn responder(&self) -> Result<HandshakeState, String> {
        Builder::new(noise_params())
            .local_private_key(&self.private_key)
            .prologue(NOISE_PROLOGUE)
            .build_responder()
            .map_err(|_| "Drop could not start a secure session.".to_string())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IdentityStorageStatus {
    Persistent,
    Created,
    Regenerated,
    Ephemeral,
}

impl IdentityStorageStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Persistent => "persistent",
            Self::Created => "persistent (new)",
            Self::Regenerated => "persistent (regenerated)",
            Self::Ephemeral => "ephemeral; identity file unavailable",
        }
    }
}

pub(crate) struct LoadedIdentity {
    pub identity: LocalIdentity,
    pub status: IdentityStorageStatus,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedIdentity {
    version: u16,
    private_key: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IdentityFile<'a> {
    version: u16,
    private_key: &'a str,
}

pub(crate) fn load_or_create(path: Option<PathBuf>) -> Result<LoadedIdentity, String> {
    if let Some(path) = path.as_deref() {
        if let Some(private_key) = read_private_key(path) {
            // A valid key file may have been copied or have had its mode
            // loosened outside Drop. Tighten it before allowing the key to
            // become the active installation identity.
            if apply_restrictive_permissions(path).is_ok() {
                return Ok(LoadedIdentity {
                    identity: LocalIdentity::from_private_key(private_key),
                    status: IdentityStorageStatus::Persistent,
                });
            }
        }
    }

    let identity = LocalIdentity::generate()?;
    let was_present = path.as_ref().is_some_and(|path| path.exists());
    let status = match path {
        Some(path) => {
            if persist(&path, &identity).is_ok() {
                if was_present {
                    IdentityStorageStatus::Regenerated
                } else {
                    IdentityStorageStatus::Created
                }
            } else {
                IdentityStorageStatus::Ephemeral
            }
        }
        None => IdentityStorageStatus::Ephemeral,
    };
    Ok(LoadedIdentity { identity, status })
}

fn read_private_key(path: &Path) -> Option<[u8; PRIVATE_KEY_BYTES]> {
    let raw = fs::read(path).ok()?;
    if raw.len() > MAX_IDENTITY_FILE_BYTES {
        return None;
    }
    let value: PersistedIdentity = serde_json::from_slice(&raw).ok()?;
    if value.version != IDENTITY_FILE_VERSION {
        return None;
    }
    let bytes = STANDARD_NO_PAD.decode(value.private_key).ok()?;
    bytes.try_into().ok()
}

fn persist(path: &Path, identity: &LocalIdentity) -> io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "identity path has no parent")
    })?;
    fs::create_dir_all(parent)?;
    let encoded = STANDARD_NO_PAD.encode(identity.private_key);
    let file = IdentityFile {
        version: IDENTITY_FILE_VERSION,
        private_key: &encoded,
    };
    let bytes = serde_json::to_vec(&file)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    let temporary = path.with_file_name(format!(
        ".{}.identity-{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("drop"),
        Uuid::new_v4()
    ));
    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        std::io::Write::write_all(&mut file, &bytes)?;
        file.sync_all()?;
        drop(file);
        apply_restrictive_permissions(&temporary)?;
        crate::platform::replace_file(&temporary, path)?;
        apply_restrictive_permissions(path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn apply_restrictive_permissions(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path)?.permissions();
        permissions.set_mode(0o600);
        fs::set_permissions(path, permissions)?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

pub(crate) fn fingerprint_for_public_key(public_key: &[u8]) -> Option<String> {
    if public_key.len() != PRIVATE_KEY_BYTES {
        return None;
    }
    let digest = Sha256::digest(public_key);
    Some(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub(crate) fn valid_fingerprint(value: &str) -> bool {
    value.len() == FINGERPRINT_BYTES * 2 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(crate) fn short_fingerprint(value: &str) -> String {
    if valid_fingerprint(value) {
        format!("{}…{}", &value[..6], &value[value.len() - 6..])
    } else {
        "unavailable".to_string()
    }
}

fn noise_params() -> NoiseParams {
    NOISE_PATTERN
        .parse()
        .expect("the compiled Drop Noise pattern must be valid")
}

#[cfg(any(test, feature = "integration-tests"))]
pub(crate) fn test_identity(seed: &str) -> LocalIdentity {
    let digest = Sha256::digest(seed.as_bytes());
    let mut private_key = [0_u8; PRIVATE_KEY_BYTES];
    private_key.copy_from_slice(&digest);
    LocalIdentity::from_private_key(private_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn generated_identities_have_stable_public_fingerprints() {
        let identity = test_identity("identity-test");
        assert_eq!(identity.fingerprint().len(), 64);
        assert!(valid_fingerprint(identity.fingerprint()));
        assert_eq!(
            fingerprint_for_public_key(identity.public_key()),
            Some(identity.fingerprint().to_string())
        );
        assert_ne!(identity.fingerprint(), test_identity("other").fingerprint());
    }

    #[test]
    fn identity_file_round_trips_without_exposing_a_private_key_in_metadata() {
        let directory = std::env::temp_dir().join(format!("drop-identity-{}", Uuid::new_v4()));
        fs::create_dir_all(&directory).expect("test directory should be created");
        let path = directory.join("identity.json");
        let original = test_identity("persisted");
        persist(&path, &original).expect("identity should persist");
        let loaded = load_or_create(Some(path.clone())).expect("identity should load");
        assert_eq!(loaded.status, IdentityStorageStatus::Persistent);
        assert_eq!(loaded.identity.fingerprint(), original.fingerprint());
        let raw = fs::read_to_string(&path).expect("identity file should be readable");
        assert!(!raw.contains(original.fingerprint()));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        fs::remove_dir_all(directory).expect("test directory should be removed");
    }

    #[test]
    fn malformed_identity_is_replaced_and_gets_a_new_fingerprint() {
        let directory = std::env::temp_dir().join(format!("drop-identity-{}", Uuid::new_v4()));
        fs::create_dir_all(&directory).expect("test directory should be created");
        let path = directory.join("identity.json");
        fs::write(&path, b"not an identity").expect("malformed fixture should be written");
        let loaded = load_or_create(Some(path.clone())).expect("identity should regenerate");
        assert_eq!(loaded.status, IdentityStorageStatus::Regenerated);
        assert!(valid_fingerprint(loaded.identity.fingerprint()));
        fs::remove_dir_all(directory).expect("test directory should be removed");
    }

    #[cfg(unix)]
    #[test]
    fn loading_an_identity_reapplies_restrictive_file_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let directory = std::env::temp_dir().join(format!("drop-identity-{}", Uuid::new_v4()));
        fs::create_dir_all(&directory).expect("test directory should be created");
        let path = directory.join("identity.json");
        let original = test_identity("permission-repair");
        persist(&path, &original).expect("identity should persist");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644))
            .expect("test should loosen the identity permissions");
        let loaded = load_or_create(Some(path.clone())).expect("identity should load");
        assert_eq!(loaded.status, IdentityStorageStatus::Persistent);
        assert_eq!(loaded.identity.fingerprint(), original.fingerprint());
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        fs::remove_dir_all(directory).expect("test directory should be removed");
    }
}
