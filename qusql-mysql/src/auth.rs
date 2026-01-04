//! Implementation of auth methods
#[cfg(feature = "sha2_auth")]
use crate::{ConnectionError, ConnectionErrorContent};
#[cfg(feature = "sha2_auth")]
use rsa::{Oaep, RsaPublicKey, pkcs8::DecodePublicKey, rand_core::OsRng};
use sha1::{Digest, Sha1, digest::Output};
#[cfg(feature = "sha2_auth")]
use sha2::Sha256;

/// Auth method used
#[derive(Clone, Copy)]
pub enum AuthPlugin {
    /// Old auth mechanism deprecated in mysql, still used by mariadb
    NativePassword,
    #[cfg(feature = "sha2_auth")]
    /// https://mariadb.com/kb/en/caching_sha2_password-authentication-plugin/
    CachingSha2Password,
}

/// Result of compute_auth either a sha1 or sha256 hash
pub enum AuthResult {
    /// Sha1 output
    Sha1(Output<Sha1>),
    #[cfg(feature = "sha2_auth")]
    /// Sha256 output
    Sha256(Output<Sha256>),
}

impl AuthResult {
    /// Get the output as a slice
    pub fn as_slice(&self) -> &[u8] {
        match self {
            AuthResult::Sha1(v) => v.as_slice(),
            #[cfg(feature = "sha2_auth")]
            AuthResult::Sha256(v) => v.as_slice(),
        }
    }
}

/// Compute the auth values based on the password and nonces
///
/// buffer must be 20 at least bytes long
pub(crate) fn compute_auth(password: &str, nonce: &[u8], method: AuthPlugin) -> AuthResult {
    match method {
        AuthPlugin::NativePassword => {
            // SHA1( password ) ^ SHA1( seed + SHA1( SHA1( password ) ) )
            // https://mariadb.com/kb/en/connection/#mysql_native_password-plugin
            let mut ctx: sha1::digest::core_api::CoreWrapper<sha1::Sha1Core> = Sha1::new();
            ctx.update(password.as_bytes());
            let mut pw_hash = ctx.finalize_reset();
            ctx.update(pw_hash);
            let pw_hash_hash = ctx.finalize_reset();
            ctx.update(nonce);
            ctx.update(pw_hash_hash);
            let pw_seed_hash_hash = ctx.finalize_reset();
            for i in 0..pw_hash.len() {
                pw_hash[i] ^= pw_seed_hash_hash[i];
            }
            AuthResult::Sha1(pw_hash)
        }
        #[cfg(feature = "sha2_auth")]
        AuthPlugin::CachingSha2Password => {
            // XOR(SHA256(password), SHA256(seed, SHA256(SHA256(password))))
            // https://mariadb.com/kb/en/caching_sha2_password-authentication-plugin/#sha-2-encrypted-password
            let mut ctx = Sha256::new();
            ctx.update(password);
            let mut pw_hash = ctx.finalize_reset();
            ctx.update(pw_hash);
            let pw_hash_hash = ctx.finalize_reset();
            ctx.update(nonce);
            ctx.update(pw_hash_hash);
            let pw_seed_hash_hash = ctx.finalize();
            for i in 0..pw_hash.len() {
                pw_hash[i] ^= pw_seed_hash_hash[i];
            }
            AuthResult::Sha256(pw_hash)
        }
    }
}

#[cfg(feature = "sha2_auth")]
/// RSA encrypt password for some reason
pub fn encrypt_rsa(pem: &str, password: &str, nonce: &[u8]) -> Result<Vec<u8>, ConnectionError> {
    let key = RsaPublicKey::from_public_key_pem(pem).map_err(|e| {
        ConnectionError::from(ConnectionErrorContent::ProtocolError(format!(
            "Invalid public key pem: {e:?}"
        )))
    })?;

    let mut passwd = Vec::with_capacity(password.len() + 1);
    passwd.extend_from_slice(password.as_bytes());
    passwd.push(0);

    for (i, c) in passwd.iter_mut().enumerate() {
        *c ^= nonce[i % nonce.len()];
    }

    let padding = Oaep::new::<sha1::Sha1>();
    key.encrypt(&mut OsRng, padding, &passwd).map_err(|e| {
        ConnectionError::from(ConnectionErrorContent::ProtocolError(format!(
            "Rsa encrypt failed: {e:?}"
        )))
    })
}
