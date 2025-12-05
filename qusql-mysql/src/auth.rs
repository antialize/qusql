//! Implementation of auth methods
use sha1_smol::Sha1;

/// Compute the auth values based on the password and nonces
///
/// Res must be 20 bytes long
pub(crate) fn compute_auth(password: &str, nonce_1: &[u8], nonce_2: &[u8], res: &mut [u8]) {
    // SHA1( password ) ^ SHA1( seed + SHA1( SHA1( password ) ) )
    // https://mariadb.com/kb/en/connection/#mysql_native_password-plugin

    let mut ctx = Sha1::new();

    ctx.update(password.as_bytes());

    let mut pw_hash = ctx.digest().bytes();

    ctx.reset();
    ctx.update(&pw_hash);

    let pw_hash_hash = ctx.digest().bytes();
    ctx.reset();

    ctx.update(nonce_1);
    ctx.update(nonce_2);
    ctx.update(&pw_hash_hash);

    let pw_seed_hash_hash = ctx.digest().bytes();

    for i in 0..pw_hash.len() {
        pw_hash[i] ^= pw_seed_hash_hash[i];
    }

    res.copy_from_slice(pw_hash.as_slice());
}
