//! NIP-49 encrypted private-key export helpers.
//!
//! This module owns the low-level `ncryptsec1...` encoding used by the account
//! home backup surface. It deliberately keeps all key material in local
//! zeroizing buffers and returns only the final bech32 string.

use bech32::{Bech32, Hrp};
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use rand::RngCore;
use rand::rngs::OsRng;
use scrypt::Params as ScryptParams;
use unicode_normalization::UnicodeNormalization;
use zeroize::Zeroizing;

use crate::error::{AccountHomeError, AccountHomeResult};

pub(crate) const NIP49_DEFAULT_LOG_N: u8 = 18;

const HRP_NCRYPTSEC: &str = "ncryptsec";
const VERSION_BYTE: u8 = 0x02;
const SCRYPT_R: u32 = 8;
const SCRYPT_P: u32 = 1;
const SALT_LEN: usize = 16;
const NONCE_LEN: usize = 24;
const SECRET_KEY_LEN: usize = 32;
const TAG_LEN: usize = 16;
const CIPHERTEXT_LEN: usize = SECRET_KEY_LEN + TAG_LEN;
const NCRYPTSEC_BYTES_LEN: usize = 1 + 1 + SALT_LEN + NONCE_LEN + 1 + CIPHERTEXT_LEN;

pub(crate) fn export_ncryptsec(
    secret_key: &nostr::SecretKey,
    passphrase: &str,
    key_security_byte: u8,
) -> AccountHomeResult<String> {
    let mut salt = [0_u8; SALT_LEN];
    OsRng.fill_bytes(&mut salt);
    let mut nonce = [0_u8; NONCE_LEN];
    OsRng.fill_bytes(&mut nonce);
    export_ncryptsec_with_material(
        secret_key,
        passphrase,
        NIP49_DEFAULT_LOG_N,
        key_security_byte,
        salt,
        nonce,
    )
}

fn export_ncryptsec_with_material(
    secret_key: &nostr::SecretKey,
    passphrase: &str,
    log_n: u8,
    key_security_byte: u8,
    salt: [u8; SALT_LEN],
    nonce: [u8; NONCE_LEN],
) -> AccountHomeResult<String> {
    if passphrase.is_empty() {
        return Err(AccountHomeError::EmptyPassphrase);
    }
    validate_key_security_byte(key_security_byte)?;

    let normalized_passphrase = Zeroizing::new(passphrase.nfkc().collect::<String>());
    let params = ScryptParams::new(log_n, SCRYPT_R, SCRYPT_P, SECRET_KEY_LEN)
        .map_err(encrypted_export_error)?;
    let mut symmetric_key = Zeroizing::new([0_u8; SECRET_KEY_LEN]);
    scrypt::scrypt(
        normalized_passphrase.as_bytes(),
        &salt,
        &params,
        &mut symmetric_key[..],
    )
    .map_err(encrypted_export_error)?;

    let cipher =
        XChaCha20Poly1305::new_from_slice(&symmetric_key[..]).map_err(encrypted_export_error)?;
    let secret_bytes = Zeroizing::new(secret_key.to_secret_bytes());
    let aad = [key_security_byte];
    let ciphertext = cipher
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: &secret_bytes[..],
                aad: &aad,
            },
        )
        .map_err(|_| AccountHomeError::EncryptedSecretExport("XChaCha20-Poly1305 failed".into()))?;
    if ciphertext.len() != CIPHERTEXT_LEN {
        return Err(AccountHomeError::EncryptedSecretExport(format!(
            "unexpected ciphertext length: expected {CIPHERTEXT_LEN}, got {}",
            ciphertext.len()
        )));
    }

    let mut bytes = Zeroizing::new(Vec::with_capacity(NCRYPTSEC_BYTES_LEN));
    bytes.push(VERSION_BYTE);
    bytes.push(log_n);
    bytes.extend_from_slice(&salt);
    bytes.extend_from_slice(&nonce);
    bytes.push(key_security_byte);
    bytes.extend_from_slice(&ciphertext);
    debug_assert_eq!(bytes.len(), NCRYPTSEC_BYTES_LEN);

    let hrp = Hrp::parse(HRP_NCRYPTSEC).map_err(encrypted_export_error)?;
    bech32::encode::<Bech32>(hrp, bytes.as_slice()).map_err(encrypted_export_error)
}

fn validate_key_security_byte(value: u8) -> AccountHomeResult<()> {
    match value {
        0x00..=0x02 => Ok(()),
        other => Err(AccountHomeError::EncryptedSecretExport(format!(
            "unsupported NIP-49 key security byte: {other:#04x}"
        ))),
    }
}

fn encrypted_export_error(error: impl std::fmt::Display) -> AccountHomeError {
    AccountHomeError::EncryptedSecretExport(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::nips::nip19::FromBech32;
    use nostr::nips::nip49::{EncryptedSecretKey, KeySecurity};

    #[test]
    fn exported_material_matches_nip49_test_vector_shape() {
        let secret_key = nostr::SecretKey::from_hex(
            "3501454135014541350145413501453fefb02227e449e57cf4d3a3ce05378683",
        )
        .unwrap();

        let ncryptsec = export_ncryptsec_with_material(
            &secret_key,
            "nostr",
            16,
            0x01,
            [0x11; SALT_LEN],
            [0x22; NONCE_LEN],
        )
        .unwrap();

        let encrypted = EncryptedSecretKey::from_bech32(&ncryptsec).unwrap();
        assert_eq!(encrypted.log_n(), 16);
        assert_eq!(encrypted.key_security(), KeySecurity::Medium);
        assert_eq!(encrypted.decrypt("nostr").unwrap(), secret_key);
    }

    #[test]
    fn export_normalizes_passphrase_nfkc() {
        let secret_key = nostr::SecretKey::from_hex(
            "3501454135014541350145413501453fefb02227e449e57cf4d3a3ce05378683",
        )
        .unwrap();

        let ncryptsec = export_ncryptsec_with_material(
            &secret_key,
            "ｔｅｓｔ１２３",
            12,
            0x02,
            [0x33; SALT_LEN],
            [0x44; NONCE_LEN],
        )
        .unwrap();

        let encrypted = EncryptedSecretKey::from_bech32(&ncryptsec).unwrap();
        assert_eq!(encrypted.key_security(), KeySecurity::Unknown);
        assert_eq!(encrypted.decrypt("test123").unwrap(), secret_key);
    }
}
