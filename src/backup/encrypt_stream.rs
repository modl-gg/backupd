use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use anyhow::{Context, Result};
use argon2::{Algorithm, Argon2, Params, Version};
use rand::RngCore;

const MAGIC: &[u8; 4] = b"MBK1";
const FORMAT_VERSION: u8 = 1;
const SALT_LEN: usize = 16;
const NONCE_LEN: usize = 12;
const ARGON2_MEMORY_KIB: u32 = 64 * 1024;
const ARGON2_ITERATIONS: u32 = 3;
const ARGON2_PARALLELISM: u32 = 1;

#[derive(Debug, Clone)]
pub struct EncryptionDescriptor {
    pub format_version: u8,
    pub chunk_size_bytes: u32,
    pub base_nonce_hex: String,
    pub argon2_memory_kib: u32,
    pub argon2_iterations: u32,
    pub argon2_parallelism: u32,
    pub salt_hex: String,
}

pub struct StreamEncryptor {
    cipher: Aes256Gcm,
    chunk_size: u32,
    base_nonce: [u8; NONCE_LEN],
    salt: [u8; SALT_LEN],
}

impl StreamEncryptor {
    pub fn new(passphrase: &str, chunk_size: u32) -> Result<Self> {
        let mut salt = [0u8; SALT_LEN];
        let mut base_nonce = [0u8; NONCE_LEN];
        rand::rngs::OsRng.fill_bytes(&mut salt);
        rand::rngs::OsRng.fill_bytes(&mut base_nonce);

        let key = derive_key(passphrase, &salt)?;
        let cipher = Aes256Gcm::new_from_slice(&key).context("failed to initialize AES-256-GCM")?;

        Ok(Self {
            cipher,
            chunk_size,
            base_nonce,
            salt,
        })
    }

    pub fn descriptor(&self) -> EncryptionDescriptor {
        EncryptionDescriptor {
            format_version: FORMAT_VERSION,
            chunk_size_bytes: self.chunk_size,
            base_nonce_hex: hex::encode(self.base_nonce),
            argon2_memory_kib: ARGON2_MEMORY_KIB,
            argon2_iterations: ARGON2_ITERATIONS,
            argon2_parallelism: ARGON2_PARALLELISM,
            salt_hex: hex::encode(self.salt),
        }
    }

    pub fn encode_header(&self) -> Vec<u8> {
        let mut header = Vec::with_capacity(4 + 1 + 4 + 4 + 4 + 4 + 1 + 1 + SALT_LEN + NONCE_LEN);
        header.extend_from_slice(MAGIC);
        header.push(FORMAT_VERSION);
        header.extend_from_slice(&self.chunk_size.to_le_bytes());
        header.extend_from_slice(&ARGON2_MEMORY_KIB.to_le_bytes());
        header.extend_from_slice(&ARGON2_ITERATIONS.to_le_bytes());
        header.extend_from_slice(&ARGON2_PARALLELISM.to_le_bytes());
        header.push(SALT_LEN as u8);
        header.push(NONCE_LEN as u8);
        header.extend_from_slice(&self.salt);
        header.extend_from_slice(&self.base_nonce);
        header
    }

    pub fn encrypt_chunk(&self, chunk_index: u64, plaintext: &[u8]) -> Result<Vec<u8>> {
        let nonce_bytes = nonce_for_chunk(&self.base_nonce, chunk_index);
        let aad = chunk_index.to_le_bytes();
        let ciphertext = self
            .cipher
            .encrypt(
                Nonce::from_slice(&nonce_bytes),
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| anyhow::anyhow!("encryption failure"))?;

        let frame_len: u32 = ciphertext
            .len()
            .try_into()
            .context("ciphertext frame too large for u32 length field")?;
        let mut frame = Vec::with_capacity(4 + ciphertext.len());
        frame.extend_from_slice(&frame_len.to_le_bytes());
        frame.extend_from_slice(&ciphertext);
        Ok(frame)
    }
}

pub(crate) fn nonce_for_chunk(base_nonce: &[u8; NONCE_LEN], index: u64) -> [u8; NONCE_LEN] {
    let mut nonce = *base_nonce;
    let mut counter_bytes = [0u8; 8];
    counter_bytes.copy_from_slice(&base_nonce[4..12]);
    let counter = u64::from_le_bytes(counter_bytes);
    let next = counter.wrapping_add(index);
    nonce[4..12].copy_from_slice(&next.to_le_bytes());
    nonce
}

fn derive_key(passphrase: &str, salt: &[u8; SALT_LEN]) -> Result<[u8; 32]> {
    let params = Params::new(
        ARGON2_MEMORY_KIB,
        ARGON2_ITERATIONS,
        ARGON2_PARALLELISM,
        Some(32),
    )
    .map_err(|error| anyhow::anyhow!("failed creating Argon2 parameters: {error}"))?;
    let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);

    let mut key = [0u8; 32];
    argon
        .hash_password_into(passphrase.as_bytes(), salt, &mut key)
        .map_err(|error| anyhow::anyhow!("failed deriving encryption key via Argon2id: {error}"))?;
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonce_changes_across_chunks() {
        let base = [1u8; NONCE_LEN];
        let first = nonce_for_chunk(&base, 0);
        let second = nonce_for_chunk(&base, 1);
        assert_ne!(first, second);
    }
}
