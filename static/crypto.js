/**
 * GhostDrop P2P - crypto.js
 * Cryptographic helpers: key derivation, AES-GCM encrypt/decrypt, SHA-256 hash.
 * All operations use the native Web Crypto API (SubtleCrypto) — no external deps.
 */

'use strict';

const PBKDF2_SALT    = 'ghostdrop_p2p_unified_mesh_2026';
const PBKDF2_ITER    = 50_000;
const PBKDF2_HASH    = 'SHA-256';
const AES_KEY_LENGTH = 256;

/**
 * Derive an AES-256-GCM CryptoKey from a raw room code string.
 *
 * @param {string} code  Raw 6-digit room code (digits only).
 * @returns {Promise<CryptoKey>}
 */
async function deriveKey(code) {
  const enc = new TextEncoder();
  const keyMaterial = await crypto.subtle.importKey(
    'raw',
    enc.encode(code),
    { name: 'PBKDF2' },
    false,
    ['deriveKey']
  );
  return crypto.subtle.deriveKey(
    {
      name:       'PBKDF2',
      salt:       enc.encode(PBKDF2_SALT),
      iterations: PBKDF2_ITER,
      hash:       PBKDF2_HASH,
    },
    keyMaterial,
    { name: 'AES-GCM', length: AES_KEY_LENGTH },
    false,
    ['encrypt', 'decrypt']
  );
}

/**
 * Encrypt a JSON-serialisable payload with AES-256-GCM.
 *
 * @param {CryptoKey} key
 * @param {*}         data  Any JSON-serialisable value.
 * @returns {Promise<{iv: number[], payload: number[]}>}
 */
async function encryptPayload(key, data) {
  const enc = new TextEncoder();
  const iv  = crypto.getRandomValues(new Uint8Array(12));
  const ciphertext = await crypto.subtle.encrypt(
    { name: 'AES-GCM', iv },
    key,
    enc.encode(JSON.stringify(data))
  );
  return {
    iv:      Array.from(iv),
    payload: Array.from(new Uint8Array(ciphertext)),
  };
}

/**
 * Decrypt an AES-256-GCM payload produced by {@link encryptPayload}.
 *
 * @param {CryptoKey}               key
 * @param {{iv: number[], payload: number[]}} encrypted
 * @returns {Promise<*|null>}  Parsed JSON value, or null on failure.
 */
async function decryptPayload(key, encrypted) {
  try {
    const iv   = new Uint8Array(encrypted.iv);
    const data = new Uint8Array(encrypted.payload);
    const decrypted = await crypto.subtle.decrypt(
      { name: 'AES-GCM', iv },
      key,
      data
    );
    return JSON.parse(new TextDecoder().decode(decrypted));
  } catch {
    return null;
  }
}

/**
 * Compute the SHA-256 hex digest of an ArrayBuffer.
 *
 * @param {ArrayBuffer} buffer
 * @returns {Promise<string>}  Lowercase hex string.
 */
async function computeHash(buffer) {
  const hashBuffer = await crypto.subtle.digest('SHA-256', buffer);
  return Array.from(new Uint8Array(hashBuffer))
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');
}