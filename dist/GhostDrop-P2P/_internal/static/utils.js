/**
 * GhostDrop P2P - utils.js
 * Generic helper functions shared across modules.
 */

'use strict';

/**
 * Format a byte count into a human-readable string.
 * @param {number} bytes
 * @param {number} [decimals=2]
 * @returns {string}
 */
function formatBytes(bytes, decimals = 2) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

/**
 * Return the current wall-clock time formatted as HH:MM.
 * @returns {string}
 */
function formatTime() {
  const d = new Date();
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

/**
 * Generate a random 6-digit room code in "NNN-NNN" format.
 * @returns {string}
 */
function generateRandomCode() {
  const num1 = Math.floor(100 + Math.random() * 900);
  const num2 = Math.floor(100 + Math.random() * 900);
  return `${num1}-${num2}`;
}

/**
 * Strip all non-digit characters from a code string.
 * @param {string} code
 * @returns {string}
 */
function cleanCode(code) {
  return String(code).replace(/[^0-9]/g, '');
}

/**
 * Format a digit string into "NNN-NNN" display format.
 * @param {string} digits
 * @returns {string}
 */
function formatCode(digits) {
  const clean = cleanCode(digits);
  if (clean.length <= 3) return clean;
  return clean.slice(0, 3) + '-' + clean.slice(3, 6);
}

/**
 * Escape a string for safe HTML insertion.
 * @param {string} str
 * @returns {string}
 */
function escapeHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/**
 * Return a deterministic color from a small palette based on a nickname string.
 * @param {string} nick
 * @returns {string}  CSS color value
 */
function getNicknameColor(nick) {
  const palette = [
    '#38bdf8', '#818cf8', '#a78bfa', '#f472b6',
    '#34d399', '#fbbf24', '#fb7185', '#2dd4bf', '#a3e635'
  ];
  let hash = 0;
  for (let i = 0; i < nick.length; i++) {
    hash = nick.charCodeAt(i) + ((hash << 5) - hash);
  }
  return palette[Math.abs(hash) % palette.length];
}

/**
 * Compute an estimated transfer ETA string.
 * @param {number} remaining  Bytes remaining.
 * @param {number} speedBps   Current speed in bytes/second.
 * @returns {string}
 */
function computeEta(remaining, speedBps) {
  if (!speedBps || speedBps <= 0) return '--';
  const secs = Math.ceil(remaining / speedBps);
  if (secs < 60) return `${secs}s`;
  const m = Math.floor(secs / 60);
  const s = secs % 60;
  return `${m}m ${s}s`;
}