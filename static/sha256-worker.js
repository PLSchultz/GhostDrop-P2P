/**
 * GhostDrop P2P - Incremental SHA-256 Web Worker
 * Computes SHA-256 incrementally over arbitrary-sized streams with O(1) memory.
 * Zero external dependencies; works in both HTTP (LAN) and HTTPS contexts.
 */
'use strict';

const K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
]);

const SHARED_W = new Uint32Array(64);

class Sha256Incremental {
  constructor() {
    this.h0 = 0x6a09e667;
    this.h1 = 0xbb67ae85;
    this.h2 = 0x3c6ef372;
    this.h3 = 0xa54ff53a;
    this.h4 = 0x510e527f;
    this.h5 = 0x9b05688c;
    this.h6 = 0x1f83d9ab;
    this.h7 = 0x5be0cd19;

    this.block = new Uint8Array(64);
    this.blockLen = 0;
    this.totalBytes = 0;
    this.cpuTimeMs = 0;
  }

  _compress(buf, offset) {
    const W = SHARED_W;
    for (let i = 0; i < 16; i++) {
      const j = offset + (i << 2);
      W[i] = ((buf[j] << 24) | (buf[j + 1] << 16) | (buf[j + 2] << 8) | buf[j + 3]) >>> 0;
    }
    for (let i = 16; i < 64; i++) {
      const w15 = W[i - 15];
      const w2 = W[i - 2];
      const s0 = ((w15 >>> 7) | (w15 << 25)) ^ ((w15 >>> 18) | (w15 << 14)) ^ (w15 >>> 3);
      const s1 = ((w2 >>> 17) | (w2 << 15)) ^ ((w2 >>> 19) | (w2 << 13)) ^ (w2 >>> 10);
      W[i] = (W[i - 16] + s0 + W[i - 7] + s1) | 0;
    }

    let a = this.h0;
    let b = this.h1;
    let c = this.h2;
    let d = this.h3;
    let e = this.h4;
    let f = this.h5;
    let g = this.h6;
    let h = this.h7;

    for (let i = 0; i < 64; i++) {
      const S1 = ((e >>> 6) | (e << 26)) ^ ((e >>> 11) | (e << 21)) ^ ((e >>> 25) | (e << 7));
      const ch = (e & f) ^ (~e & g);
      const temp1 = (h + S1 + ch + K[i] + W[i]) | 0;
      const S0 = ((a >>> 2) | (a << 30)) ^ ((a >>> 13) | (a << 19)) ^ ((a >>> 22) | (a << 10));
      const maj = (a & b) ^ (a & c) ^ (b & c);
      const temp2 = (S0 + maj) | 0;

      h = g;
      g = f;
      f = e;
      e = (d + temp1) | 0;
      d = c;
      c = b;
      b = a;
      a = (temp1 + temp2) | 0;
    }

    this.h0 = (this.h0 + a) | 0;
    this.h1 = (this.h1 + b) | 0;
    this.h2 = (this.h2 + c) | 0;
    this.h3 = (this.h3 + d) | 0;
    this.h4 = (this.h4 + e) | 0;
    this.h5 = (this.h5 + f) | 0;
    this.h6 = (this.h6 + g) | 0;
    this.h7 = (this.h7 + h) | 0;
  }

  update(data) {
    const t0 = performance.now();
    const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);
    const len = bytes.byteLength;
    this.totalBytes += len;

    let offset = 0;
    if (this.blockLen > 0) {
      const needed = 64 - this.blockLen;
      if (len < needed) {
        this.block.set(bytes, this.blockLen);
        this.blockLen += len;
        this.cpuTimeMs += performance.now() - t0;
        return;
      }
      this.block.set(bytes.subarray(0, needed), this.blockLen);
      this._compress(this.block, 0);
      this.blockLen = 0;
      offset = needed;
    }

    while (offset + 64 <= len) {
      this._compress(bytes, offset);
      offset += 64;
    }

    if (offset < len) {
      const rem = len - offset;
      this.block.set(bytes.subarray(offset), 0);
      this.blockLen = rem;
    }
    this.cpuTimeMs += performance.now() - t0;
  }

  digestHex() {
    const t0 = performance.now();
    const pad = this.block;
    let pos = this.blockLen;

    pad[pos++] = 0x80;
    if (pos > 56) {
      pad.fill(0, pos, 64);
      this._compress(pad, 0);
      pos = 0;
    }
    pad.fill(0, pos, 56);

    // 64-bit big-endian bit length (supports > 4 GB files)
    const totalBits = this.totalBytes * 8;
    const highBits = Math.floor(totalBits / 0x100000000);
    const lowBits = totalBits >>> 0;

    pad[56] = (highBits >>> 24) & 0xff;
    pad[57] = (highBits >>> 16) & 0xff;
    pad[58] = (highBits >>> 8) & 0xff;
    pad[59] = highBits & 0xff;
    pad[60] = (lowBits >>> 24) & 0xff;
    pad[61] = (lowBits >>> 16) & 0xff;
    pad[62] = (lowBits >>> 8) & 0xff;
    pad[63] = lowBits & 0xff;

    this._compress(pad, 0);
    this.cpuTimeMs += performance.now() - t0;

    const words = [this.h0, this.h1, this.h2, this.h3, this.h4, this.h5, this.h6, this.h7];
    let hex = '';
    for (let i = 0; i < 8; i++) {
      hex += (words[i] >>> 0).toString(16).padStart(8, '0');
    }
    return hex;
  }
}

// Worker message loop (when running inside a Web Worker)
if (typeof self !== 'undefined' && typeof self.postMessage === 'function') {
  const jobs = new Map();

  self.onmessage = (event) => {
    const msg = event.data;
    if (!msg || !msg.type) return;

    const { type, jobId } = msg;

    if (type === 'init') {
      jobs.set(jobId, new Sha256Incremental());
      return;
    }

    if (type === 'update') {
      const hasher = jobs.get(jobId);
      if (!hasher) return;
      if (msg.buffer) {
        const view = msg.byteLength !== undefined
          ? new Uint8Array(msg.buffer, msg.byteOffset || 0, msg.byteLength)
          : new Uint8Array(msg.buffer);
        hasher.update(view);
      }
      return;
    }

    if (type === 'finalize') {
      const hasher = jobs.get(jobId);
      if (!hasher) {
        self.postMessage({ type: 'error', jobId, error: 'Job not found' });
        return;
      }
      const hash = hasher.digestHex();
      const cpuTimeMs = hasher.cpuTimeMs;
      const totalBytes = hasher.totalBytes;
      jobs.delete(jobId);
      self.postMessage({ type: 'result', jobId, hash, cpuTimeMs, totalBytes });
      return;
    }

    if (type === 'abort') {
      jobs.delete(jobId);
    }
  };
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { Sha256Incremental };
}
