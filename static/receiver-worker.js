/**
 * GhostDrop P2P - Dedicated Receiver Web Worker
 * Handles binary frame decoding, sequence/offset validation, incremental SHA-256,
 * OPFS SyncAccessHandle / File System Access streaming, and credit-based flow control.
 * Zero full-file RAM accumulation when OPFS or File System Access is available.
 */
'use strict';

if (typeof importScripts === 'function') {
  importScripts('sha256-worker.js');
}

const FRAME_MAGIC = 0x47; // 'G'
const MIN_WINDOW_BYTES = 512 * 1024;       // 512 KB
const INITIAL_WINDOW_BYTES = 2 * 1024 * 1024; // 2 MB
const MAX_WINDOW_BYTES = 16 * 1024 * 1024; // 16 MB

const textDecoder = new TextDecoder();

function decodeDataFrame(arrayBuffer) {
  const view = new Uint8Array(arrayBuffer);
  // Header: [1B magic][1B idLen][idLen B transferId][4B seq][8B offset (2xUint32)]
  if (view.byteLength < 15 || view[0] !== FRAME_MAGIC) {
    return null;
  }
  const idLen = view[1];
  const headerLen = 2 + idLen + 12;
  if (view.byteLength < headerLen) {
    return null;
  }

  const transferId = textDecoder.decode(view.subarray(2, 2 + idLen));
  const dv = new DataView(arrayBuffer, 2 + idLen, 12);
  const seq = dv.getUint32(0, false);
  const offHigh = dv.getUint32(4, false);
  const offLow = dv.getUint32(8, false);
  const offset = offHigh * 0x100000000 + offLow;
  const payloadView = view.subarray(headerLen);

  return { transferId, seq, offset, payloadView };
}

class ReceiverJobManager {
  constructor(postFn, Sha256Class) {
    this.postFn = postFn;
    this.Sha256Class = Sha256Class;
    this.jobs = new Map();
    this.opfsRoot = null;
  }

  async _getOpfsRoot() {
    if (this.opfsRoot) return this.opfsRoot;
    if (typeof navigator !== 'undefined' && navigator.storage && typeof navigator.storage.getDirectory === 'function') {
      this.opfsRoot = await navigator.storage.getDirectory();
      return this.opfsRoot;
    }
    return null;
  }

  async initJob(msg) {
    const { transferId, name, size, mimeType, fileHandle, initialWindowBytes } = msg;
    const windowBytes = initialWindowBytes || INITIAL_WINDOW_BYTES;

    const job = {
      transferId,
      name,
      size,
      mimeType: mimeType || 'application/octet-stream',
      hasher: new this.Sha256Class(),
      expectedSeq: 0,
      expectedOffset: 0,
      bytesReceived: 0,
      bytesProcessed: 0,
      bytesWritten: 0,
      lastCreditAcked: 0,
      lastCreditTime: performance.now(),
      windowBytes,
      hashTimeMs: 0,
      storageTimeMs: 0,
      startTime: performance.now(),
      lastSpeedTime: performance.now(),
      lastSpeedBytes: 0,
      writeSpeedBps: 0,
      backend: 'blob-bounded',
      writable: null,
      syncHandle: null,
      opfsHandle: null,
      opfsTempName: null,
      chunks: [],
      aborted: false,
      writeChain: Promise.resolve(),
      pendingFinalize: null
    };

    try {
      if (fileHandle && typeof fileHandle.createWritable === 'function') {
        job.writable = await fileHandle.createWritable();
        job.backend = 'fs-access';
      } else {
        const root = await this._getOpfsRoot();
        if (root) {
          // Check storage quota before allocating OPFS file
          if (navigator.storage && typeof navigator.storage.estimate === 'function') {
            try {
              const est = await navigator.storage.estimate();
              if (typeof est.quota === 'number' && typeof est.usage === 'number') {
                const available = est.quota - est.usage;
                if (available < size + 1024 * 1024) {
                  throw new Error(`Espaço insuficiente no dispositivo (${Math.round(available / (1024 * 1024))} MB livres para ${Math.round(size / (1024 * 1024))} MB)`);
                }
              }
            } catch (quotaErr) {
              if (quotaErr.message && quotaErr.message.includes('Espaço insuficiente')) {
                throw quotaErr;
              }
            }
          }

          const safeId = transferId.replace(/[^a-zA-Z0-9_-]/g, '_');
          const tempName = `gd_tmp_${safeId}_${Date.now()}`;
          const opfsHandle = await root.getFileHandle(tempName, { create: true });

          if (typeof opfsHandle.createSyncAccessHandle === 'function') {
            job.syncHandle = await opfsHandle.createSyncAccessHandle();
            job.opfsHandle = opfsHandle;
            job.opfsTempName = tempName;
            job.backend = 'opfs-sync';
          } else if (typeof opfsHandle.createWritable === 'function') {
            job.writable = await opfsHandle.createWritable();
            job.opfsHandle = opfsHandle;
            job.opfsTempName = tempName;
            job.backend = 'opfs-writable';
          } else {
            await root.removeEntry(tempName).catch(() => {});
          }
        }
      }
    } catch (err) {
      if (err.name === 'QuotaExceededError' || (err.message && err.message.includes('Espaço insuficiente'))) {
        this.postFn({ type: 'init_error', transferId, error: err.message || 'QuotaExceededError' });
        return;
      }
      console.warn('[ReceiverWorker] Falling back to blob-bounded:', err);
      job.backend = 'blob-bounded';
    }

    this.jobs.set(transferId, job);
    this.postFn({
      type: 'ready',
      transferId,
      backend: job.backend,
      windowBytes: job.windowBytes
    });
  }

  handleChunk(arrayBuffer) {
    const frame = decodeDataFrame(arrayBuffer);
    if (!frame) return;

    const job = this.jobs.get(frame.transferId);
    if (!job || job.aborted) return;

    const { seq, offset, payloadView } = frame;
    const len = payloadView.byteLength;

    if (seq !== job.expectedSeq || offset !== job.expectedOffset) {
      this._failJob(job, `Erro de sequência/offset: esperado seq=${job.expectedSeq} off=${job.expectedOffset}, recebido seq=${seq} off=${offset}`);
      return;
    }

    job.expectedSeq += 1;
    job.expectedOffset += len;
    job.bytesReceived += len;

    // 1. Incremental SHA-256 directly on transferred buffer (zero copy)
    const tHash0 = performance.now();
    job.hasher.update(payloadView);
    job.hashTimeMs += performance.now() - tHash0;
    job.bytesProcessed += len;

    // 2. Storage write
    if (job.backend === 'opfs-sync') {
      const tWrite0 = performance.now();
      try {
        job.syncHandle.write(payloadView, { at: offset });
      } catch (err) {
        this._failJob(job, `Erro de gravação OPFS: ${err.message || err}`);
        return;
      }
      job.storageTimeMs += performance.now() - tWrite0;
      job.bytesWritten += len;
      this._maybeEmitCredit(job, seq);
    } else if (job.backend === 'fs-access' || job.backend === 'opfs-writable') {
      const copy = new Uint8Array(len);
      copy.set(payloadView);
      job.writeChain = job.writeChain.then(async () => {
        if (job.aborted) return;
        const tWrite0 = performance.now();
        await job.writable.write({ type: 'write', position: offset, data: copy });
        job.storageTimeMs += performance.now() - tWrite0;
        job.bytesWritten += len;
        this._maybeEmitCredit(job, seq);
      }).catch((err) => {
        this._failJob(job, `Erro de gravação: ${err.message || err}`);
      });
    } else {
      // blob-bounded fallback (insecure HTTP LAN or unsupported browser)
      const tWrite0 = performance.now();
      const copy = new Uint8Array(len);
      copy.set(payloadView);
      job.chunks.push(copy);
      job.storageTimeMs += performance.now() - tWrite0;
      job.bytesWritten += len;
      this._maybeEmitCredit(job, seq);
    }
  }

  _maybeEmitCredit(job, seq, force = false) {
    const now = performance.now();
    const unacked = job.bytesWritten - job.lastCreditAcked;
    const creditStep = Math.max(64 * 1024, Math.floor(job.windowBytes / 4));
    const dtSpeed = now - job.lastSpeedTime;

    if (dtSpeed >= 250) {
      const secs = Math.max(0.001, dtSpeed / 1000);
      job.writeSpeedBps = (job.bytesWritten - job.lastSpeedBytes) / secs;
      job.lastSpeedTime = now;
      job.lastSpeedBytes = job.bytesWritten;
    }

    if (force || unacked >= creditStep || job.bytesWritten >= job.size || (unacked > 0 && now - job.lastCreditTime >= 80)) {
      // Adaptive window: scale based on write throughput, keeping memory bounded
      if (job.writeSpeedBps > 0) {
        const target = Math.round(job.writeSpeedBps * 0.35); // ~350ms of receiver capacity in flight
        job.windowBytes = Math.max(MIN_WINDOW_BYTES, Math.min(MAX_WINDOW_BYTES, target));
      }

      job.lastCreditAcked = job.bytesWritten;
      job.lastCreditTime = now;

      this.postFn({
        type: 'credit',
        transferId: job.transferId,
        ackedOffset: job.bytesWritten,
        ackedSeq: seq,
        windowBytes: job.windowBytes,
        metrics: {
          bytesReceived: job.bytesReceived,
          bytesProcessed: job.bytesProcessed,
          bytesWritten: job.bytesWritten,
          hashTimeMs: job.hashTimeMs,
          storageTimeMs: job.storageTimeMs,
          writeSpeedBps: job.writeSpeedBps
        }
      });
    }
  }

  async finalizeJob(msg) {
    const { transferId, expectedSha256, expectedSize } = msg;
    const job = this.jobs.get(transferId);
    if (!job || job.aborted) return;

    try {
      await job.writeChain;
      if (job.aborted) return;

      this._maybeEmitCredit(job, Math.max(0, job.expectedSeq - 1), true);

      const tHash0 = performance.now();
      const calculatedHash = job.hasher.digestHex().toLowerCase();
      job.hashTimeMs += performance.now() - tHash0;

      const expHash = (expectedSha256 || '').toLowerCase();
      const sizeOk = job.bytesWritten === expectedSize && job.bytesProcessed === expectedSize;
      const hashOk = Boolean(expHash) && calculatedHash === expHash;

      if (!sizeOk || !hashOk) {
        const reason = !sizeOk ? 'size_mismatch' : 'hash_mismatch';
        await this._cleanupJobStorage(job);
        this.jobs.delete(transferId);
        this.postFn({
          type: 'finalized',
          transferId,
          ok: false,
          reason,
          sha256: calculatedHash,
          metrics: {
            bytesReceived: job.bytesReceived,
            bytesProcessed: job.bytesProcessed,
            bytesWritten: job.bytesWritten,
            hashTimeMs: job.hashTimeMs,
            storageTimeMs: job.storageTimeMs,
            backend: job.backend
          }
        });
        return;
      }

      const tClose0 = performance.now();
      let resultPayload = {
        type: 'finalized',
        transferId,
        ok: true,
        sha256: calculatedHash,
        backend: job.backend,
        opfsTempName: job.opfsTempName,
        metrics: {
          bytesReceived: job.bytesReceived,
          bytesProcessed: job.bytesProcessed,
          bytesWritten: job.bytesWritten,
          hashTimeMs: job.hashTimeMs,
          storageTimeMs: job.storageTimeMs,
          backend: job.backend
        }
      };

      if (job.backend === 'opfs-sync') {
        job.syncHandle.flush();
        job.syncHandle.close();
        job.syncHandle = null;
        resultPayload.file = await job.opfsHandle.getFile();
      } else if (job.backend === 'opfs-writable') {
        await job.writable.close();
        job.writable = null;
        resultPayload.file = await job.opfsHandle.getFile();
      } else if (job.backend === 'fs-access') {
        await job.writable.close();
        job.writable = null;
        resultPayload.savedToDisk = true;
      } else {
        resultPayload.blob = new Blob(job.chunks, { type: job.mimeType });
        job.chunks = [];
      }
      resultPayload.metrics.storageTimeMs += performance.now() - tClose0;

      this.jobs.delete(transferId);
      this.postFn(resultPayload);
    } catch (err) {
      await this._cleanupJobStorage(job);
      this.jobs.delete(transferId);
      this.postFn({
        type: 'finalized',
        transferId,
        ok: false,
        reason: err.message || 'finalize_error'
      });
    }
  }

  async _failJob(job, reason) {
    if (job.aborted) return;
    job.aborted = true;
    await this._cleanupJobStorage(job);
    this.jobs.delete(job.transferId);
    this.postFn({ type: 'error', transferId: job.transferId, error: reason });
  }

  async abortJob(transferId) {
    const job = this.jobs.get(transferId);
    if (!job) return;
    job.aborted = true;
    await this._cleanupJobStorage(job);
    this.jobs.delete(transferId);
  }

  async removeOpfsTemp(tempFileName) {
    if (!tempFileName) return;
    try {
      const root = await this._getOpfsRoot();
      if (root) {
        await root.removeEntry(tempFileName);
      }
    } catch (e) { }
  }

  async _cleanupJobStorage(job) {
    try {
      if (job.syncHandle) {
        job.syncHandle.close();
        job.syncHandle = null;
      }
    } catch (e) { }
    try {
      if (job.writable) {
        await job.writable.abort();
        job.writable = null;
      }
    } catch (e) { }
    if (job.opfsTempName) {
      await this.removeOpfsTemp(job.opfsTempName);
      job.opfsTempName = null;
    }
    job.chunks = [];
  }
}

if (typeof self !== 'undefined' && typeof self.postMessage === 'function') {
  const manager = new ReceiverJobManager((msg) => self.postMessage(msg), self.Sha256Incremental);

  self.onmessage = (event) => {
    const msg = event.data;
    if (!msg || !msg.type) return;

    if (msg.type === 'chunk') {
      manager.handleChunk(msg.buffer);
    } else if (msg.type === 'init') {
      manager.initJob(msg);
    } else if (msg.type === 'finalize') {
      manager.finalizeJob(msg);
    } else if (msg.type === 'abort') {
      manager.abortJob(msg.transferId);
    } else if (msg.type === 'remove_opfs_temp') {
      manager.removeOpfsTemp(msg.tempFileName);
    }
  };
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { ReceiverJobManager, decodeDataFrame, INITIAL_WINDOW_BYTES, MIN_WINDOW_BYTES, MAX_WINDOW_BYTES };
}
