/**
 * GhostDrop P2P - transfer.js
 * High-performance P2P file transfer engine:
 * - Incremental SHA-256 via Web Worker (parallel with transmission, O(1) memory)
 * - Binary chunk framing for multi-transfer multiplexing per DataChannel
 * - Adaptive chunk sizing respecting RTCPeerConnection.sctp.maxMessageSize
 * - Read-ahead I/O pipeline overlapping Blob reads with DataChannel sends
 * - Event-driven DataChannel backpressure (bufferedamountlow + safety fallback)
 * - File System Access API streaming sink with Blob fallback
 * - Per-transfer state management (zero global transfer state)
 * - Real-time instrumentation & RTCPeerConnection.getStats() diagnostics
 */
'use strict';

(function (globalScope) {
  const FRAME_MAGIC = 0x47; // 'G'
  const DEFAULT_MAX_CHUNK = 256 * 1024; // 256 KB upper cap
  const FALLBACK_MAX_MSG_SIZE = 64 * 1024; // 64 KB safe SCTP fallback
  const MIN_CHUNK_SIZE = 16 * 1024; // 16 KB
  const INITIAL_CHUNK_SIZE = 64 * 1024; // 64 KB for fast first-chunk start
  const READ_BLOCK_SIZE = 2 * 1024 * 1024; // 2 MB read-ahead block

  const BUFFER_HIGH = 4 * 1024 * 1024; // 4 MB high-water mark
  const BUFFER_LOW = 1 * 1024 * 1024;  // 1 MB low-water mark (bufferedAmountLowThreshold)
  const ACK_TIMEOUT_MS = 60_000;       // 60s for receiver disk flush & hash finalization

  const textEncoder = new TextEncoder();
  const textDecoder = new TextDecoder();

  let transferSeq = 0;

  function generateTransferId(peerId) {
    transferSeq += 1;
    return `tx_${peerId}_${transferSeq}_${Date.now().toString(36)}`;
  }

  // =========================================================================
  // 1. Binary Chunk Framer (Multiplexing multiple transfers per DataChannel)
  // =========================================================================
  const headerCache = new Map();
  const idDecodeCache = new Map();

  function getHeaderBytes(transferId) {
    let hdr = headerCache.get(transferId);
    if (!hdr) {
      const idBytes = textEncoder.encode(transferId);
      if (idBytes.length > 255) {
        throw new Error(`transferId too long (${idBytes.length} bytes)`);
      }
      hdr = new Uint8Array(2 + idBytes.length);
      hdr[0] = FRAME_MAGIC;
      hdr[1] = idBytes.length;
      hdr.set(idBytes, 2);
      headerCache.set(transferId, hdr);
    }
    return hdr;
  }

  function clearHeaderCache(transferId) {
    headerCache.delete(transferId);
  }

  function encodeChunkFrame(transferId, payloadView) {
    const hdr = getHeaderBytes(transferId);
    const frame = new Uint8Array(hdr.length + payloadView.byteLength);
    frame.set(hdr, 0);
    frame.set(payloadView, hdr.length);
    return frame.buffer;
  }

  function decodeChunkFrame(arrayBuffer) {
    const view = new Uint8Array(arrayBuffer);
    if (view.byteLength < 3 || view[0] !== FRAME_MAGIC) {
      return null;
    }
    const idLen = view[1];
    const headerLen = 2 + idLen;
    if (view.byteLength < headerLen) {
      return null;
    }
    const idSlice = view.subarray(2, headerLen);
    // Fast cache lookup by string key only if needed; TextDecoder is fast for ~25 bytes
    const transferId = textDecoder.decode(idSlice);
    const payloadView = view.subarray(headerLen);
    return { transferId, payloadView };
  }

  // =========================================================================
  // 2. Incremental SHA-256 Worker Client
  // =========================================================================
  class Sha256WorkerClient {
    constructor(workerPath = 'sha256-worker.js') {
      this.workerPath = workerPath;
      this.worker = null;
      this.pendingFinalizes = new Map();
      this.fallbackJobs = null;
      this._initWorker();
    }

    _initWorker() {
      if (typeof Worker === 'undefined') {
        this.fallbackJobs = new Map();
        return;
      }
      try {
        this.worker = new Worker(this.workerPath);
        this.worker.onmessage = (event) => {
          const msg = event.data;
          if (!msg || !msg.jobId) return;
          const pending = this.pendingFinalizes.get(msg.jobId);
          if (!pending) return;
          this.pendingFinalizes.delete(msg.jobId);
          if (msg.type === 'result') {
            pending.resolve({
              hash: msg.hash,
              cpuTimeMs: msg.cpuTimeMs,
              totalBytes: msg.totalBytes
            });
          } else if (msg.type === 'error') {
            pending.reject(new Error(msg.error || 'SHA-256 worker error'));
          }
        };
        this.worker.onerror = (err) => {
          console.warn('[Sha256WorkerClient] Worker error:', err);
        };
      } catch (e) {
        console.warn('[Sha256WorkerClient] Failed to spawn Worker, using fallback:', e);
        this.worker = null;
        this.fallbackJobs = new Map();
      }
    }

    init(jobId) {
      if (this.worker) {
        this.worker.postMessage({ type: 'init', jobId });
      } else if (this.fallbackJobs && globalScope.Sha256Incremental) {
        this.fallbackJobs.set(jobId, new globalScope.Sha256Incremental());
      }
    }

    update(jobId, data, transferBuffer = false) {
      if (this.worker) {
        if (data instanceof ArrayBuffer) {
          if (transferBuffer) {
            this.worker.postMessage({ type: 'update', jobId, buffer: data }, [data]);
          } else {
            const copy = data.slice(0);
            this.worker.postMessage({ type: 'update', jobId, buffer: copy }, [copy]);
          }
        } else if (ArrayBuffer.isView(data)) {
          if (transferBuffer && data.byteOffset === 0 && data.byteLength === data.buffer.byteLength) {
            this.worker.postMessage({ type: 'update', jobId, buffer: data.buffer }, [data.buffer]);
          } else {
            const copy = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
            this.worker.postMessage({ type: 'update', jobId, buffer: copy }, [copy]);
          }
        }
      } else if (this.fallbackJobs) {
        const hasher = this.fallbackJobs.get(jobId);
        if (hasher) {
          hasher.update(data instanceof ArrayBuffer ? new Uint8Array(data) : data);
        }
      }
    }

    finalize(jobId) {
      if (this.worker) {
        return new Promise((resolve, reject) => {
          this.pendingFinalizes.set(jobId, { resolve, reject });
          this.worker.postMessage({ type: 'finalize', jobId });
        });
      }
      if (this.fallbackJobs) {
        const hasher = this.fallbackJobs.get(jobId);
        this.fallbackJobs.delete(jobId);
        if (!hasher) return Promise.reject(new Error('Job not found'));
        return Promise.resolve({
          hash: hasher.digestHex(),
          cpuTimeMs: hasher.cpuTimeMs,
          totalBytes: hasher.totalBytes
        });
      }
      return Promise.reject(new Error('No SHA-256 backend available'));
    }

    abort(jobId) {
      if (this.worker) {
        this.worker.postMessage({ type: 'abort', jobId });
      }
      if (this.fallbackJobs) {
        this.fallbackJobs.delete(jobId);
      }
      const pending = this.pendingFinalizes.get(jobId);
      if (pending) {
        this.pendingFinalizes.delete(jobId);
        pending.reject(new Error('Hashing aborted'));
      }
    }
  }

  // =========================================================================
  // 3. Per-DataChannel Backpressure Coordinator
  // =========================================================================
  const channelCoordinators = new WeakMap();

  class DataChannelBackpressure {
    constructor(dc) {
      this.dc = dc;
      this.waiters = new Set();
      try {
        dc.bufferedAmountLowThreshold = BUFFER_LOW;
      } catch (e) { }

      this._onLow = () => this._notifyWaiters();
      this._onClose = () => this._failWaiters(new Error('DataChannel closed'));
      this._onError = () => this._failWaiters(new Error('DataChannel error'));

      dc.addEventListener('bufferedamountlow', this._onLow);
      dc.addEventListener('close', this._onClose);
      dc.addEventListener('error', this._onError);
    }

    static forChannel(dc) {
      let coord = channelCoordinators.get(dc);
      if (!coord) {
        coord = new DataChannelBackpressure(dc);
        channelCoordinators.set(dc, coord);
      }
      return coord;
    }

    _notifyWaiters() {
      for (const w of Array.from(this.waiters)) {
        if (this.dc.bufferedAmount <= w.threshold) {
          this.waiters.delete(w);
          w.cleanup();
          w.resolve();
        }
      }
    }

    _failWaiters(err) {
      for (const w of Array.from(this.waiters)) {
        this.waiters.delete(w);
        w.cleanup();
        w.reject(err);
      }
    }

    waitUntilBelow(threshold, abortSignal) {
      if (this.dc.readyState !== 'open') {
        return Promise.reject(new Error(`DataChannel not open (${this.dc.readyState})`));
      }
      if (abortSignal && abortSignal.aborted) {
        return Promise.reject(new Error('Transfer cancelled'));
      }
      if (this.dc.bufferedAmount <= threshold) {
        return Promise.resolve();
      }

      try {
        // Ensure threshold triggers bufferedamountlow when draining
        const targetThreshold = Math.min(threshold, BUFFER_LOW);
        if (this.dc.bufferedAmountLowThreshold !== targetThreshold) {
          this.dc.bufferedAmountLowThreshold = targetThreshold;
        }
      } catch (e) { }

      return new Promise((resolve, reject) => {
        let safetyTimer = null;

        const onAbort = () => {
          this.waiters.delete(waiter);
          cleanup();
          reject(new Error('Transfer cancelled'));
        };

        const cleanup = () => {
          if (safetyTimer) {
            clearInterval(safetyTimer);
            safetyTimer = null;
          }
          if (abortSignal) {
            abortSignal.removeEventListener('abort', onAbort);
          }
        };

        const waiter = { threshold, resolve, reject, cleanup };
        this.waiters.add(waiter);

        if (abortSignal) {
          abortSignal.addEventListener('abort', onAbort, { once: true });
        }

        // Safety net in case browser misses bufferedamountlow event
        safetyTimer = setInterval(() => {
          if (!this.waiters.has(waiter)) {
            cleanup();
            return;
          }
          if (this.dc.readyState !== 'open') {
            this.waiters.delete(waiter);
            cleanup();
            reject(new Error('DataChannel closed while waiting for buffer'));
            return;
          }
          if (this.dc.bufferedAmount <= threshold) {
            this.waiters.delete(waiter);
            cleanup();
            resolve();
          }
        }, 120);

        // Re-check immediately to avoid race between check and registration
        if (this.dc.bufferedAmount <= threshold) {
          this.waiters.delete(waiter);
          cleanup();
          resolve();
        }
      });
    }
  }

  // =========================================================================
  // 4. Receiver Storage Sinks (File System Access API + Blob Fallback)
  // =========================================================================
  class FileSystemFileSink {
    constructor(fileHandle, writable) {
      this.kind = 'fs-access';
      this.fileHandle = fileHandle;
      this.writable = writable;
      this.writeChain = Promise.resolve();
      this.closed = false;
    }

    write(uint8Array) {
      // Copy slice into independent buffer because underlying frame buffer may be reused/GC'd
      const copy = new Uint8Array(uint8Array.byteLength);
      copy.set(uint8Array);
      this.writeChain = this.writeChain.then(() => {
        if (!this.closed) {
          return this.writable.write(copy);
        }
      });
      return this.writeChain;
    }

    async close() {
      await this.writeChain;
      if (!this.closed) {
        this.closed = true;
        await this.writable.close();
      }
      return { blob: null, blobUrl: null, savedToDisk: true };
    }

    async abort() {
      this.closed = true;
      try {
        await this.writable.abort();
      } catch (e) { }
    }
  }

  class BlobFileSink {
    constructor(mimeType) {
      this.kind = 'blob';
      this.mimeType = mimeType || 'application/octet-stream';
      this.chunks = [];
    }

    write(uint8Array) {
      // Copy payload bytes out of the framed ArrayBuffer so header bytes aren't retained
      const copy = new Uint8Array(uint8Array.byteLength);
      copy.set(uint8Array);
      this.chunks.push(copy);
      return Promise.resolve();
    }

    async close() {
      const blob = new Blob(this.chunks, { type: this.mimeType });
      this.chunks = [];
      return { blob, blobUrl: null, savedToDisk: false };
    }

    async abort() {
      this.chunks = [];
    }
  }

  async function createReceiverSink(fileName, mimeType) {
    if (typeof window !== 'undefined' && typeof window.showSaveFilePicker === 'function' && window.isSecureContext) {
      try {
        const handle = await window.showSaveFilePicker({ suggestedName: fileName });
        const writable = await handle.createWritable();
        return { sink: new FileSystemFileSink(handle, writable), userCancelled: false };
      } catch (err) {
        if (err && err.name === 'AbortError') {
          return { sink: null, userCancelled: true };
        }
        // SecurityError / NotAllowedError / other -> fallback silently to Blob sink
        console.warn('[ReceiverSink] File System Access fallback to Blob:', err.message || err);
      }
    }
    return { sink: new BlobFileSink(mimeType), userCancelled: false };
  }

  // =========================================================================
  // 5. WebRTC Stats & Bottleneck Diagnostics
  // =========================================================================
  async function collectRtcStats(pc) {
    if (!pc || typeof pc.getStats !== 'function') return null;
    try {
      const report = await pc.getStats();
      let selectedPair = null;
      const localCandidates = new Map();
      const remoteCandidates = new Map();
      let transportSelectedId = null;

      report.forEach((stat) => {
        if (stat.type === 'transport' && stat.selectedCandidatePairId) {
          transportSelectedId = stat.selectedCandidatePairId;
        } else if (stat.type === 'local-candidate') {
          localCandidates.set(stat.id, stat);
        } else if (stat.type === 'remote-candidate') {
          remoteCandidates.set(stat.id, stat);
        }
      });

      report.forEach((stat) => {
        if (stat.type === 'candidate-pair') {
          if (stat.id === transportSelectedId || (stat.state === 'succeeded' && stat.nominated)) {
            selectedPair = stat;
          }
        }
      });

      if (!selectedPair) return null;

      const local = localCandidates.get(selectedPair.localCandidateId);
      const remote = remoteCandidates.get(selectedPair.remoteCandidateId);
      const isRelay = (local && local.candidateType === 'relay') || (remote && remote.candidateType === 'relay');

      return {
        rttMs: typeof selectedPair.currentRoundTripTime === 'number'
          ? Math.round(selectedPair.currentRoundTripTime * 1000 * 10) / 10
          : null,
        availableOutgoingBitrate: selectedPair.availableOutgoingBitrate || null,
        localCandidateType: local ? local.candidateType : 'unknown',
        remoteCandidateType: remote ? remote.candidateType : 'unknown',
        protocol: local ? local.protocol : 'unknown',
        usingTurnRelay: Boolean(isRelay),
        bytesSent: selectedPair.bytesSent || 0,
        bytesReceived: selectedPair.bytesReceived || 0
      };
    } catch (e) {
      return null;
    }
  }

  function classifyBottleneck(metrics, rtcStats) {
    if (!metrics) return 'unknown';
    const totalMs = metrics.totalDurationMs || 1;
    const bpRatio = (metrics.backpressureWaitMs || 0) / totalMs;

    if (rtcStats && rtcStats.usingTurnRelay && bpRatio > 0.25) {
      return 'network_turn_relay';
    }
    if (bpRatio > 0.35 || (rtcStats && rtcStats.rttMs !== null && rtcStats.rttMs > 80 && bpRatio > 0.15)) {
      return 'network_congestion_or_rtt';
    }
    if ((metrics.sha256DurationMs || 0) > totalMs * 0.8 && bpRatio < 0.1) {
      return 'local_cpu_sha256';
    }
    if (bpRatio < 0.05 && (metrics.peakBufferedAmount || 0) < BUFFER_LOW) {
      return 'local_io_or_cpu';
    }
    return 'balanced_p2p';
  }

  // =========================================================================
  // 6. Adaptive Chunk Sizing Helper
  // =========================================================================
  function resolveMaxPayloadSize(pc, transferId) {
    const hdrLen = getHeaderBytes(transferId).length;
    let sctpMax = FALLBACK_MAX_MSG_SIZE;
    if (pc && pc.sctp && typeof pc.sctp.maxMessageSize === 'number' && pc.sctp.maxMessageSize > 0) {
      sctpMax = pc.sctp.maxMessageSize;
    }
    const capped = Math.min(sctpMax, DEFAULT_MAX_CHUNK);
    return Math.max(MIN_CHUNK_SIZE - hdrLen, capped - hdrLen);
  }

  // =========================================================================
  // 7. TransferManager (Per-Transfer State, Send & Receive Pipelines)
  // =========================================================================
  class TransferManager {
    constructor(options) {
      this.myPeerId = options.myPeerId;
      this.getPeer = options.getPeer;
      this.sendPeerMessage = options.sendPeerMessage;
      this.callbacks = options.callbacks || {};
      this.hashClient = new Sha256WorkerClient(options.workerPath || 'sha256-worker.js');

      this.transfers = new Map(); // transferId -> transferState
      this.completedMetrics = [];
    }

    getTransfer(transferId) {
      return this.transfers.get(transferId) || null;
    }

    getActiveTransfers() {
      return Array.from(this.transfers.values()).filter(
        t => t.status === 'offered' || t.status === 'accepted' || t.status === 'transferring' || t.status === 'validating'
      );
    }

    createSendOffer(file, peerId, peerNickname, senderNickname) {
      const transferId = generateTransferId(this.myPeerId);
      const fileId = `file_${transferSeq}_${Date.now()}`;
      const abortController = new AbortController();

      const state = {
        transferId,
        fileId,
        direction: 'send',
        peerId,
        peerNickname,
        senderNickname,
        file,
        name: file.name,
        size: file.size,
        mimeType: file.type || 'application/octet-stream',
        status: 'offered',
        bytesTransferred: 0,
        chunksCount: 0,
        sha256: null,
        abortController,
        ackResolver: null,
        ackRejecter: null,
        metrics: {
          transferId,
          direction: 'send',
          fileName: file.name,
          fileSize: file.size,
          offeredAt: performance.now(),
          acceptedAt: null,
          firstByteAt: null,
          completedAt: null,
          acceptToFirstByteMs: null,
          totalDurationMs: null,
          avgThroughputMBps: 0,
          instantThroughputMBps: 0,
          chunksCount: 0,
          avgChunkSizeBytes: 0,
          peakBufferedAmount: 0,
          backpressureWaitMs: 0,
          sha256DurationMs: 0,
          validationDurationMs: null,
          rtcStats: null,
          bottleneck: 'unknown',
          lastSampleTime: 0,
          lastSampleBytes: 0
        }
      };

      this.transfers.set(transferId, state);
      return state;
    }

    registerReceiveTransfer(offer, sink) {
      const abortController = new AbortController();
      const state = {
        transferId: offer.transferId,
        fileId: offer.fileId,
        direction: 'receive',
        peerId: offer.senderPeerId,
        peerNickname: offer.sender || 'Participante',
        name: offer.name,
        size: offer.size,
        mimeType: offer.mimeType || 'application/octet-stream',
        status: 'accepted',
        bytesTransferred: 0,
        chunksCount: 0,
        sha256: null,
        expectedSha256: null,
        pendingCompleteMsg: null,
        finalizing: false,
        sink,
        abortController,
        metrics: {
          transferId: offer.transferId,
          direction: 'receive',
          fileName: offer.name,
          fileSize: offer.size,
          acceptedAt: performance.now(),
          firstByteAt: null,
          completedAt: null,
          acceptToFirstByteMs: null,
          totalDurationMs: null,
          avgThroughputMBps: 0,
          instantThroughputMBps: 0,
          chunksCount: 0,
          avgChunkSizeBytes: 0,
          peakBufferedAmount: 0,
          backpressureWaitMs: 0,
          sha256DurationMs: 0,
          validationDurationMs: 0,
          rtcStats: null,
          bottleneck: 'unknown',
          lastSampleTime: performance.now(),
          lastSampleBytes: 0
        }
      };

      this.transfers.set(offer.transferId, state);
      this.hashClient.init(offer.transferId);
      return state;
    }

    async handleOfferAccepted(fromPeerId, msg) {
      const state = this.transfers.get(msg.transferId);
      if (!state || state.direction !== 'send' || state.peerId !== fromPeerId) {
        return;
      }
      if (state.status !== 'offered') return;

      state.status = 'transferring';
      state.metrics.acceptedAt = performance.now();
      state.metrics.lastSampleTime = state.metrics.acceptedAt;
      state.metrics.lastSampleBytes = 0;

      if (this.callbacks.onTransferStart) {
        this.callbacks.onTransferStart(state);
      }

      try {
        await this._executeSendPipeline(state);
      } catch (err) {
        if (state.status !== 'cancelled') {
          state.status = 'failed';
          if (this.callbacks.onTransferError) {
            this.callbacks.onTransferError(state, err);
          }
        }
      } finally {
        clearHeaderCache(state.transferId);
      }
    }

    handleOfferRejected(fromPeerId, msg) {
      const state = this.transfers.get(msg.transferId);
      if (!state || state.peerId !== fromPeerId) return;
      state.status = 'rejected';
      this.transfers.delete(msg.transferId);
      if (this.callbacks.onTransferRejected) {
        this.callbacks.onTransferRejected(state);
      }
    }

    async _executeSendPipeline(state) {
      const peer = this.getPeer(state.peerId);
      if (!peer || !peer.dc || peer.dc.readyState !== 'open') {
        throw new Error('DataChannel não está aberto para envio');
      }

      const dc = peer.dc;
      const pc = peer.pc;
      const signal = state.abortController.signal;
      const backpressure = DataChannelBackpressure.forChannel(dc);
      const maxPayload = resolveMaxPayloadSize(pc, state.transferId);

      // Start incremental SHA-256 job in parallel
      this.hashClient.init(state.transferId);

      // Send FILE_META immediately (hash will follow in FILE_COMPLETE)
      this.sendPeerMessage(state.peerId, {
        type: 'FILE_META',
        transferId: state.transferId,
        fileId: state.fileId,
        name: state.name,
        size: state.size,
        mimeType: state.mimeType,
        sender: state.senderNickname,
        sha256: null,
        hashPending: true
      });

      const file = state.file;
      const totalBytes = file.size;
      let offset = 0;
      let chunkSize = totalBytes < 512 * 1024
        ? Math.min(maxPayload, 32 * 1024)
        : Math.min(maxPayload, INITIAL_CHUNK_SIZE);

      // Read-ahead pipeline: kick off first 2 MB block read
      let nextReadOffset = Math.min(READ_BLOCK_SIZE, totalBytes);
      let currentBlockPromise = totalBytes > 0
        ? file.slice(0, nextReadOffset).arrayBuffer()
        : Promise.resolve(new ArrayBuffer(0));

      while (offset < totalBytes) {
        if (signal.aborted) {
          throw new Error('Transfer cancelled');
        }

        const blockBuffer = await currentBlockPromise;
        if (signal.aborted) {
          throw new Error('Transfer cancelled');
        }

        // Kick off next read-ahead block while we send the current block
        if (nextReadOffset < totalBytes) {
          const nextEnd = Math.min(nextReadOffset + READ_BLOCK_SIZE, totalBytes);
          currentBlockPromise = file.slice(nextReadOffset, nextEnd).arrayBuffer();
          nextReadOffset = nextEnd;
        } else {
          currentBlockPromise = null;
        }

        // Send entire 2 MB block to SHA-256 worker in parallel!
        // Note: copy is only needed if we slice chunks from blockBuffer;
        // we copy-transfer to worker after reading so main thread keeps blockBuffer for dc.send.
        this.hashClient.update(state.transferId, blockBuffer, false);

        const blockView = new Uint8Array(blockBuffer);
        let blockPos = 0;

        while (blockPos < blockView.byteLength) {
          if (signal.aborted) {
            throw new Error('Transfer cancelled');
          }

          if (dc.bufferedAmount > BUFFER_HIGH) {
            const waitStart = performance.now();
            await backpressure.waitUntilBelow(BUFFER_LOW, signal);
            state.metrics.backpressureWaitMs += performance.now() - waitStart;
          }

          const endPos = Math.min(blockPos + chunkSize, blockView.byteLength);
          const payloadSlice = blockView.subarray(blockPos, endPos);
          const frameBuffer = encodeChunkFrame(state.transferId, payloadSlice);

          dc.send(frameBuffer);

          if (state.metrics.firstByteAt === null) {
            state.metrics.firstByteAt = performance.now();
            state.metrics.acceptToFirstByteMs = state.metrics.firstByteAt - state.metrics.acceptedAt;
          }

          if (dc.bufferedAmount > state.metrics.peakBufferedAmount) {
            state.metrics.peakBufferedAmount = dc.bufferedAmount;
          }

          blockPos = endPos;
          offset += payloadSlice.byteLength;
          state.bytesTransferred = offset;
          state.chunksCount += 1;

          // Adaptive chunk scaling: ramp up to maxPayload after initial chunks when buffer is healthy
          if (state.chunksCount >= 4 && chunkSize < maxPayload && dc.bufferedAmount < BUFFER_HIGH * 0.6) {
            chunkSize = Math.min(maxPayload, chunkSize * 2);
          }

          this._updateProgressMetrics(state, false);
        }
      }

      // Wait for DataChannel buffer to drain below low-water mark (event-driven, no polling)
      if (dc.bufferedAmount > 0) {
        const drainStart = performance.now();
        await backpressure.waitUntilBelow(0, signal);
        state.metrics.backpressureWaitMs += performance.now() - drainStart;
      }

      this._updateProgressMetrics(state, true);

      if (this.callbacks.onTransferValidating) {
        this.callbacks.onTransferValidating(state, 'Finalizando SHA-256...');
      }

      // Finalize incremental SHA-256
      const hashResult = await this.hashClient.finalize(state.transferId);
      state.sha256 = hashResult.hash;
      state.metrics.sha256DurationMs = hashResult.cpuTimeMs;

      // Signal completion with definitive SHA-256
      this.sendPeerMessage(state.peerId, {
        type: 'FILE_COMPLETE',
        transferId: state.transferId,
        fileId: state.fileId,
        totalBytes: totalBytes,
        totalChunks: state.chunksCount,
        sha256: state.sha256
      });

      if (this.callbacks.onTransferValidating) {
        this.callbacks.onTransferValidating(state, 'Aguardando confirmação de integridade...');
      }

      // Wait for receiver's FILE_COMPLETE_ACK matching this transferId
      const ackMsg = await new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          if (state.ackResolver === resolve) {
            state.ackResolver = null;
            state.ackRejecter = null;
            reject(new Error('Timeout aguardando confirmação do destinatário'));
          }
        }, ACK_TIMEOUT_MS);

        const onAbort = () => {
          clearTimeout(timer);
          state.ackResolver = null;
          state.ackRejecter = null;
          reject(new Error('Transfer cancelled'));
        };

        signal.addEventListener('abort', onAbort, { once: true });

        state.ackResolver = (ack) => {
          clearTimeout(timer);
          signal.removeEventListener('abort', onAbort);
          state.ackResolver = null;
          state.ackRejecter = null;
          resolve(ack);
        };
        state.ackRejecter = (err) => {
          clearTimeout(timer);
          signal.removeEventListener('abort', onAbort);
          state.ackResolver = null;
          state.ackRejecter = null;
          reject(err);
        };
      });

      if (ackMsg && ackMsg.ok === false) {
        throw new Error(`Falha na validação do destinatário: ${ackMsg.reason || 'hash inválido'}`);
      }

      state.status = 'completed';
      state.metrics.completedAt = performance.now();
      state.metrics.totalDurationMs = state.metrics.completedAt - state.metrics.acceptedAt;
      state.metrics.chunksCount = state.chunksCount;
      state.metrics.avgChunkSizeBytes = state.chunksCount > 0 ? Math.round(totalBytes / state.chunksCount) : 0;
      const secs = Math.max(0.001, state.metrics.totalDurationMs / 1000);
      state.metrics.avgThroughputMBps = (totalBytes / (1024 * 1024)) / secs;
      state.metrics.rtcStats = await collectRtcStats(pc);
      state.metrics.bottleneck = classifyBottleneck(state.metrics, state.metrics.rtcStats);

      this.completedMetrics.push(state.metrics);
      console.info('[GhostDrop Metrics - Send]', state.metrics);

      if (this.callbacks.onTransferComplete) {
        this.callbacks.onTransferComplete(state);
      }
    }

    handleCompleteAck(fromPeerId, msg) {
      const state = this.transfers.get(msg.transferId);
      if (!state || state.direction !== 'send' || state.peerId !== fromPeerId) {
        return;
      }
      if (state.ackResolver) {
        state.ackResolver(msg);
      }
    }

    handleFileMeta(fromPeerId, msg) {
      const state = this.transfers.get(msg.transferId);
      if (!state || state.direction !== 'receive' || state.peerId !== fromPeerId) {
        return;
      }
      state.status = 'transferring';
      if (msg.sha256) {
        state.expectedSha256 = msg.sha256.toLowerCase();
      }
      if (this.callbacks.onTransferStart) {
        this.callbacks.onTransferStart(state);
      }
    }

    handleBinaryChunk(fromPeerId, arrayBuffer) {
      const decoded = decodeChunkFrame(arrayBuffer);
      if (!decoded) return;

      const state = this.transfers.get(decoded.transferId);
      if (!state || state.direction !== 'receive' || state.peerId !== fromPeerId) {
        return;
      }
      if (state.status === 'cancelled' || state.status === 'failed' || state.status === 'completed') {
        return;
      }

      if (state.metrics.firstByteAt === null) {
        state.metrics.firstByteAt = performance.now();
        state.metrics.acceptToFirstByteMs = state.metrics.firstByteAt - state.metrics.acceptedAt;
      }

      const payload = decoded.payloadView;
      state.bytesTransferred += payload.byteLength;
      state.chunksCount += 1;

      // Feed incremental SHA-256 worker & storage sink in parallel
      this.hashClient.update(state.transferId, payload, false);
      state.sink.write(payload).catch((err) => {
        console.error('[ReceiverSink] Write error:', err);
        this.cancelTransfer(state.transferId, 'sink_write_error', false);
      });

      this._updateProgressMetrics(state, false);

      if (state.pendingCompleteMsg && state.bytesTransferred >= state.size && !state.finalizing) {
        const comp = state.pendingCompleteMsg;
        state.pendingCompleteMsg = null;
        this._finalizeReceiveTransfer(state, comp);
      }
    }

    handleFileComplete(fromPeerId, msg) {
      const state = this.transfers.get(msg.transferId);
      if (!state || state.direction !== 'receive' || state.peerId !== fromPeerId) {
        return;
      }
      if (state.status === 'cancelled' || state.status === 'failed' || state.status === 'completed') {
        return;
      }

      if (state.bytesTransferred < state.size) {
        state.pendingCompleteMsg = msg;
        return;
      }

      if (!state.finalizing) {
        this._finalizeReceiveTransfer(state, msg);
      }
    }

    async _finalizeReceiveTransfer(state, completeMsg) {
      state.finalizing = true;
      state.status = 'validating';
      this._updateProgressMetrics(state, true);

      if (this.callbacks.onTransferValidating) {
        this.callbacks.onTransferValidating(state, 'Validando integridade SHA-256...');
      }

      const validationStart = performance.now();
      try {
        const expectedHash = (completeMsg.sha256 || state.expectedSha256 || '').toLowerCase();
        const expectedSize = typeof completeMsg.totalBytes === 'number' ? completeMsg.totalBytes : state.size;

        const [hashResult, sinkResult] = await Promise.all([
          this.hashClient.finalize(state.transferId),
          state.sink.close()
        ]);

        const calculatedHash = hashResult.hash.toLowerCase();
        state.sha256 = calculatedHash;
        state.metrics.sha256DurationMs = hashResult.cpuTimeMs;
        state.metrics.validationDurationMs = performance.now() - validationStart;

        const sizeMatches = state.bytesTransferred === expectedSize;
        const hashMatches = Boolean(expectedHash) && calculatedHash === expectedHash;

        if (!sizeMatches || !hashMatches) {
          const reason = !sizeMatches ? 'size_mismatch' : 'hash_mismatch';
          await state.sink.abort();
          this.sendPeerMessage(state.peerId, {
            type: 'FILE_COMPLETE_ACK',
            transferId: state.transferId,
            ok: false,
            sha256: calculatedHash,
            reason
          });
          state.status = 'failed';
          const err = new Error(
            !sizeMatches
              ? `Tamanho divergente: recebido ${state.bytesTransferred}, esperado ${expectedSize}`
              : `Hash SHA-256 inválido: esperado ${expectedHash.slice(0, 12)}..., obtido ${calculatedHash.slice(0, 12)}...`
          );
          if (this.callbacks.onTransferError) {
            this.callbacks.onTransferError(state, err);
          }
          return;
        }

        // Integrity confirmed — send ACK only now!
        this.sendPeerMessage(state.peerId, {
          type: 'FILE_COMPLETE_ACK',
          transferId: state.transferId,
          ok: true,
          sha256: calculatedHash
        });

        state.status = 'completed';
        state.metrics.completedAt = performance.now();
        state.metrics.totalDurationMs = state.metrics.completedAt - state.metrics.acceptedAt;
        state.metrics.chunksCount = state.chunksCount;
        state.metrics.avgChunkSizeBytes = state.chunksCount > 0 ? Math.round(state.bytesTransferred / state.chunksCount) : 0;
        const secs = Math.max(0.001, state.metrics.totalDurationMs / 1000);
        state.metrics.avgThroughputMBps = (state.bytesTransferred / (1024 * 1024)) / secs;

        const peer = this.getPeer(state.peerId);
        state.metrics.rtcStats = peer ? await collectRtcStats(peer.pc) : null;
        state.metrics.bottleneck = classifyBottleneck(state.metrics, state.metrics.rtcStats);

        this.completedMetrics.push(state.metrics);
        console.info('[GhostDrop Metrics - Receive]', state.metrics);

        if (this.callbacks.onTransferComplete) {
          this.callbacks.onTransferComplete(state, sinkResult);
        }
      } catch (err) {
        state.status = 'failed';
        await state.sink.abort();
        this.sendPeerMessage(state.peerId, {
          type: 'FILE_COMPLETE_ACK',
          transferId: state.transferId,
          ok: false,
          reason: err.message || 'validation_error'
        });
        if (this.callbacks.onTransferError) {
          this.callbacks.onTransferError(state, err);
        }
      }
    }

    cancelTransfer(transferId, reason = 'user_cancelled', notifyPeer = true) {
      const state = this.transfers.get(transferId);
      if (!state) return;
      if (state.status === 'completed' || state.status === 'cancelled') return;

      state.status = 'cancelled';
      state.abortController.abort();
      this.hashClient.abort(transferId);
      clearHeaderCache(transferId);

      if (state.ackRejecter) {
        state.ackRejecter(new Error('Transfer cancelled'));
      }
      if (state.sink) {
        state.sink.abort();
      }
      if (notifyPeer) {
        this.sendPeerMessage(state.peerId, {
          type: 'TRANSFER_CANCELLED',
          transferId: state.transferId,
          fromPeerId: this.myPeerId,
          reason
        });
      }
      if (this.callbacks.onTransferCancelled) {
        this.callbacks.onTransferCancelled(state, notifyPeer);
      }
    }

    handleRemoteCancel(fromPeerId, msg) {
      if (msg.transferId) {
        const state = this.transfers.get(msg.transferId);
        if (state && state.peerId === fromPeerId) {
          this.cancelTransfer(msg.transferId, msg.reason || 'remote_cancelled', false);
        }
        return;
      }
      // Fallback if a legacy peer sent TRANSFER_CANCELLED without transferId
      for (const state of this.transfers.values()) {
        if (state.peerId === fromPeerId && (state.status === 'transferring' || state.status === 'accepted')) {
          this.cancelTransfer(state.transferId, 'remote_cancelled', false);
        }
      }
    }

    cancelTransfersForPeer(peerId) {
      for (const state of this.transfers.values()) {
        if (state.peerId === peerId && state.status !== 'completed' && state.status !== 'cancelled' && state.status !== 'failed') {
          this.cancelTransfer(state.transferId, 'peer_disconnected', false);
        }
      }
    }

    cancelAll() {
      for (const transferId of Array.from(this.transfers.keys())) {
        this.cancelTransfer(transferId, 'session_reset', false);
      }
      this.transfers.clear();
    }

    _updateProgressMetrics(state, force) {
      const now = performance.now();
      const timeDeltaMs = now - state.metrics.lastSampleTime;
      if (force || timeDeltaMs >= 250) {
        const secs = Math.max(0.001, timeDeltaMs / 1000);
        const bytesDelta = state.bytesTransferred - state.metrics.lastSampleBytes;
        const speedBps = bytesDelta / secs;
        state.metrics.instantThroughputMBps = speedBps / (1024 * 1024);
        state.metrics.lastSampleTime = now;
        state.metrics.lastSampleBytes = state.bytesTransferred;
      }
      if (this.callbacks.onTransferProgress) {
        this.callbacks.onTransferProgress(state, force);
      }
    }
  }

  const exportsObj = {
    TransferManager,
    Sha256WorkerClient,
    DataChannelBackpressure,
    BlobFileSink,
    FileSystemFileSink,
    createReceiverSink,
    encodeChunkFrame,
    decodeChunkFrame,
    collectRtcStats,
    classifyBottleneck,
    resolveMaxPayloadSize,
    BUFFER_HIGH,
    BUFFER_LOW
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = exportsObj;
  } else {
    globalScope.GhostDropTransfer = exportsObj;
  }
})(typeof self !== 'undefined' ? self : globalThis);
