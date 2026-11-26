/**
 * GhostDrop P2P - transfer.js (v2 - Dual-Channel Credit-Based Streaming Engine)
 *
 * Architecture:
 * - Dual WebRTC DataChannels:
 *     - 'ghostdrop-ctrl' (id: 0): JSON control, credits, ACKs, metadata, cancel
 *     - 'ghostdrop-data' (id: 1): Binary file frames only (zero HoL blocking of control)
 * - Receiver-driven sliding-window credit flow control (bytesAcked + windowBytes)
 * - Dedicated Receiver Web Worker (receiver-worker.js) with zero-copy ArrayBuffer transfer,
 *   sequence/offset validation, incremental SHA-256, and OPFS SyncAccessHandle / FS Access I/O
 * - Sender incremental SHA-256 Web Worker (sha256-worker.js) + 2MB read-ahead pipeline
 * - Continuous RTCPeerConnection.getStats() sampling & evidence-based bottleneck diagnostics
 */
'use strict';

(function (globalScope) {
  const FRAME_MAGIC = 0x47; // 'G'
  const DEFAULT_MAX_CHUNK = 256 * 1024; // 256 KB upper cap
  const FALLBACK_MAX_MSG_SIZE = 64 * 1024; // 64 KB safe SCTP fallback
  const MIN_CHUNK_SIZE = 16 * 1024; // 16 KB
  const INITIAL_CHUNK_SIZE = 64 * 1024; // 64 KB
  const READ_BLOCK_SIZE = 2 * 1024 * 1024; // 2 MB read-ahead block

  const BUFFER_HIGH = 4 * 1024 * 1024; // 4 MB SCTP send buffer high-water
  const BUFFER_LOW = 1 * 1024 * 1024;  // 1 MB SCTP send buffer low-water
  const INITIAL_WINDOW_BYTES = 2 * 1024 * 1024; // 2 MB initial receiver credit window
  const ACK_TIMEOUT_MS = 60_000;
  const STATS_INTERVAL_MS = 500;

  const textEncoder = new TextEncoder();
  const textDecoder = new TextDecoder();

  let transferSeq = 0;

  function generateTransferId(peerId) {
    transferSeq += 1;
    return `tx_${peerId}_${transferSeq}_${Date.now().toString(36)}`;
  }

  // =========================================================================
  // 1. Binary Data Frame Encoder ([1B magic][1B idLen][id][4B seq][8B offset][payload])
  // =========================================================================
  const idBytesCache = new Map();

  function getEncodedId(transferId) {
    let b = idBytesCache.get(transferId);
    if (!b) {
      b = textEncoder.encode(transferId);
      if (b.length > 255) throw new Error(`transferId too long (${b.length})`);
      idBytesCache.set(transferId, b);
    }
    return b;
  }

  function clearEncodedId(transferId) {
    idBytesCache.delete(transferId);
  }

  function encodeDataFrame(transferId, seq, offset, payloadView) {
    const idBytes = getEncodedId(transferId);
    const headerLen = 2 + idBytes.length + 12;
    const frame = new Uint8Array(headerLen + payloadView.byteLength);
    frame[0] = FRAME_MAGIC;
    frame[1] = idBytes.length;
    frame.set(idBytes, 2);

    const dv = new DataView(frame.buffer, 2 + idBytes.length, 12);
    dv.setUint32(0, seq >>> 0, false);
    const offHigh = Math.floor(offset / 0x100000000);
    const offLow = offset >>> 0;
    dv.setUint32(4, offHigh >>> 0, false);
    dv.setUint32(8, offLow, false);

    frame.set(payloadView, headerLen);
    return frame.buffer;
  }

  // =========================================================================
  // 2. Sender Incremental SHA-256 Worker Client
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
          const copy = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
          this.worker.postMessage({ type: 'update', jobId, buffer: copy }, [copy]);
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
  // 3. Dedicated Receiver Worker Client (with Node/Fallback support)
  // =========================================================================
  class ReceiverWorkerClient {
    constructor(workerPath = 'receiver-worker.js', handlers = {}) {
      this.workerPath = workerPath;
      this.handlers = handlers;
      this.worker = null;
      this.fallbackManager = null;
      this._init();
    }

    _init() {
      if (typeof Worker !== 'undefined') {
        try {
          this.worker = new Worker(this.workerPath);
          this.worker.onmessage = (event) => this._dispatch(event.data);
          this.worker.onerror = (err) => console.error('[ReceiverWorkerClient] Worker error:', err);
          return;
        } catch (e) {
          console.warn('[ReceiverWorkerClient] Worker spawn failed, using fallback:', e);
        }
      }
      if (globalScope.ReceiverJobManager && globalScope.Sha256Incremental) {
        this.fallbackManager = new globalScope.ReceiverJobManager(
          (msg) => this._dispatch(msg),
          globalScope.Sha256Incremental
        );
      }
    }

    _dispatch(msg) {
      if (!msg || !msg.type) return;
      if (msg.type === 'ready' && this.handlers.onReady) this.handlers.onReady(msg);
      else if (msg.type === 'init_error' && this.handlers.onInitError) this.handlers.onInitError(msg);
      else if (msg.type === 'credit' && this.handlers.onCredit) this.handlers.onCredit(msg);
      else if (msg.type === 'finalized' && this.handlers.onFinalized) this.handlers.onFinalized(msg);
      else if (msg.type === 'error' && this.handlers.onError) this.handlers.onError(msg);
    }

    initJob(payload) {
      if (this.worker) {
        this.worker.postMessage({ type: 'init', ...payload });
      } else if (this.fallbackManager) {
        this.fallbackManager.initJob(payload);
      }
    }

    pushChunk(arrayBuffer) {
      if (this.worker) {
        // Zero-copy transfer of ownership to Receiver Worker
        this.worker.postMessage({ type: 'chunk', buffer: arrayBuffer }, [arrayBuffer]);
      } else if (this.fallbackManager) {
        this.fallbackManager.handleChunk(arrayBuffer);
      }
    }

    finalizeJob(payload) {
      if (this.worker) {
        this.worker.postMessage({ type: 'finalize', ...payload });
      } else if (this.fallbackManager) {
        this.fallbackManager.finalizeJob(payload);
      }
    }

    abortJob(transferId) {
      if (this.worker) {
        this.worker.postMessage({ type: 'abort', transferId });
      } else if (this.fallbackManager) {
        this.fallbackManager.abortJob(transferId);
      }
    }

    removeOpfsTemp(tempFileName) {
      if (!tempFileName) return;
      if (this.worker) {
        this.worker.postMessage({ type: 'remove_opfs_temp', tempFileName });
      } else if (this.fallbackManager) {
        this.fallbackManager.removeOpfsTemp(tempFileName);
      }
    }
  }

  // =========================================================================
  // 4. Per-DataChannel Backpressure Coordinator
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

        if (this.dc.bufferedAmount <= threshold) {
          this.waiters.delete(waiter);
          cleanup();
          resolve();
        }
      });
    }
  }

  // =========================================================================
  // 5. WebRTC Stats & Evidence-Based Bottleneck Diagnostics
  // =========================================================================
  async function collectRtcStats(pc) {
    if (!pc || typeof pc.getStats !== 'function') return null;
    try {
      const report = await pc.getStats();
      let selectedPair = null;
      const localCandidates = new Map();
      const remoteCandidates = new Map();
      let transportStat = null;
      let dcStat = null;

      report.forEach((stat) => {
        if (stat.type === 'transport') {
          transportStat = stat;
        } else if (stat.type === 'local-candidate') {
          localCandidates.set(stat.id, stat);
        } else if (stat.type === 'remote-candidate') {
          remoteCandidates.set(stat.id, stat);
        } else if (stat.type === 'data-channel') {
          dcStat = stat;
        }
      });

      const selectedId = transportStat ? transportStat.selectedCandidatePairId : null;
      report.forEach((stat) => {
        if (stat.type === 'candidate-pair') {
          if (stat.id === selectedId || (stat.state === 'succeeded' && stat.nominated)) {
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
        directP2P: !isRelay,
        usingTurnRelay: Boolean(isRelay),
        transportBytesSent: transportStat ? (transportStat.bytesSent || 0) : (selectedPair.bytesSent || 0),
        transportBytesReceived: transportStat ? (transportStat.bytesReceived || 0) : (selectedPair.bytesReceived || 0),
        dcBytesSent: dcStat ? (dcStat.bytesSent || 0) : 0,
        dcBytesReceived: dcStat ? (dcStat.bytesReceived || 0) : 0
      };
    } catch (e) {
      return null;
    }
  }

  function diagnoseBottleneck(state, rtcStats) {
    const m = state.metrics;
    if (rtcStats && rtcStats.usingTurnRelay) {
      return 'turn_relay_detected';
    }
    if (state.direction === 'send') {
      const creditWait = m.creditWaitMs || 0;
      const bpWait = m.backpressureWaitMs || 0;
      const recv = m.receiverMetrics;
      if (creditWait > bpWait && creditWait > 50) {
        if (recv && recv.storageTimeMs > recv.hashTimeMs * 1.5) {
          return 'receiver_storage_io';
        }
        if (recv && recv.hashTimeMs > (recv.storageTimeMs || 1) * 1.5) {
          return 'receiver_cpu_hash';
        }
        return 'waiting_receiver_credits';
      }
      if (bpWait > 50) {
        if (rtcStats && rtcStats.rttMs !== null && rtcStats.rttMs > 80) {
          return 'network_high_rtt';
        }
        return 'waiting_local_sctp_buffer';
      }
      return 'direct_p2p_optimal';
    } else {
      const storageMs = m.storageDurationMs || 0;
      const hashMs = m.sha256DurationMs || 0;
      const totalMs = m.totalDurationMs || 1;
      if (storageMs > hashMs * 1.5 && storageMs > totalMs * 0.25) {
        return 'receiver_storage_io';
      }
      if (hashMs > storageMs * 1.5 && hashMs > totalMs * 0.25) {
        return 'receiver_cpu_hash';
      }
      if (rtcStats && rtcStats.rttMs !== null && rtcStats.rttMs > 80) {
        return 'network_high_rtt';
      }
      return 'direct_p2p_optimal';
    }
  }

  function resolveMaxPayloadSize(pc, transferId) {
    const hdrLen = 2 + getEncodedId(transferId).length + 12;
    let sctpMax = FALLBACK_MAX_MSG_SIZE;
    if (pc && pc.sctp && typeof pc.sctp.maxMessageSize === 'number' && pc.sctp.maxMessageSize > 0) {
      sctpMax = pc.sctp.maxMessageSize;
    }
    const capped = Math.min(sctpMax, DEFAULT_MAX_CHUNK);
    return Math.max(MIN_CHUNK_SIZE - hdrLen, capped - hdrLen);
  }

  async function pickSaveFileHandleIfAvailable(fileName) {
    if (typeof window !== 'undefined' && typeof window.showSaveFilePicker === 'function' && window.isSecureContext) {
      try {
        const handle = await window.showSaveFilePicker({ suggestedName: fileName });
        return { fileHandle: handle, userCancelled: false };
      } catch (err) {
        if (err && err.name === 'AbortError') {
          return { fileHandle: null, userCancelled: true };
        }
        console.warn('[GhostDropTransfer] showSaveFilePicker unavailable, using OPFS/fallback:', err.message || err);
      }
    }
    return { fileHandle: null, userCancelled: false };
  }

  // =========================================================================
  // 6. TransferManager (Dual-Channel, Credit-Controlled, Instrumented)
  // =========================================================================
  class TransferManager {
    constructor(options) {
      this.myPeerId = options.myPeerId;
      this.getPeer = options.getPeer;
      this.sendPeerMessage = options.sendPeerMessage;
      this.callbacks = options.callbacks || {};

      this.hashClient = new Sha256WorkerClient(options.sha256WorkerPath || options.workerPath || 'sha256-worker.js');
      this.receiverWorker = new ReceiverWorkerClient(options.receiverWorkerPath || 'receiver-worker.js', {
        onReady: (msg) => this._onReceiverReady(msg),
        onInitError: (msg) => this._onReceiverInitError(msg),
        onCredit: (msg) => this._onReceiverCredit(msg),
        onFinalized: (msg) => this._onReceiverFinalized(msg),
        onError: (msg) => this._onReceiverError(msg)
      });

      this.transfers = new Map();
      this.completedMetrics = [];
    }

    getTransfer(transferId) {
      return this.transfers.get(transferId) || null;
    }

    getActiveTransfers() {
      return Array.from(this.transfers.values()).filter(
        t => ['offered', 'initializing', 'accepted', 'transferring', 'validating'].includes(t.status)
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

        // Honest byte accounting
        bytesSentTransport: 0,
        bytesAcked: 0,
        bytesTransferred: 0, // alias for UI compatibility (= bytesAcked on send)
        windowBytes: INITIAL_WINDOW_BYTES,
        creditLimitOffset: INITIAL_WINDOW_BYTES,
        inFlightBytes: 0,
        chunksCount: 0,
        sha256: null,
        abortController,

        creditWaiter: null,
        ackResolver: null,
        ackRejecter: null,
        statsTimer: null,
        waitStateLabel: null,

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
          sendSpeedMBps: 0,
          ackedSpeedMBps: 0,
          avgThroughputMBps: 0,
          instantThroughputMBps: 0,
          chunksCount: 0,
          avgChunkSizeBytes: 0,
          peakBufferedAmount: 0,
          peakInFlightBytes: 0,
          backpressureWaitMs: 0,
          creditWaitMs: 0,
          sha256DurationMs: 0,
          receiverMetrics: null,
          rtcStats: null,
          statsSamples: [],
          bottleneck: 'unknown',
          lastAckSampleTime: 0,
          lastAckSampleBytes: 0,
          lastSendSampleTime: 0,
          lastSendSampleBytes: 0
        }
      };

      this.transfers.set(transferId, state);
      return state;
    }

    async prepareReceiveTransfer(offer, fileHandle = null) {
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
        status: 'initializing',

        bytesReceived: 0,
        bytesProcessed: 0,
        bytesWritten: 0,
        bytesTransferred: 0, // = bytesWritten
        chunksCount: 0,
        windowBytes: INITIAL_WINDOW_BYTES,
        storageBackend: 'pending',
        sha256: null,
        expectedSha256: null,
        pendingCompleteMsg: null,
        finalizing: false,
        abortController,
        readyResolve: null,
        readyReject: null,
        statsTimer: null,
        opfsTempName: null,

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
          writeSpeedMBps: 0,
          chunksCount: 0,
          avgChunkSizeBytes: 0,
          sha256DurationMs: 0,
          storageDurationMs: 0,
          validationDurationMs: 0,
          storageBackend: 'pending',
          rtcStats: null,
          statsSamples: [],
          bottleneck: 'unknown',
          lastSampleTime: performance.now(),
          lastSampleBytes: 0
        }
      };

      this.transfers.set(offer.transferId, state);

      const readyPromise = new Promise((resolve, reject) => {
        state.readyResolve = resolve;
        state.readyReject = reject;
      });

      this.receiverWorker.initJob({
        transferId: offer.transferId,
        name: offer.name,
        size: offer.size,
        mimeType: state.mimeType,
        fileHandle,
        initialWindowBytes: INITIAL_WINDOW_BYTES
      });

      return readyPromise;
    }

    _onReceiverReady(msg) {
      const state = this.transfers.get(msg.transferId);
      if (!state) return;
      state.status = 'accepted';
      state.storageBackend = msg.backend;
      state.windowBytes = msg.windowBytes;
      state.metrics.storageBackend = msg.backend;
      if (state.readyResolve) {
        state.readyResolve(state);
        state.readyResolve = null;
        state.readyReject = null;
      }
    }

    _onReceiverInitError(msg) {
      const state = this.transfers.get(msg.transferId);
      if (!state) return;
      state.status = 'failed';
      const err = new Error(msg.error || 'Falha ao inicializar armazenamento');
      if (state.readyReject) {
        state.readyReject(err);
        state.readyResolve = null;
        state.readyReject = null;
      }
      this.transfers.delete(msg.transferId);
    }

    _onReceiverCredit(msg) {
      const state = this.transfers.get(msg.transferId);
      if (!state || state.status === 'cancelled' || state.status === 'failed') return;

      const m = msg.metrics || {};
      state.bytesReceived = m.bytesReceived || state.bytesReceived;
      state.bytesProcessed = m.bytesProcessed || state.bytesProcessed;
      state.bytesWritten = m.bytesWritten || state.bytesWritten;
      state.bytesTransferred = state.bytesWritten;
      state.windowBytes = msg.windowBytes || state.windowBytes;

      state.metrics.sha256DurationMs = m.hashTimeMs || 0;
      state.metrics.storageDurationMs = m.storageTimeMs || 0;
      state.metrics.writeSpeedMBps = (m.writeSpeedBps || 0) / (1024 * 1024);

      const now = performance.now();
      const dt = (now - state.metrics.lastSampleTime) / 1000;
      if (dt >= 0.25) {
        state.metrics.instantThroughputMBps = ((state.bytesWritten - state.metrics.lastSampleBytes) / (1024 * 1024)) / dt;
        state.metrics.lastSampleTime = now;
        state.metrics.lastSampleBytes = state.bytesWritten;
      }

      // Send credit update over independent control channel
      this.sendPeerMessage(state.peerId, {
        type: 'TRANSFER_CREDIT',
        transferId: state.transferId,
        ackedOffset: msg.ackedOffset,
        ackedSeq: msg.ackedSeq,
        windowBytes: msg.windowBytes,
        metrics: m
      });

      if (this.callbacks.onTransferProgress) {
        this.callbacks.onTransferProgress(state, false);
      }

      if (state.pendingCompleteMsg && state.bytesWritten >= state.size && !state.finalizing) {
        const comp = state.pendingCompleteMsg;
        state.pendingCompleteMsg = null;
        this._triggerReceiverFinalize(state, comp);
      }
    }

    _onReceiverError(msg) {
      const state = this.transfers.get(msg.transferId);
      if (!state) return;
      this._stopStatsSampling(state);
      state.status = 'failed';
      this.sendPeerMessage(state.peerId, {
        type: 'FILE_COMPLETE_ACK',
        transferId: state.transferId,
        ok: false,
        reason: msg.error || 'receiver_worker_error'
      });
      if (this.callbacks.onTransferError) {
        this.callbacks.onTransferError(state, new Error(msg.error || 'Erro no receptor'));
      }
    }

    async _onReceiverFinalized(msg) {
      const state = this.transfers.get(msg.transferId);
      if (!state) return;
      this._stopStatsSampling(state);

      const m = msg.metrics || {};
      state.bytesReceived = m.bytesReceived ?? state.bytesReceived;
      state.bytesProcessed = m.bytesProcessed ?? state.bytesProcessed;
      state.bytesWritten = m.bytesWritten ?? state.bytesWritten;
      state.bytesTransferred = state.bytesWritten;
      state.metrics.sha256DurationMs = m.hashTimeMs || state.metrics.sha256DurationMs;
      state.metrics.storageDurationMs = m.storageTimeMs || state.metrics.storageDurationMs;
      state.metrics.validationDurationMs = performance.now() - (state.validationStartTime || performance.now());
      state.opfsTempName = msg.opfsTempName || null;

      if (!msg.ok) {
        state.status = 'failed';
        this.sendPeerMessage(state.peerId, {
          type: 'FILE_COMPLETE_ACK',
          transferId: state.transferId,
          ok: false,
          sha256: msg.sha256 || '',
          reason: msg.reason || 'validation_failed'
        });
        if (this.callbacks.onTransferError) {
          this.callbacks.onTransferError(state, new Error(`Falha na validação: ${msg.reason}`));
        }
        return;
      }

      state.sha256 = msg.sha256;
      state.status = 'completed';
      state.metrics.completedAt = performance.now();
      state.metrics.totalDurationMs = state.metrics.completedAt - state.metrics.acceptedAt;
      state.metrics.chunksCount = state.chunksCount;
      state.metrics.avgChunkSizeBytes = state.chunksCount > 0 ? Math.round(state.bytesWritten / state.chunksCount) : 0;
      const secs = Math.max(0.001, state.metrics.totalDurationMs / 1000);
      state.metrics.avgThroughputMBps = (state.bytesWritten / (1024 * 1024)) / secs;

      const peer = this.getPeer(state.peerId);
      state.metrics.rtcStats = peer ? await collectRtcStats(peer.pc) : null;
      state.metrics.bottleneck = diagnoseBottleneck(state, state.metrics.rtcStats);

      // Confirm completion ONLY after size, sequence, disk write, and SHA-256 pass
      this.sendPeerMessage(state.peerId, {
        type: 'FILE_COMPLETE_ACK',
        transferId: state.transferId,
        ok: true,
        sha256: state.sha256
      });

      this.completedMetrics.push(state.metrics);
      console.info('[GhostDrop Metrics - Receive]', state.metrics);

      if (this.callbacks.onTransferComplete) {
        this.callbacks.onTransferComplete(state, {
          file: msg.file || null,
          blob: msg.blob || null,
          savedToDisk: Boolean(msg.savedToDisk),
          opfsTempName: msg.opfsTempName || null,
          cleanupOpfs: () => {
            if (msg.opfsTempName) this.receiverWorker.removeOpfsTemp(msg.opfsTempName);
          }
        });
      }
    }

    // =======================================================================
    // Sender Flow Control & Pipeline
    // =======================================================================
    handleTransferCredit(fromPeerId, msg) {
      const state = this.transfers.get(msg.transferId);
      if (!state || state.direction !== 'send' || state.peerId !== fromPeerId) return;

      const prevAcked = state.bytesAcked;
      if (typeof msg.ackedOffset === 'number' && msg.ackedOffset > state.bytesAcked) {
        state.bytesAcked = msg.ackedOffset;
        state.bytesTransferred = state.bytesAcked;
      }
      if (typeof msg.windowBytes === 'number' && msg.windowBytes > 0) {
        state.windowBytes = msg.windowBytes;
      }
      state.creditLimitOffset = state.bytesAcked + state.windowBytes;
      state.inFlightBytes = Math.max(0, state.bytesSentTransport - state.bytesAcked);
      state.metrics.receiverMetrics = msg.metrics || state.metrics.receiverMetrics;

      const now = performance.now();
      const dt = (now - state.metrics.lastAckSampleTime) / 1000;
      if (dt >= 0.25 && state.bytesAcked > prevAcked) {
        state.metrics.ackedSpeedMBps = ((state.bytesAcked - state.metrics.lastAckSampleBytes) / (1024 * 1024)) / dt;
        state.metrics.instantThroughputMBps = state.metrics.ackedSpeedMBps;
        state.metrics.lastAckSampleTime = now;
        state.metrics.lastAckSampleBytes = state.bytesAcked;
      }

      if (state.creditWaiter) {
        state.creditWaiter();
      }
      if (this.callbacks.onTransferProgress) {
        this.callbacks.onTransferProgress(state, false);
      }
    }

    _waitForCredits(state, neededOffset, signal) {
      if (neededOffset <= state.creditLimitOffset) return Promise.resolve();
      if (signal.aborted) return Promise.reject(new Error('Transfer cancelled'));

      return new Promise((resolve, reject) => {
        const check = () => {
          if (signal.aborted) {
            state.creditWaiter = null;
            signal.removeEventListener('abort', onAbort);
            reject(new Error('Transfer cancelled'));
            return;
          }
          if (neededOffset <= state.creditLimitOffset) {
            state.creditWaiter = null;
            signal.removeEventListener('abort', onAbort);
            resolve();
          }
        };
        const onAbort = () => check();
        signal.addEventListener('abort', onAbort, { once: true });
        state.creditWaiter = check;
      });
    }

    async handleOfferAccepted(fromPeerId, msg) {
      const state = this.transfers.get(msg.transferId);
      if (!state || state.direction !== 'send' || state.peerId !== fromPeerId) return;
      if (state.status !== 'offered') return;

      if (typeof msg.windowBytes === 'number' && msg.windowBytes > 0) {
        state.windowBytes = msg.windowBytes;
        state.creditLimitOffset = msg.windowBytes;
      }

      state.status = 'transferring';
      state.metrics.acceptedAt = performance.now();
      state.metrics.lastAckSampleTime = state.metrics.acceptedAt;
      state.metrics.lastSendSampleTime = state.metrics.acceptedAt;

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
        this._stopStatsSampling(state);
        clearEncodedId(state.transferId);
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
      const dcData = peer ? (peer.dcData || peer.dc) : null;
      const pc = peer ? peer.pc : null;
      if (!peer || !dcData || dcData.readyState !== 'open') {
        throw new Error('Canal de dados P2P não está aberto');
      }

      const signal = state.abortController.signal;
      const backpressure = DataChannelBackpressure.forChannel(dcData);
      const maxPayload = resolveMaxPayloadSize(pc, state.transferId);

      this._startStatsSampling(state, pc, dcData);
      this.hashClient.init(state.transferId);

      // Send FILE_META on control channel
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
      let seq = 0;
      let chunkSize = totalBytes < 512 * 1024
        ? Math.min(maxPayload, 32 * 1024)
        : Math.min(maxPayload, INITIAL_CHUNK_SIZE);

      let nextReadOffset = Math.min(READ_BLOCK_SIZE, totalBytes);
      let currentBlockPromise = totalBytes > 0
        ? file.slice(0, nextReadOffset).arrayBuffer()
        : Promise.resolve(new ArrayBuffer(0));

      while (offset < totalBytes) {
        if (signal.aborted) throw new Error('Transfer cancelled');

        const blockBuffer = await currentBlockPromise;
        if (signal.aborted) throw new Error('Transfer cancelled');

        if (nextReadOffset < totalBytes) {
          const nextEnd = Math.min(nextReadOffset + READ_BLOCK_SIZE, totalBytes);
          currentBlockPromise = file.slice(nextReadOffset, nextEnd).arrayBuffer();
          nextReadOffset = nextEnd;
        } else {
          currentBlockPromise = null;
        }

        // Incremental SHA-256 in parallel
        this.hashClient.update(state.transferId, blockBuffer, false);

        const blockView = new Uint8Array(blockBuffer);
        let blockPos = 0;

        while (blockPos < blockView.byteLength) {
          if (signal.aborted) throw new Error('Transfer cancelled');

          const endPos = Math.min(blockPos + chunkSize, blockView.byteLength);
          const payloadLen = endPos - blockPos;

          // 1. Receiver-credit sliding window enforcement
          if (offset + payloadLen > state.creditLimitOffset) {
            state.waitStateLabel = 'Aguardando créditos do receptor...';
            const tCred = performance.now();
            await this._waitForCredits(state, offset + payloadLen, signal);
            state.metrics.creditWaitMs += performance.now() - tCred;
            state.waitStateLabel = null;
          }

          // 2. Local SCTP DataChannel backpressure
          if (dcData.bufferedAmount > BUFFER_HIGH) {
            state.waitStateLabel = 'Aguardando buffer de envio P2P...';
            const tBp = performance.now();
            await backpressure.waitUntilBelow(BUFFER_LOW, signal);
            state.metrics.backpressureWaitMs += performance.now() - tBp;
            state.waitStateLabel = null;
          }

          const payloadSlice = blockView.subarray(blockPos, endPos);
          const frameBuffer = encodeDataFrame(state.transferId, seq, offset, payloadSlice);

          dcData.send(frameBuffer);

          if (state.metrics.firstByteAt === null) {
            state.metrics.firstByteAt = performance.now();
            state.metrics.acceptToFirstByteMs = state.metrics.firstByteAt - state.metrics.acceptedAt;
          }

          if (dcData.bufferedAmount > state.metrics.peakBufferedAmount) {
            state.metrics.peakBufferedAmount = dcData.bufferedAmount;
          }

          blockPos = endPos;
          offset += payloadLen;
          seq += 1;
          state.bytesSentTransport = offset;
          state.chunksCount = seq;
          state.inFlightBytes = Math.max(0, state.bytesSentTransport - state.bytesAcked);
          if (state.inFlightBytes > state.metrics.peakInFlightBytes) {
            state.metrics.peakInFlightBytes = state.inFlightBytes;
          }

          if (seq >= 4 && chunkSize < maxPayload && dcData.bufferedAmount < BUFFER_HIGH * 0.5) {
            chunkSize = Math.min(maxPayload, chunkSize * 2);
          }

          const now = performance.now();
          const dtSend = (now - state.metrics.lastSendSampleTime) / 1000;
          if (dtSend >= 0.25) {
            state.metrics.sendSpeedMBps = ((state.bytesSentTransport - state.metrics.lastSendSampleBytes) / (1024 * 1024)) / dtSend;
            state.metrics.lastSendSampleTime = now;
            state.metrics.lastSendSampleBytes = state.bytesSentTransport;
          }

          if (this.callbacks.onTransferProgress) {
            this.callbacks.onTransferProgress(state, false);
          }
        }
      }

      if (dcData.bufferedAmount > 0) {
        const tDrain = performance.now();
        await backpressure.waitUntilBelow(0, signal);
        state.metrics.backpressureWaitMs += performance.now() - tDrain;
      }

      if (this.callbacks.onTransferValidating) {
        this.callbacks.onTransferValidating(state, 'Finalizando SHA-256 e aguardando confirmação do receptor...');
      }

      const hashResult = await this.hashClient.finalize(state.transferId);
      state.sha256 = hashResult.hash;
      state.metrics.sha256DurationMs = hashResult.cpuTimeMs;

      // Send FILE_COMPLETE on control channel
      this.sendPeerMessage(state.peerId, {
        type: 'FILE_COMPLETE',
        transferId: state.transferId,
        fileId: state.fileId,
        totalBytes,
        totalChunks: seq,
        sha256: state.sha256
      });

      const ackMsg = await new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          if (state.ackResolver === resolve) {
            state.ackResolver = null;
            state.ackRejecter = null;
            reject(new Error('Timeout aguardando confirmação final do destinatário'));
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
        throw new Error(`Receptor rejeitou integridade: ${ackMsg.reason || 'hash inválido'}`);
      }

      state.bytesAcked = totalBytes;
      state.bytesTransferred = totalBytes;
      state.inFlightBytes = 0;
      state.status = 'completed';
      state.metrics.completedAt = performance.now();
      state.metrics.totalDurationMs = state.metrics.completedAt - state.metrics.acceptedAt;
      state.metrics.chunksCount = seq;
      state.metrics.avgChunkSizeBytes = seq > 0 ? Math.round(totalBytes / seq) : 0;
      const secs = Math.max(0.001, state.metrics.totalDurationMs / 1000);
      state.metrics.avgThroughputMBps = (totalBytes / (1024 * 1024)) / secs;
      state.metrics.rtcStats = await collectRtcStats(pc);
      state.metrics.bottleneck = diagnoseBottleneck(state, state.metrics.rtcStats);

      this.completedMetrics.push(state.metrics);
      console.info('[GhostDrop Metrics - Send]', state.metrics);

      if (this.callbacks.onTransferComplete) {
        this.callbacks.onTransferComplete(state);
      }
    }

    handleCompleteAck(fromPeerId, msg) {
      const state = this.transfers.get(msg.transferId);
      if (!state || state.direction !== 'send' || state.peerId !== fromPeerId) return;
      if (state.ackResolver) state.ackResolver(msg);
    }

    handleFileMeta(fromPeerId, msg) {
      const state = this.transfers.get(msg.transferId);
      if (!state || state.direction !== 'receive' || state.peerId !== fromPeerId) return;
      state.status = 'transferring';
      if (msg.sha256) state.expectedSha256 = msg.sha256.toLowerCase();

      const peer = this.getPeer(fromPeerId);
      if (peer) {
        this._startStatsSampling(state, peer.pc, peer.dcData || peer.dc);
      }

      if (this.callbacks.onTransferStart) {
        this.callbacks.onTransferStart(state);
      }
    }

    handleBinaryChunk(fromPeerId, arrayBuffer) {
      // Peek transferId length & string only to track per-transfer firstByteAt/chunksCount;
      // actual frame decoding, hashing, and storage run in Receiver Worker with zero-copy transfer.
      const view = new Uint8Array(arrayBuffer);
      if (view.byteLength < 15 || view[0] !== FRAME_MAGIC) return;
      const idLen = view[1];
      if (view.byteLength < 2 + idLen + 12) return;
      const transferId = textDecoder.decode(view.subarray(2, 2 + idLen));

      const state = this.transfers.get(transferId);
      if (!state || state.direction !== 'receive' || state.peerId !== fromPeerId) return;
      if (['cancelled', 'failed', 'completed'].includes(state.status)) return;

      if (state.metrics.firstByteAt === null) {
        state.metrics.firstByteAt = performance.now();
        state.metrics.acceptToFirstByteMs = state.metrics.firstByteAt - state.metrics.acceptedAt;
      }
      state.chunksCount += 1;

      this.receiverWorker.pushChunk(arrayBuffer);
    }

    handleFileComplete(fromPeerId, msg) {
      const state = this.transfers.get(msg.transferId);
      if (!state || state.direction !== 'receive' || state.peerId !== fromPeerId) return;
      if (['cancelled', 'failed', 'completed'].includes(state.status)) return;

      const expectedSize = typeof msg.totalBytes === 'number' ? msg.totalBytes : state.size;
      state.pendingCompleteMsg = msg;
      if (state.bytesWritten >= expectedSize && !state.finalizing) {
        state.pendingCompleteMsg = null;
        this._triggerReceiverFinalize(state, msg);
      }
    }

    _triggerReceiverFinalize(state, completeMsg) {
      if (state.finalizing) return;
      state.finalizing = true;
      state.status = 'validating';
      state.validationStartTime = performance.now();

      if (this.callbacks.onTransferValidating) {
        this.callbacks.onTransferValidating(state, 'Validando sequência, gravação e SHA-256...');
      }

      this.receiverWorker.finalizeJob({
        transferId: state.transferId,
        expectedSha256: completeMsg.sha256 || state.expectedSha256 || '',
        expectedSize: typeof completeMsg.totalBytes === 'number' ? completeMsg.totalBytes : state.size
      });
    }

    cancelTransfer(transferId, reason = 'user_cancelled', notifyPeer = true) {
      const state = this.transfers.get(transferId);
      if (!state || state.status === 'completed' || state.status === 'cancelled') return;

      state.status = 'cancelled';
      this._stopStatsSampling(state);
      state.abortController.abort();
      clearEncodedId(transferId);

      if (state.direction === 'send') {
        this.hashClient.abort(transferId);
        if (state.ackRejecter) state.ackRejecter(new Error('Transfer cancelled'));
      } else {
        this.receiverWorker.abortJob(transferId);
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
      for (const state of this.transfers.values()) {
        if (state.peerId === fromPeerId && ['transferring', 'accepted', 'initializing'].includes(state.status)) {
          this.cancelTransfer(state.transferId, 'remote_cancelled', false);
        }
      }
    }

    cancelTransfersForPeer(peerId) {
      for (const state of this.transfers.values()) {
        if (state.peerId === peerId && !['completed', 'cancelled', 'failed'].includes(state.status)) {
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

    _startStatsSampling(state, pc, dcData) {
      if (!pc || state.statsTimer) return;
      state.statsTimer = setInterval(async () => {
        if (['completed', 'cancelled', 'failed'].includes(state.status)) {
          this._stopStatsSampling(state);
          return;
        }
        const stats = await collectRtcStats(pc);
        if (stats) {
          state.metrics.rtcStats = stats;
          state.metrics.statsSamples.push({
            t: Math.round(performance.now() - (state.metrics.acceptedAt || performance.now())),
            rttMs: stats.rttMs,
            bufferedAmount: dcData ? dcData.bufferedAmount : 0,
            inFlightBytes: state.inFlightBytes || 0,
            bytesAcked: state.bytesAcked || 0,
            bytesWritten: state.bytesWritten || 0
          });
          if (state.metrics.statsSamples.length > 120) {
            state.metrics.statsSamples.shift();
          }
        }
      }, STATS_INTERVAL_MS);
    }

    _stopStatsSampling(state) {
      if (state.statsTimer) {
        clearInterval(state.statsTimer);
        state.statsTimer = null;
      }
    }
  }

  const exportsObj = {
    TransferManager,
    Sha256WorkerClient,
    ReceiverWorkerClient,
    DataChannelBackpressure,
    encodeDataFrame,
    collectRtcStats,
    diagnoseBottleneck,
    resolveMaxPayloadSize,
    pickSaveFileHandleIfAvailable,
    BUFFER_HIGH,
    BUFFER_LOW,
    INITIAL_WINDOW_BYTES
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = exportsObj;
  } else {
    globalScope.GhostDropTransfer = exportsObj;
  }
})(typeof self !== 'undefined' ? self : globalThis);
