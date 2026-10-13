/**
 * GhostDrop P2P v2.0 - Synchronized Multi-Peer WebRTC Mesh Engine
 * Real-time Group Chat & End-to-End Encrypted File Transfer
 */

// WebRTC STUN Configuration
const STUN_SERVERS = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
    { urls: 'stun:stun2.l.google.com:19302' },
    { urls: 'stun:stun3.l.google.com:19302' },
    { urls: 'stun:stun4.l.google.com:19302' },
    { urls: 'stun:stun.cloudflare.com:3478' },
    { urls: 'stun:stun.sipgate.net:3478' }
  ],
  iceCandidatePoolSize: 10
};

const MQTT_BROKERS = [
  'wss://broker.emqx.io:8084/mqtt',
  'wss://broker.hivemq.com:8884/mqtt'
];
let currentBrokerIndex = 0;

const CHUNK_SIZE = 64 * 1024; // 64 KB per chunk
const BUFFER_THRESHOLD = 2 * 1024 * 1024; // 2 MB backpressure threshold

// Local Identity State
const myPeerId = 'p_' + Math.random().toString(36).substring(2, 9) + '_' + Date.now().toString(36);
let myNickname = '';
let localLanIp = '';
let localServerPort = window.location.port;

// Application State
let currentTab = 'connect';
let mqttClient = null;
let currentSignalingTopic = '';
let encryptionKey = null;
let activeSessionRoomCode = '';
let heartbeatTimer = null;
let inRoom = false;

// Connected Peers Map: peerId -> { pc, dc, nickname, isPolite, state: 'connecting'|'connected', mode: 'p2p'|'relay', relayFallbackTimer, retryTimer, hasAnswer }
const connectedPeers = new Map();

// Pending ICE Candidates queue: peerId -> [candidates]
const pendingIceCandidates = new Map();

// Session transfers history
let sessionTransfers = [];
let pendingSendQueue = [];
let pendingIncomingOffer = null;

// Receiving state per transfer
let currentReceivingMeta = null;
let receivedChunks = [];
let receivedBytes = 0;
let transferStartTime = 0;
let lastBytes = 0;
let lastTime = 0;

// Typing indicator state
const typingPeers = new Map(); // peerId -> { timeout, nickname }

// DOM Elements
const pairingSection = document.getElementById('pairingSection');
const activeRoomSection = document.getElementById('activeRoomSection');
const activeRoomCode = document.getElementById('activeRoomCode');
const participantCountText = document.getElementById('participantCountText');
const participantsChips = document.getElementById('participantsChips');
const btnDisconnect = document.getElementById('btnDisconnect');
const btnRoomCopyCode = document.getElementById('btnRoomCopyCode');
const btnRoomCopyLink = document.getElementById('btnRoomCopyLink');
const btnRoomQrCode = document.getElementById('btnRoomQrCode');
const qrContainer = document.getElementById('qrContainer');

const tabConnect = document.getElementById('tabConnect');
const tabSecurity = document.getElementById('tabSecurity');
const tabConnectBtn = document.getElementById('tabConnectBtn');
const tabSecurityBtn = document.getElementById('tabSecurityBtn');

const globalNicknameInput = document.getElementById('globalNicknameInput');
const roomCodeInput = document.getElementById('roomCodeInput');
const btnGenerateRandomCode = document.getElementById('btnGenerateRandomCode');
const btnEnterRoom = document.getElementById('btnEnterRoom');
const connectStatusBanner = document.getElementById('connectStatusBanner');
const connectStatusText = document.getElementById('connectStatusText');

const statusDot = document.getElementById('statusDot');
const statusPillText = document.getElementById('statusPillText');
const panicButton = document.getElementById('panicButton');

// Chat Elements
const chatMessages = document.getElementById('chatMessages');
const chatForm = document.getElementById('chatForm');
const chatInput = document.getElementById('chatInput');
const btnSendChat = document.getElementById('btnSendChat');
const typingIndicator = document.getElementById('typingIndicator');
const typingIndicatorText = document.getElementById('typingIndicatorText');

// Room File Transfer Elements
const roomDropZone = document.getElementById('roomDropZone');
const roomFileInput = document.getElementById('roomFileInput');
const transfersList = document.getElementById('transfersList');
const emptyTransfersMsg = document.getElementById('emptyTransfersMsg');

// Modals
const transferOverlay = document.getElementById('transferOverlay');
const transferCurrentFileName = document.getElementById('transferCurrentFileName');
const transferPercent = document.getElementById('transferPercent');
const progressBarFill = document.getElementById('progressBarFill');
const transferSpeed = document.getElementById('transferSpeed');
const transferBytes = document.getElementById('transferBytes');
const transferEta = document.getElementById('transferEta');
const transferHash = document.getElementById('transferHash');
const transferStatePill = document.getElementById('transferStatePill');
const btnCancelTransfer = document.getElementById('btnCancelTransfer');
const btnCloseTransferModal = document.getElementById('btnCloseTransferModal');

const incomingModal = document.getElementById('incomingModal');
const incomingModalFileName = document.getElementById('incomingModalFileName');
const incomingModalFileSize = document.getElementById('incomingModalFileSize');
const incomingModalSenderName = document.getElementById('incomingModalSenderName');
const btnModalAccept = document.getElementById('btnModalAccept');
const btnModalReject = document.getElementById('btnModalReject');

// Initialize Application
document.addEventListener('DOMContentLoaded', () => {
  initNickname();
  setupEventListeners();

  // Fetch local LAN IP from server info endpoint
  fetch('/api/info')
    .then(r => r.json())
    .then(data => {
      if (data && data.lan_ip) localLanIp = data.lan_ip;
      if (data && data.port) localServerPort = data.port;
    })
    .catch(() => {});

  // Generate initial code into input
  roomCodeInput.value = generateRandomCode();

  // Check if URL has ?room=... or ?receive=...
  checkUrlParams();
});

// Nickname Management
function initNickname() {
  const saved = localStorage.getItem('ghostdrop_nickname');
  if (saved && saved.trim()) {
    myNickname = saved.trim().substring(0, 18);
  } else {
    myNickname = 'Ghost_' + Math.floor(100 + Math.random() * 900);
  }
  if (globalNicknameInput) {
    globalNicknameInput.value = myNickname;
  }
}

function updateNickname(newNick) {
  const clean = newNick.trim().substring(0, 18);
  if (!clean || clean === myNickname) return;
  const oldNick = myNickname;
  myNickname = clean;
  localStorage.setItem('ghostdrop_nickname', myNickname);
  if (globalNicknameInput) globalNicknameInput.value = myNickname;

  // Broadcast to all connected peers
  broadcastAppMessage({
    type: 'NICKNAME_CHANGED',
    peerId: myPeerId,
    oldNickname: oldNick,
    nickname: myNickname
  });

  renderParticipantsList();
  appendSystemMessage(`✏️ Você alterou seu apelido para <strong>${escapeHtml(myNickname)}</strong>.`);
}

// Deterministic Color Generator for Nicknames
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

function escapeHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// Format Byte Size Helper
function formatBytes(bytes, decimals = 2) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

// Format Current Time HH:MM
function formatTime() {
  const d = new Date();
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

// Generate random 6-digit code
function generateRandomCode() {
  const num1 = Math.floor(100 + Math.random() * 900);
  const num2 = Math.floor(100 + Math.random() * 900);
  return `${num1}-${num2}`;
}

function cleanCode(code) {
  return String(code).replace(/[^0-9]/g, '');
}

function formatCode(digits) {
  const clean = cleanCode(digits);
  if (clean.length <= 3) return clean;
  return clean.slice(0, 3) + '-' + clean.slice(3, 6);
}

// Tab Switching
function switchTab(tab) {
  currentTab = tab;
  [tabConnect, tabSecurity].forEach(el => el.classList.remove('active'));
  [tabConnectBtn, tabSecurityBtn].forEach(el => el.classList.remove('active'));

  if (tab === 'connect') {
    tabConnect.classList.add('active');
    tabConnectBtn.classList.add('active');
  } else if (tab === 'security') {
    tabSecurity.classList.add('active');
    tabSecurityBtn.classList.add('active');
  }
}

// Check URL Query Parameters (e.g., ?room=849210 or ?receive=849210)
function checkUrlParams() {
  const params = new URLSearchParams(window.location.search);
  const code = params.get('room') || params.get('receive');
  if (code) {
    roomCodeInput.value = formatCode(code);
    setTimeout(() => {
      enterSelectedRoom();
    }, 200);
    return true;
  }
  return false;
}

// Cryptography: Derive AES-256 Key from Room Code
async function deriveKey(code) {
  const enc = new TextEncoder();
  const keyMaterial = await crypto.subtle.importKey(
    'raw',
    enc.encode(cleanCode(code)),
    { name: 'PBKDF2' },
    false,
    ['deriveKey']
  );
  return crypto.subtle.deriveKey(
    {
      name: 'PBKDF2',
      salt: enc.encode('ghostdrop_p2p_unified_mesh_2026'),
      iterations: 50000,
      hash: 'SHA-256'
    },
    keyMaterial,
    { name: 'AES-GCM', length: 256 },
    false,
    ['encrypt', 'decrypt']
  );
}

// Encrypt JSON payload
async function encryptPayload(key, data) {
  const enc = new TextEncoder();
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const encoded = enc.encode(JSON.stringify(data));
  const ciphertext = await crypto.subtle.encrypt(
    { name: 'AES-GCM', iv },
    key,
    encoded
  );
  return {
    iv: Array.from(iv),
    payload: Array.from(new Uint8Array(ciphertext))
  };
}

// Decrypt JSON payload
async function decryptPayload(key, encrypted) {
  try {
    const iv = new Uint8Array(encrypted.iv);
    const data = new Uint8Array(encrypted.payload);
    const decrypted = await crypto.subtle.decrypt(
      { name: 'AES-GCM', iv },
      key,
      data
    );
    const dec = new TextDecoder();
    return JSON.parse(dec.decode(decrypted));
  } catch (err) {
    return null;
  }
}

// SHA-256 calculation
async function computeHash(buffer) {
  const hashBuffer = await crypto.subtle.digest('SHA-256', buffer);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
}

// Setup Event Listeners
function setupEventListeners() {
  // Nickname Input Event
  globalNicknameInput.addEventListener('change', (e) => {
    updateNickname(e.target.value);
  });
  globalNicknameInput.addEventListener('blur', (e) => {
    updateNickname(e.target.value);
  });

  // Generate random code button
  btnGenerateRandomCode.addEventListener('click', () => {
    roomCodeInput.value = generateRandomCode();
  });

  // Room input formatter
  roomCodeInput.addEventListener('input', (e) => {
    e.target.value = formatCode(e.target.value);
  });

  roomCodeInput.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
      enterSelectedRoom();
    }
  });

  btnEnterRoom.addEventListener('click', enterSelectedRoom);

  // In-Room Copy Code
  btnRoomCopyCode.addEventListener('click', () => {
    navigator.clipboard.writeText(activeSessionRoomCode).then(() => {
      const orig = btnRoomCopyCode.innerHTML;
      btnRoomCopyCode.innerHTML = '<span>✓ Copiado!</span>';
      setTimeout(() => { btnRoomCopyCode.innerHTML = orig; }, 2000);
    });
  });

  // In-Room Copy Link
  btnRoomCopyLink.addEventListener('click', () => {
    let baseUrl = window.location.origin;
    if ((window.location.hostname === '127.0.0.1' || window.location.hostname === 'localhost') && localLanIp && localLanIp !== '127.0.0.1') {
      baseUrl = `http://${localLanIp}:${localServerPort || window.location.port}`;
    }
    const url = `${baseUrl}/?room=${cleanCode(activeSessionRoomCode)}`;
    navigator.clipboard.writeText(url).then(() => {
      const orig = btnRoomCopyLink.innerHTML;
      btnRoomCopyLink.innerHTML = '<span>✓ Link Copiado!</span>';
      setTimeout(() => { btnRoomCopyLink.innerHTML = orig; }, 2000);
    });
  });

  // In-Room QR Code
  btnRoomQrCode.addEventListener('click', () => {
    const isHidden = qrContainer.classList.toggle('hidden');
    if (!isHidden) {
      const qrEl = document.getElementById('qrcode');
      qrEl.innerHTML = '';
      let baseUrl = window.location.origin;
      if ((window.location.hostname === '127.0.0.1' || window.location.hostname === 'localhost') && localLanIp && localLanIp !== '127.0.0.1') {
        baseUrl = `http://${localLanIp}:${localServerPort || window.location.port}`;
      }
      const url = `${baseUrl}/?room=${cleanCode(activeSessionRoomCode)}`;
      new QRCode(qrEl, {
        text: url,
        width: 160,
        height: 160,
        colorDark: "#000000",
        colorLight: "#ffffff",
        correctLevel: QRCode.CorrectLevel.M
      });
    }
  });

  // Chat Form
  chatForm.addEventListener('submit', (e) => {
    e.preventDefault();
    sendChatMessage();
  });

  let typingTimer = null;
  chatInput.addEventListener('input', () => {
    broadcastAppMessage({
      type: 'TYPING',
      peerId: myPeerId,
      nickname: myNickname,
      isTyping: true
    });
    clearTimeout(typingTimer);
    typingTimer = setTimeout(() => {
      broadcastAppMessage({
        type: 'TYPING',
        peerId: myPeerId,
        nickname: myNickname,
        isTyping: false
      });
    }, 1500);
  });

  // Room Drag and Drop Zone
  ['dragenter', 'dragover'].forEach(name => {
    roomDropZone.addEventListener(name, (e) => {
      e.preventDefault();
      roomDropZone.classList.add('dragover');
    });
  });

  ['dragleave', 'drop'].forEach(name => {
    roomDropZone.addEventListener(name, (e) => {
      e.preventDefault();
      roomDropZone.classList.remove('dragover');
    });
  });

  roomDropZone.addEventListener('drop', (e) => {
    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      queueFilesToSend(Array.from(e.dataTransfer.files));
    }
  });

  roomFileInput.addEventListener('change', (e) => {
    if (e.target.files && e.target.files.length > 0) {
      queueFilesToSend(Array.from(e.target.files));
    }
  });

  // Incoming modal actions
  btnModalAccept.addEventListener('click', () => {
    incomingModal.classList.add('hidden');
    if (pendingIncomingOffer) {
      sendPeerAppMessage(pendingIncomingOffer.senderPeerId, {
        type: 'FILE_OFFER_ACCEPTED',
        fileId: pendingIncomingOffer.fileId,
        targetPeerId: pendingIncomingOffer.senderPeerId,
        fromPeerId: myPeerId
      });
    }
  });

  btnModalReject.addEventListener('click', () => {
    incomingModal.classList.add('hidden');
    if (pendingIncomingOffer) {
      sendPeerAppMessage(pendingIncomingOffer.senderPeerId, {
        type: 'FILE_OFFER_REJECTED',
        fileId: pendingIncomingOffer.fileId,
        targetPeerId: pendingIncomingOffer.senderPeerId,
        fromPeerId: myPeerId
      });
    }
    pendingIncomingOffer = null;
  });

  // Cancel Transfer
  btnCancelTransfer.addEventListener('click', () => {
    if (confirm('Deseja cancelar a transferência em andamento?')) {
      broadcastAppMessage({ type: 'TRANSFER_CANCELLED' });
      transferOverlay.classList.add('hidden');
    }
  });

  btnCloseTransferModal.addEventListener('click', () => {
    transferOverlay.classList.add('hidden');
  });

  // Disconnect button
  btnDisconnect.addEventListener('click', () => {
    if (confirm('Deseja sair desta sala e desconectar de todos os participantes?')) {
      leaveRoom();
    }
  });

  // Panic button
  panicButton.addEventListener('click', () => {
    if (confirm('Isso irá encerrar o executável e destruir todos os buffers e sessões da memória. Continuar?')) {
      resetAllConnections();
      fetch('/api/shutdown', { method: 'POST' }).finally(() => {
        document.body.innerHTML = `
          <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100vh;background:#090d16;color:#94a3b8;font-family:sans-serif;text-align:center;padding:20px;">
            <h1 style="color:#f8fafc;margin-bottom:12px;">Sessão Encerrada</h1>
            <p>O executável foi finalizado e todos os buffers foram destruídos da memória.</p>
            <p style="margin-top:8px;font-size:0.85rem;">Você pode fechar esta aba com segurança.</p>
          </div>
        `;
        setTimeout(() => { window.close(); }, 1500);
      });
    }
  });
}

// =========================================================================
// SYNCHRONIZED MULTI-PEER SIGNALING & WEBRTC MESH
// =========================================================================

// Enter Selected Room
async function enterSelectedRoom() {
  const rawCode = cleanCode(roomCodeInput.value);
  if (rawCode.length < 6) {
    alert('Digite um código de 6 dígitos válido.');
    return;
  }

  connectStatusBanner.classList.remove('hidden');
  connectStatusText.textContent = 'Conectando à rede da sala #' + formatCode(rawCode) + '...';
  activeSessionRoomCode = formatCode(rawCode);

  await joinMeshRoom(rawCode);
  enterRoomUI(activeSessionRoomCode);
}

// Join Mesh Room over encrypted MQTT
async function joinMeshRoom(rawCode) {
  encryptionKey = await deriveKey(rawCode);
  currentSignalingTopic = `ghostdrop/room/${rawCode}`;

  return new Promise((resolve) => {
    connectSignaling(currentSignalingTopic, async (message) => {
      const decrypted = await decryptPayload(encryptionKey, message);
      if (!decrypted) return;
      handleSignalingMessage(decrypted);
    }, () => {
      // Announce arrival immediately
      sendSignalingMessage(currentSignalingTopic, {
        type: 'PEER_HELLO',
        fromPeerId: myPeerId,
        nickname: myNickname
      });

      // Start recurring presence broadcast every 4s to catch any latecomers
      clearInterval(heartbeatTimer);
      heartbeatTimer = setInterval(() => {
        if (inRoom) {
          sendSignalingMessage(currentSignalingTopic, {
            type: 'PEER_HEARTBEAT',
            fromPeerId: myPeerId,
            nickname: myNickname
          });
        }
      }, 4000);

      resolve();
    });
  });
}

// Handle Incoming Signaling Messages in Mesh
async function handleSignalingMessage(msg) {
  if (!msg || msg.fromPeerId === myPeerId) return;

  const senderId = msg.fromPeerId;
  const senderNick = msg.nickname || 'Ghost Peer';

  // 1. Discovery: PEER_HELLO
  if (msg.type === 'PEER_HELLO') {
    // Immediately reply with PEER_WELCOME so the new joiner knows we are in the room without waiting!
    sendSignalingMessage(currentSignalingTopic, {
      type: 'PEER_WELCOME',
      targetPeerId: senderId,
      fromPeerId: myPeerId,
      nickname: myNickname
    });
    getOrCreatePeerConnection(senderId, senderNick);
  }

  // 2. Discovery: PEER_WELCOME
  else if (msg.type === 'PEER_WELCOME' && (msg.targetPeerId === myPeerId || !msg.targetPeerId)) {
    getOrCreatePeerConnection(senderId, senderNick);
  }

  // 3. Discovery: PEER_HEARTBEAT
  else if (msg.type === 'PEER_HEARTBEAT') {
    if (!connectedPeers.has(senderId)) {
      getOrCreatePeerConnection(senderId, senderNick);
    }
  }

  // 4. SDP_OFFER: Offer received
  else if (msg.type === 'SDP_OFFER' && msg.targetPeerId === myPeerId) {
    handleIncomingOffer(senderId, senderNick, msg.sdp);
  }

  // 5. SDP_ANSWER: Answer received
  else if (msg.type === 'SDP_ANSWER' && msg.targetPeerId === myPeerId) {
    const peer = connectedPeers.get(senderId);
    if (peer && peer.pc) {
      try {
        if (peer.pc.signalingState === 'have-local-offer') {
          await peer.pc.setRemoteDescription(new RTCSessionDescription(msg.sdp));
          peer.hasAnswer = true;
          await flushPendingIceCandidates(senderId, peer.pc);
        }
      } catch (err) {
        console.error('Erro ao processar SDP_ANSWER:', err);
      }
    }
  }

  // 6. ICE: Targeted ICE candidate received
  else if (msg.type === 'ICE' && msg.targetPeerId === myPeerId) {
    if (msg.candidate) {
      const peer = connectedPeers.get(senderId);
      if (peer && peer.pc && peer.pc.remoteDescription && peer.pc.remoteDescription.type) {
        await addCandidateToPeer(peer.pc, msg.candidate);
      } else {
        if (!pendingIceCandidates.has(senderId)) {
          pendingIceCandidates.set(senderId, []);
        }
        pendingIceCandidates.get(senderId).push(msg.candidate);
      }
    }
  }

  // 7. Relayed Application Message (Fallback for strict NAT/Firewall)
  else if (msg.type === 'RELAY_MSG' && (msg.targetPeerId === myPeerId || !msg.targetPeerId)) {
    handleAppMessage(senderId, msg.payload);
  }

  // 8. Relayed File Chunk Stream (Fallback for strict NAT/Firewall)
  else if (msg.type === 'RELAY_CHUNK' && msg.targetPeerId === myPeerId) {
    handleRelayChunk(msg);
  }
}

// Safely add candidate to RTCPeerConnection
async function addCandidateToPeer(pc, candData) {
  if (!candData || (!candData.candidate && candData.candidate !== "")) return;
  try {
    if (typeof RTCIceCandidate === 'function') {
      await pc.addIceCandidate(new RTCIceCandidate(candData));
    } else {
      await pc.addIceCandidate(candData);
    }
  } catch (err) {
    // Ignored safely
  }
}

// Flush Queued ICE Candidates
async function flushPendingIceCandidates(peerId, pc) {
  if (pendingIceCandidates.has(peerId)) {
    const candidates = pendingIceCandidates.get(peerId);
    for (const cand of candidates) {
      await addCandidateToPeer(pc, cand);
    }
    pendingIceCandidates.delete(peerId);
  }
}

// Get or Create Peer Connection using Negotiated DataChannel (id: 0) + E2EE Relay Fallback
async function getOrCreatePeerConnection(targetPeerId, targetNickname) {
  if (connectedPeers.has(targetPeerId)) {
    const existing = connectedPeers.get(targetPeerId);
    if (targetNickname && existing.nickname !== targetNickname) {
      existing.nickname = targetNickname;
      renderParticipantsList();
    }
    return existing;
  }

  // Deterministic polarity: smaller peerId is the impolite initiator
  const isPolite = myPeerId > targetPeerId;

  const pc = new RTCPeerConnection(STUN_SERVERS);
  const dc = pc.createDataChannel('ghostdrop', { negotiated: true, id: 0 });
  dc.binaryType = 'arraybuffer';

  const peerObj = {
    pc: pc,
    dc: dc,
    nickname: targetNickname,
    isPolite: isPolite,
    state: 'connecting',
    mode: 'p2p',
    hasAnswer: false,
    relayFallbackTimer: null,
    retryTimer: null
  };
  connectedPeers.set(targetPeerId, peerObj);
  renderParticipantsList();

  function markConnected(mode = 'p2p') {
    const stateChanged = peerObj.state !== 'connected' || peerObj.mode !== mode;
    peerObj.state = 'connected';
    peerObj.mode = mode;
    if (stateChanged) {
      renderParticipantsList();
    }
    if (peerObj.relayFallbackTimer) {
      clearTimeout(peerObj.relayFallbackTimer);
      peerObj.relayFallbackTimer = null;
    }
    if (peerObj.retryTimer) {
      clearInterval(peerObj.retryTimer);
      peerObj.retryTimer = null;
    }
  }

  // Graceful Relay Fallback timer: if direct P2P takes more than 5 seconds (e.g. strict symmetric NAT / firewall),
  // seamlessly activate E2EE Relay Mode so users NEVER get stuck on "conectando infinitamente"!
  peerObj.relayFallbackTimer = setTimeout(() => {
    if (peerObj.state !== 'connected') {
      console.log(`[Mesh] Fallback E2EE Relay ativado para ${targetPeerId}`);
      markConnected('relay');
      appendSystemMessage(`🛡️ Conexão estabelecida com <strong>${escapeHtml(peerObj.nickname)}</strong> via <strong>Relé E2EE Seguro</strong>.`);
    }
  }, 5000);

  setupDataChannelListeners(targetPeerId, dc, markConnected);

  pc.ondatachannel = (event) => {
    const remoteDc = event.channel;
    remoteDc.binaryType = 'arraybuffer';
    peerObj.dc = remoteDc;
    setupDataChannelListeners(targetPeerId, remoteDc, markConnected);
  };

  pc.onicecandidate = (event) => {
    if (event.candidate) {
      sendSignalingMessage(currentSignalingTopic, {
        type: 'ICE',
        targetPeerId: targetPeerId,
        fromPeerId: myPeerId,
        candidate: {
          candidate: event.candidate.candidate,
          sdpMid: event.candidate.sdpMid,
          sdpMLineIndex: event.candidate.sdpMLineIndex
        }
      });
    }
  };

  pc.onconnectionstatechange = () => {
    if (pc.connectionState === 'connected') {
      markConnected('p2p');
    } else if (pc.connectionState === 'failed') {
      if (peerObj.mode !== 'relay') {
        markConnected('relay');
      }
    } else if (pc.connectionState === 'closed') {
      removePeer(targetPeerId);
    }
  };

  pc.oniceconnectionstatechange = () => {
    if (pc.iceConnectionState === 'connected' || pc.iceConnectionState === 'completed') {
      markConnected('p2p');
    } else if (pc.iceConnectionState === 'failed') {
      if (peerObj.mode !== 'relay') {
        markConnected('relay');
      }
    }
  };

  // Impolite peer initiates offer
  if (!isPolite) {
    async function makeOffer(isRestart = false) {
      if (pc.signalingState !== 'stable' && !isRestart) return;
      try {
        const offer = await pc.createOffer(isRestart ? { iceRestart: true } : {});
        await pc.setLocalDescription(offer);

        sendSignalingMessage(currentSignalingTopic, {
          type: 'SDP_OFFER',
          targetPeerId: targetPeerId,
          fromPeerId: myPeerId,
          nickname: myNickname,
          sdp: offer
        });
      } catch (err) {
        console.error('Erro ao gerar SDP_OFFER:', err);
      }
    }

    makeOffer(false);

    // Offer retry timer in case packet was lost over network
    peerObj.retryTimer = setInterval(() => {
      if (peerObj.state !== 'connected' && !peerObj.hasAnswer && inRoom) {
        makeOffer(true);
      } else {
        clearInterval(peerObj.retryTimer);
        peerObj.retryTimer = null;
      }
    }, 3500);
  }

  return peerObj;
}

// Handle Incoming Offer from remote peer
async function handleIncomingOffer(fromPeerId, fromNickname, offerSdp) {
  let peerObj = connectedPeers.get(fromPeerId);
  if (!peerObj) {
    peerObj = await getOrCreatePeerConnection(fromPeerId, fromNickname);
  }

  const pc = peerObj.pc;
  try {
    if (pc.signalingState !== 'stable' && !peerObj.isPolite) {
      return;
    }
    await pc.setRemoteDescription(new RTCSessionDescription(offerSdp));
    await flushPendingIceCandidates(fromPeerId, pc);

    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);

    sendSignalingMessage(currentSignalingTopic, {
      type: 'SDP_ANSWER',
      targetPeerId: fromPeerId,
      fromPeerId: myPeerId,
      nickname: myNickname,
      sdp: answer
    });
  } catch (err) {
    console.error('Erro ao processar oferta ou responder SDP_ANSWER:', err);
  }
}

// Setup DataChannel Listeners with immediate readyState check
function setupDataChannelListeners(peerId, channel, onConnectedCallback) {
  function handleOpen() {
    const peer = connectedPeers.get(peerId);
    if (peer) {
      peer.state = 'connected';
      peer.mode = 'p2p';
      peer.dc = channel;
    }
    if (onConnectedCallback) onConnectedCallback('p2p');

    try {
      channel.send(JSON.stringify({
        type: 'PEER_INFO',
        peerId: myPeerId,
        nickname: myNickname
      }));
    } catch (e) {}

    const peerNick = peer ? peer.nickname : 'Participante';
    appendSystemMessage(`⚡ <strong>${escapeHtml(peerNick)}</strong> conectado via <strong>P2P Direto</strong>.`);
    renderParticipantsList();
  }

  if (channel.readyState === 'open') {
    handleOpen();
  } else {
    channel.onopen = handleOpen;
  }

  channel.onclose = () => {
    const peer = connectedPeers.get(peerId);
    if (peer && peer.mode === 'p2p' && inRoom) {
      peer.mode = 'relay';
      renderParticipantsList();
    } else {
      removePeer(peerId);
    }
  };

  channel.onerror = (err) => {
    console.error(`Erro no canal P2P com ${peerId}:`, err);
  };

  channel.onmessage = (event) => {
    if (typeof event.data === 'string') {
      try {
        const msg = JSON.parse(event.data);
        handleAppMessage(peerId, msg);
      } catch (e) {}
    } else if (event.data instanceof ArrayBuffer) {
      // Binary file chunk received over direct P2P
      receivedChunks.push(event.data);
      receivedBytes += event.data.byteLength;
      if (currentReceivingMeta) {
        updateTransferProgress(receivedBytes, currentReceivingMeta.size);
      }
    }
  };
}

// Send Message to Specific Peer (P2P direct DataChannel or encrypted MQTT relay)
function sendPeerAppMessage(targetPeerId, msgObj) {
  const peer = connectedPeers.get(targetPeerId);
  if (!peer) return;

  if (peer.mode === 'p2p' && peer.dc && peer.dc.readyState === 'open') {
    try {
      peer.dc.send(JSON.stringify(msgObj));
      return;
    } catch (e) {}
  }

  // Fallback to Encrypted MQTT Relay
  sendSignalingMessage(currentSignalingTopic, {
    type: 'RELAY_MSG',
    targetPeerId: targetPeerId,
    fromPeerId: myPeerId,
    payload: msgObj
  });
}

// Broadcast Message Across All Connected Peers
function broadcastAppMessage(msgObj) {
  connectedPeers.forEach((peer, peerId) => {
    sendPeerAppMessage(peerId, msgObj);
  });
}

// Handle Application Messages (Chat, Nickname, File metadata)
async function handleAppMessage(fromPeerId, msg) {
  if (!msg || !msg.type) return;

  // 1. Peer Info
  if (msg.type === 'PEER_INFO') {
    const peer = connectedPeers.get(fromPeerId);
    if (peer && msg.nickname) {
      peer.nickname = msg.nickname;
      renderParticipantsList();
    }
  }
  // 2. Chat Message
  else if (msg.type === 'CHAT') {
    appendChatMessage(msg.text, 'received', msg.time, msg.sender || 'Ghost', msg.peerId);
  }
  // 3. Nickname Changed
  else if (msg.type === 'NICKNAME_CHANGED') {
    const peer = connectedPeers.get(fromPeerId);
    if (peer) {
      peer.nickname = msg.nickname;
      renderParticipantsList();
      appendSystemMessage(`✏️ <strong>${escapeHtml(msg.oldNickname)}</strong> mudou o apelido para <strong>${escapeHtml(msg.nickname)}</strong>.`);
    }
  }
  // 4. Typing Indicator
  else if (msg.type === 'TYPING') {
    handlePeerTyping(fromPeerId, msg.nickname, msg.isTyping);
  }
  // 5. File Offer Received
  else if (msg.type === 'FILE_OFFER') {
    pendingIncomingOffer = msg;
    pendingIncomingOffer.senderPeerId = fromPeerId;
    incomingModalFileName.textContent = msg.name;
    incomingModalFileSize.textContent = formatBytes(msg.size);
    incomingModalSenderName.textContent = msg.sender || 'Um participante';
    incomingModal.classList.remove('hidden');
  }
  // 6. File Offer Accepted
  else if (msg.type === 'FILE_OFFER_ACCEPTED') {
    startSendingNextQueuedFile(fromPeerId);
  }
  // 7. File Offer Rejected
  else if (msg.type === 'FILE_OFFER_REJECTED') {
    alert(`O participante ${escapeHtml(msg.sender || '')} recusou o arquivo.`);
  }
  // 8. File Start Stream
  else if (msg.type === 'FILE_START') {
    currentReceivingMeta = msg;
    receivedChunks = [];
    receivedBytes = 0;
    transferStartTime = Date.now();
    lastBytes = 0;
    lastTime = Date.now();
    openTransferModal(msg.name, msg.size, 'Recebendo...');
  }
  // 9. File End Stream
  else if (msg.type === 'FILE_END') {
    await finishReceivedFile(msg);
  }
  // 10. Cancel Transfer
  else if (msg.type === 'TRANSFER_CANCELLED') {
    alert('A transferência foi cancelada.');
    transferOverlay.classList.add('hidden');
  }
}

// Handle Relayed File Chunk
function handleRelayChunk(msg) {
  if (!msg.chunk) return;
  const chunkBuffer = new Uint8Array(msg.chunk).buffer;
  receivedChunks.push(chunkBuffer);
  receivedBytes += chunkBuffer.byteLength;
  if (currentReceivingMeta) {
    updateTransferProgress(receivedBytes, currentReceivingMeta.size);
  }
}

// Complete and Save Received File
async function finishReceivedFile(msg) {
  const blob = new Blob(receivedChunks, { type: currentReceivingMeta ? currentReceivingMeta.mimeType : 'application/octet-stream' });
  const arrayBuffer = await blob.arrayBuffer();
  const calculatedHash = await computeHash(arrayBuffer);

  if (msg.sha256 && calculatedHash.toLowerCase() === msg.sha256.toLowerCase()) {
    transferHash.textContent = '✓ ' + calculatedHash.substring(0, 16) + '... (Válido)';
    transferHash.style.color = '#10b981';
  } else {
    transferHash.textContent = calculatedHash.substring(0, 16) + '...';
  }

  // Download file to disk
  downloadBlob(blob, currentReceivingMeta ? currentReceivingMeta.name : 'download');

  // Add to session history
  addTransferToHistory({
    name: currentReceivingMeta ? currentReceivingMeta.name : 'arquivo',
    size: currentReceivingMeta ? currentReceivingMeta.size : receivedBytes,
    direction: 'received',
    sha256: calculatedHash,
    sender: currentReceivingMeta ? currentReceivingMeta.sender : 'Peer'
  });

  progressBarFill.style.width = '100%';
  transferPercent.textContent = '100%';
  transferStatePill.textContent = 'Concluído!';
  transferStatePill.style.background = 'rgba(16, 185, 129, 0.2)';
  transferStatePill.style.color = '#6ee7b7';
  btnCloseTransferModal.classList.remove('hidden');
  btnCancelTransfer.classList.add('hidden');
}

// Remove Peer & Clean Up Connection
function removePeer(peerId) {
  const peer = connectedPeers.get(peerId);
  if (peer) {
    if (peer.relayFallbackTimer) clearTimeout(peer.relayFallbackTimer);
    if (peer.retryTimer) clearInterval(peer.retryTimer);
    if (peer.state === 'connected') {
      appendSystemMessage(`🚪 <strong>${escapeHtml(peer.nickname)}</strong> desconectou-se da sala.`);
    }
    try {
      if (peer.dc) peer.dc.close();
      if (peer.pc) peer.pc.close();
    } catch (e) {}
    connectedPeers.delete(peerId);
    pendingIceCandidates.delete(peerId);
    renderParticipantsList();
  }
}

// Ephemeral MQTT Connection with broker failover
function connectSignaling(topic, onMessageCallback, onConnectCallback) {
  if (mqttClient) {
    try { mqttClient.end(true); } catch (e) {}
    mqttClient = null;
  }

  const brokerUrl = MQTT_BROKERS[currentBrokerIndex % MQTT_BROKERS.length];
  mqttClient = mqtt.connect(brokerUrl, {
    clientId: 'ghost_' + Math.random().toString(16).substring(2, 10),
    clean: true,
    connectTimeout: 5000,
    reconnectPeriod: 2000
  });

  let connectedOnce = false;

  mqttClient.on('connect', () => {
    mqttClient.subscribe(topic, { qos: 0 }, (err) => {
      if (!err && !connectedOnce) {
        connectedOnce = true;
        if (onConnectCallback) {
          onConnectCallback();
        }
      }
    });
  });

  mqttClient.on('error', (err) => {
    console.warn(`[MQTT] Falha de conexão no broker ${brokerUrl}:`, err);
    if (!connectedOnce) {
      currentBrokerIndex++;
      setTimeout(() => {
        if (inRoom) connectSignaling(topic, onMessageCallback, onConnectCallback);
      }, 1000);
    }
  });

  mqttClient.on('message', (t, msg) => {
    if (t === topic) {
      try {
        const data = JSON.parse(msg.toString());
        onMessageCallback(data);
      } catch (err) {
        console.error('Erro ao processar mensagem MQTT:', err);
      }
    }
  });
}

// Send Encrypted Signaling Message over MQTT
async function sendSignalingMessage(topic, data) {
  if (!mqttClient || !encryptionKey) return;
  const encrypted = await encryptPayload(encryptionKey, data);
  mqttClient.publish(topic, JSON.stringify(encrypted), { qos: 0, retain: false });
}

// UI Transition: Enter Connected Room
function enterRoomUI(code) {
  inRoom = true;
  pairingSection.classList.add('hidden');
  activeRoomSection.classList.remove('hidden');
  activeRoomCode.textContent = `#${code}`;
  statusDot.className = 'status-dot green pulse';
  statusPillText.textContent = 'Sala P2P Ativa';

  renderParticipantsList();
}

// UI Transition: Leave Room
function leaveRoom() {
  inRoom = false;
  clearInterval(heartbeatTimer);
  resetAllConnections();
  activeRoomSection.classList.add('hidden');
  pairingSection.classList.remove('hidden');
  connectStatusBanner.classList.add('hidden');
  qrContainer.classList.add('hidden');
  statusDot.className = 'status-dot green';
  statusPillText.textContent = 'E2EE DTLS Pronto';
  chatMessages.innerHTML = `
    <div class="chat-system-msg">
      <span>🔒 Conexão segura estabelecida. Mensagens e arquivos são transmitidos diretamente entre os nós P2P.</span>
    </div>
  `;
  sessionTransfers = [];
  renderTransfersList();
}

// Render Participants List & Counter with Mode Badge
function renderParticipantsList() {
  const activeCount = Array.from(connectedPeers.values()).filter(p => p.state === 'connected').length + 1;
  participantCountText.textContent = activeCount === 1 ? '1 participante (você)' : `${activeCount} participantes`;

  participantsChips.innerHTML = '';

  // 1. Add Myself
  const myChip = document.createElement('div');
  myChip.className = 'peer-chip is-me';
  myChip.innerHTML = `
    <span class="peer-chip-avatar" style="background:${getNicknameColor(myNickname)};"></span>
    <span>${escapeHtml(myNickname)} (Você)</span>
  `;
  participantsChips.appendChild(myChip);

  // 2. Add Connected/Connecting Peers
  connectedPeers.forEach(peer => {
    const isConn = peer.state === 'connected';
    let badgeHtml = '';
    if (isConn) {
      if (peer.mode === 'p2p') {
        badgeHtml = `<span style="font-size:0.75rem; color:#34d399; margin-left:4px;" title="WebRTC DataChannel direto de alta velocidade">● P2P</span>`;
      } else {
        badgeHtml = `<span style="font-size:0.75rem; color:#38bdf8; margin-left:4px;" title="Túnel criptografado E2EE via relé de segurança">● Relé E2EE</span>`;
      }
    } else {
      badgeHtml = `<small style="opacity:0.65; font-size:0.75rem; margin-left:4px; color:#fbbf24;">(conectando...)</small>`;
    }
    const chip = document.createElement('div');
    chip.className = 'peer-chip';
    chip.innerHTML = `
      <span class="peer-chip-avatar" style="background:${getNicknameColor(peer.nickname)};"></span>
      <span>${escapeHtml(peer.nickname)} ${badgeHtml}</span>
    `;
    participantsChips.appendChild(chip);
  });
}

// =========================================================================
// CHAT & FILE TRANSFERS
// =========================================================================

// Send Chat Message
function sendChatMessage() {
  const text = chatInput.value.trim();
  if (!text) return;

  const timeStr = formatTime();

  // Send over all open data channels or encrypted relay
  broadcastAppMessage({
    type: 'CHAT',
    text: text,
    time: timeStr,
    sender: myNickname,
    peerId: myPeerId
  });

  appendChatMessage(text, 'sent', timeStr, myNickname, myPeerId);
  chatInput.value = '';
  chatInput.focus();
}

function appendChatMessage(text, direction, timeStr = formatTime(), sender = myNickname, peerId = myPeerId) {
  const bubble = document.createElement('div');
  bubble.className = `chat-bubble ${direction}`;

  // Author label
  const authorEl = document.createElement('div');
  authorEl.className = 'chat-bubble-author';
  const color = getNicknameColor(sender);
  authorEl.style.color = color;
  authorEl.textContent = direction === 'sent' ? `${sender} (Você)` : sender;

  const textEl = document.createElement('div');
  textEl.textContent = text;

  const timeEl = document.createElement('div');
  timeEl.className = 'chat-time';
  timeEl.textContent = timeStr;

  bubble.appendChild(authorEl);
  bubble.appendChild(textEl);
  bubble.appendChild(timeEl);
  chatMessages.appendChild(bubble);
  chatMessages.scrollTop = chatMessages.scrollHeight;
}

function appendSystemMessage(htmlText) {
  const msg = document.createElement('div');
  msg.className = 'chat-system-msg';
  msg.innerHTML = `<span>${htmlText}</span>`;
  chatMessages.appendChild(msg);
  chatMessages.scrollTop = chatMessages.scrollHeight;
}

// Typing Indicator Handler
function handlePeerTyping(peerId, nickname, isTyping) {
  if (isTyping) {
    if (typingPeers.has(peerId)) {
      clearTimeout(typingPeers.get(peerId).timeout);
    }
    const timeout = setTimeout(() => {
      typingPeers.delete(peerId);
      updateTypingIndicatorUI();
    }, 2500);
    typingPeers.set(peerId, { timeout, nickname });
  } else {
    if (typingPeers.has(peerId)) {
      clearTimeout(typingPeers.get(peerId).timeout);
      typingPeers.delete(peerId);
    }
  }
  updateTypingIndicatorUI();
}

function updateTypingIndicatorUI() {
  if (typingPeers.size === 0) {
    typingIndicator.classList.add('hidden');
  } else {
    const names = Array.from(typingPeers.values()).map(p => p.nickname);
    const text = names.length === 1 ? `${names[0]} está digitando...` : `${names.join(', ')} estão digitando...`;
    typingIndicatorText.textContent = text;
    typingIndicator.classList.remove('hidden');
  }
}

// Queue Files to Send to Connected Peers
function queueFilesToSend(files) {
  const connectedCount = Array.from(connectedPeers.values()).filter(p => p.state === 'connected').length;
  if (connectedCount === 0) {
    alert('Nenhum outro participante conectado na sala ainda. Compartilhe o código ou link da sala para que outras pessoas entrem!');
    return;
  }

  files.forEach(file => {
    pendingSendQueue.push(file);
  });

  if (pendingSendQueue.length > 0) {
    offerNextFile();
  }
}

function offerNextFile() {
  if (pendingSendQueue.length === 0) return;
  const file = pendingSendQueue[0];

  broadcastAppMessage({
    type: 'FILE_OFFER',
    fileId: 'file_' + Date.now(),
    name: file.name,
    size: file.size,
    mimeType: file.type || 'application/octet-stream',
    sender: myNickname,
    senderPeerId: myPeerId
  });

  appendSystemMessage(`📤 Oferta de arquivo enviada ao grupo: <strong>${escapeHtml(file.name)}</strong> (${formatBytes(file.size)}).`);
}

// Send File Stream to Specific Peer or All Accepting Peers
async function startSendingNextQueuedFile(targetPeerId) {
  if (pendingSendQueue.length === 0) return;
  const file = pendingSendQueue.shift();

  const targetPeer = connectedPeers.get(targetPeerId);
  if (!targetPeer) {
    return;
  }

  const isP2P = targetPeer.mode === 'p2p' && targetPeer.dc && targetPeer.dc.readyState === 'open';
  openTransferModal(file.name, file.size, `Enviando para ${targetPeer.nickname} (${isP2P ? 'P2P Direto' : 'Relé Seguro'})...`);

  const fileBuffer = await file.arrayBuffer();
  const sha256 = await computeHash(fileBuffer);

  sendPeerAppMessage(targetPeerId, {
    type: 'FILE_START',
    name: file.name,
    size: file.size,
    mimeType: file.type || 'application/octet-stream',
    sha256: sha256,
    sender: myNickname
  });

  transferStartTime = Date.now();
  lastBytes = 0;
  lastTime = Date.now();

  const chunkSize = isP2P ? CHUNK_SIZE : 32 * 1024;
  let offset = 0;

  while (offset < file.size) {
    if (isP2P) {
      if (targetPeer.dc.bufferedAmount > BUFFER_THRESHOLD) {
        await new Promise(resolve => {
          targetPeer.dc.onbufferedamountlow = () => {
            targetPeer.dc.onbufferedamountlow = null;
            resolve();
          };
        });
      }
      const chunk = fileBuffer.slice(offset, offset + chunkSize);
      targetPeer.dc.send(chunk);
      offset += chunk.byteLength;
    } else {
      const chunk = fileBuffer.slice(offset, offset + chunkSize);
      const chunkArray = Array.from(new Uint8Array(chunk));
      sendSignalingMessage(currentSignalingTopic, {
        type: 'RELAY_CHUNK',
        targetPeerId: targetPeerId,
        fromPeerId: myPeerId,
        chunk: chunkArray
      });
      offset += chunk.byteLength;
      await new Promise(r => setTimeout(r, 6));
    }

    updateTransferProgress(offset, file.size);
  }

  sendPeerAppMessage(targetPeerId, {
    type: 'FILE_END',
    sha256: sha256
  });

  addTransferToHistory({
    name: file.name,
    size: file.size,
    direction: 'sent',
    sha256: sha256,
    sender: myNickname
  });

  transferHash.textContent = '✓ ' + sha256.substring(0, 16) + '... (Válido)';
  transferHash.style.color = '#10b981';
  progressBarFill.style.width = '100%';
  transferPercent.textContent = '100%';
  transferStatePill.textContent = 'Concluído com Sucesso!';
  transferStatePill.style.background = 'rgba(16, 185, 129, 0.2)';
  transferStatePill.style.color = '#6ee7b7';
  btnCloseTransferModal.classList.remove('hidden');
  btnCancelTransfer.classList.add('hidden');

  if (pendingSendQueue.length > 0) {
    setTimeout(offerNextFile, 500);
  }
}

// Add Transfer to History List
function addTransferToHistory(transfer) {
  sessionTransfers.unshift(transfer);
  renderTransfersList();
}

function renderTransfersList() {
  if (sessionTransfers.length === 0) {
    emptyTransfersMsg.classList.remove('hidden');
    transfersList.innerHTML = '';
    transfersList.appendChild(emptyTransfersMsg);
    return;
  }

  emptyTransfersMsg.classList.add('hidden');
  transfersList.innerHTML = '';

  sessionTransfers.forEach(item => {
    const card = document.createElement('div');
    card.className = 'transfer-item-card';
    card.innerHTML = `
      <div class="transfer-item-info">
        <span style="font-size:1.1rem;">${item.direction === 'sent' ? '📤' : '📥'}</span>
        <div>
          <div class="transfer-item-name">${escapeHtml(item.name)}</div>
          <div class="transfer-item-meta">${formatBytes(item.size)} • SHA: ${item.sha256.substring(0, 8)}...</div>
        </div>
      </div>
      <span class="transfer-badge ${item.direction}">${item.direction === 'sent' ? 'Enviado' : 'Recebido'}</span>
    `;
    transfersList.appendChild(card);
  });
}

// Update UI Progress Metrics
function updateTransferProgress(current, total) {
  const percent = Math.min(100, Math.round((current / total) * 100));
  progressBarFill.style.width = percent + '%';
  transferPercent.textContent = percent + '%';
  transferBytes.textContent = `${formatBytes(current)} / ${formatBytes(total)}`;

  const now = Date.now();
  const timeDelta = (now - lastTime) / 1000;
  if (timeDelta >= 0.5) {
    const bytesDelta = current - lastBytes;
    const speedBps = bytesDelta / timeDelta;
    transferSpeed.textContent = (speedBps / (1024 * 1024)).toFixed(1) + ' MB/s';

    const remainingBytes = total - current;
    if (speedBps > 0) {
      const remainingSeconds = Math.round(remainingBytes / speedBps);
      transferEta.textContent = remainingSeconds < 60 ? `${remainingSeconds}s` : `${Math.floor(remainingSeconds / 60)}m ${remainingSeconds % 60}s`;
    }

    lastBytes = current;
    lastTime = now;
  }
}

function openTransferModal(fileName, fileSize, title) {
  transferCurrentFileName.textContent = fileName;
  transferBytes.textContent = `0 Bytes / ${formatBytes(fileSize)}`;
  progressBarFill.style.width = '0%';
  transferPercent.textContent = '0%';
  transferSpeed.textContent = '0.0 MB/s';
  transferEta.textContent = 'Calculando...';
  transferHash.textContent = 'Calculando hash...';
  transferHash.style.color = '#a5b4fc';
  transferStatePill.textContent = title;
  transferStatePill.style.background = 'rgba(99, 102, 241, 0.15)';
  transferStatePill.style.color = '#a5b4fc';
  btnCancelTransfer.classList.remove('hidden');
  btnCloseTransferModal.classList.add('hidden');
  transferOverlay.classList.remove('hidden');
}

// Download file blob directly
function downloadBlob(blob, fileName) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.style.display = 'none';
  a.href = url;
  a.download = fileName;
  document.body.appendChild(a);
  a.click();
  setTimeout(() => {
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }, 1000);
}

// Reset All Connections
function resetAllConnections() {
  connectedPeers.forEach(peer => {
    try {
      if (peer.dc) peer.dc.close();
      if (peer.pc) peer.pc.close();
    } catch (e) {}
  });
  connectedPeers.clear();
  pendingIceCandidates.clear();

  if (mqttClient) {
    mqttClient.end();
    mqttClient = null;
  }
}
