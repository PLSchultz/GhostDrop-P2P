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

const CHUNK_SIZE = 256 * 1024;          // 256 KB per chunk for high throughput
const BUFFER_HIGH = 8 * 1024 * 1024;    // 8 MB: stop sending above this
const BUFFER_LOW = 2 * 1024 * 1024;    // 2 MB: resume sending below this

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

// Video & Audio Conference (Google Meet Style) State
let inCall = false;
let isMicMuted = false;
let isVideoOff = false;
let isScreenSharing = false;
let isVideoMinimized = false;
let isTheaterMode = false;
let localStream = null;
let screenStream = null;
let localAudioMeter = null;

// Peer Media Streams and States: peerId -> MediaStream
const peerRemoteStreams = new Map();
// peerId -> { inCall, isMuted, isVideoOff, isScreenSharing, nickname }
const peerMediaStates = new Map();
// peerId -> audioMeter
const peerAudioMeters = new Map();

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

// Video Conference DOM Elements
const btnToggleCallHeader = document.getElementById('btnToggleCallHeader');
const callButtonText = document.getElementById('callButtonText');
const callActiveBadge = document.getElementById('callActiveBadge');
const callLobbyBanner = document.getElementById('callLobbyBanner');
const btnJoinCallVideo = document.getElementById('btnJoinCallVideo');
const btnJoinCallAudio = document.getElementById('btnJoinCallAudio');

const videoConferenceSection = document.getElementById('videoConferenceSection');
const videoGrid = document.getElementById('videoGrid');
const videoParticipantsCount = document.getElementById('videoParticipantsCount');
const btnToggleVideoTheater = document.getElementById('btnToggleVideoTheater');
const btnMinimizeVideoSection = document.getElementById('btnMinimizeVideoSection');
const minimizeIcon = document.getElementById('minimizeIcon');
const minimizeText = document.getElementById('minimizeText');

const tileLocal = document.getElementById('tileLocal');
const localVideo = document.getElementById('localVideo');
const localAvatarPlaceholder = document.getElementById('localAvatarPlaceholder');
const localAvatarCircle = document.getElementById('localAvatarCircle');
const localAvatarInitial = document.getElementById('localAvatarInitial');
const localAvatarName = document.getElementById('localAvatarName');
const localScreenBadge = document.getElementById('localScreenBadge');
const localTileName = document.getElementById('localTileName');
const localTileMicIcon = document.getElementById('localTileMicIcon');
const localTileCamIcon = document.getElementById('localTileCamIcon');

const btnMeetMic = document.getElementById('btnMeetMic');
const btnMeetCam = document.getElementById('btnMeetCam');
const btnMeetScreen = document.getElementById('btnMeetScreen');
const btnMeetHangup = document.getElementById('btnMeetHangup');
const remoteAudiosContainer = document.getElementById('remoteAudiosContainer');

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
    .catch(() => { });

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

  // Video Conference Controls
  if (btnToggleCallHeader) {
    btnToggleCallHeader.addEventListener('click', () => {
      if (inCall) {
        leaveCall();
      } else {
        joinCall({ video: true });
      }
    });
  }

  if (btnJoinCallVideo) {
    btnJoinCallVideo.addEventListener('click', () => {
      joinCall({ video: true });
    });
  }

  if (btnJoinCallAudio) {
    btnJoinCallAudio.addEventListener('click', () => {
      joinCall({ video: false });
    });
  }

  if (btnMeetMic) btnMeetMic.addEventListener('click', toggleMic);
  if (btnMeetCam) btnMeetCam.addEventListener('click', toggleVideo);
  if (btnMeetScreen) btnMeetScreen.addEventListener('click', toggleScreenShare);
  if (btnMeetHangup) btnMeetHangup.addEventListener('click', leaveCall);

  if (btnToggleVideoTheater) {
    btnToggleVideoTheater.addEventListener('click', () => {
      isTheaterMode = !isTheaterMode;
      const container = document.querySelector('.container');
      if (container) container.classList.toggle('theater-active', isTheaterMode);
      btnToggleVideoTheater.classList.toggle('active', isTheaterMode);
    });
  }

  if (btnMinimizeVideoSection) {
    btnMinimizeVideoSection.addEventListener('click', () => {
      isVideoMinimized = !isVideoMinimized;
      videoConferenceSection.classList.toggle('minimized', isVideoMinimized);
      if (minimizeText) minimizeText.textContent = isVideoMinimized ? 'Expandir' : 'Recolher';
    });
  }

  // Global Keyboard Shortcuts (Google Meet style)
  document.addEventListener('keydown', (e) => {
    const tag = document.activeElement ? document.activeElement.tagName.toLowerCase() : '';
    if (tag === 'input' || tag === 'textarea' || (document.activeElement && document.activeElement.isContentEditable)) {
      return; // Do not intercept while typing in chat or inputs
    }
    // Ctrl+D / Cmd+D -> Mute / Unmute mic
    if ((e.ctrlKey || e.metaKey) && (e.key === 'd' || e.key === 'D')) {
      e.preventDefault();
      toggleMic();
    }
    // Ctrl+E / Cmd+E -> Toggle camera
    if ((e.ctrlKey || e.metaKey) && (e.key === 'e' || e.key === 'E')) {
      e.preventDefault();
      toggleVideo();
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

  // Google Meet Style: Setup audio and video transceivers
  const audioTransceiver = pc.addTransceiver('audio', { direction: 'sendrecv' });
  const videoTransceiver = pc.addTransceiver('video', { direction: 'sendrecv' });

  // If local call is already active, attach tracks immediately
  if (inCall && localStream) {
    const aTrack = localStream.getAudioTracks()[0];
    const vTrack = isScreenSharing && screenStream ? screenStream.getVideoTracks()[0] : localStream.getVideoTracks()[0];
    if (aTrack) audioTransceiver.sender.replaceTrack(aTrack);
    if (vTrack && (!isVideoOff || isScreenSharing)) videoTransceiver.sender.replaceTrack(vTrack);
  }

  const peerObj = {
    pc: pc,
    dc: dc,
    audioTransceiver: audioTransceiver,
    videoTransceiver: videoTransceiver,
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

  // Remote Audio/Video Track received
  pc.ontrack = (event) => {
    handleRemoteTrack(targetPeerId, event);
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
    } catch (e) { }

    if (inCall) {
      try {
        channel.send(JSON.stringify({
          type: 'MEDIA_STATE',
          peerId: myPeerId,
          nickname: myNickname,
          inCall: inCall,
          isMuted: isMicMuted,
          isVideoOff: isVideoOff,
          isScreenSharing: isScreenSharing
        }));
      } catch (e) { }
    }

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
      } catch (e) { }
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
    } catch (e) { }
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
  // 11. Media State (Video/Audio call status)
  else if (msg.type === 'MEDIA_STATE') {
    handlePeerMediaState(fromPeerId, msg);
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
    } catch (e) { }
    connectedPeers.delete(peerId);
    pendingIceCandidates.delete(peerId);
    peerRemoteStreams.delete(peerId);
    peerMediaStates.delete(peerId);
    if (peerAudioMeters.has(peerId)) {
      try { peerAudioMeters.get(peerId).stop(); } catch (e) { }
      peerAudioMeters.delete(peerId);
    }
    const tile = document.getElementById(`tile_${peerId}`);
    if (tile) tile.remove();
    const audioEl = document.getElementById(`remoteAudio_${peerId}`);
    if (audioEl) audioEl.remove();

    renderParticipantsList();
    updateVideoGridCount();
  }
}

// Ephemeral MQTT Connection with broker failover
function connectSignaling(topic, onMessageCallback, onConnectCallback) {
  if (mqttClient) {
    try { mqttClient.end(true); } catch (e) { }
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

  // Google Meet UI initial state in room
  if (!inCall) {
    if (callLobbyBanner) callLobbyBanner.classList.remove('hidden');
    if (videoConferenceSection) videoConferenceSection.classList.add('hidden');
  }

  renderParticipantsList();
  updateVideoGridCount();
}

// UI Transition: Leave Room
function leaveRoom() {
  if (inCall) {
    leaveCall();
  }
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
  const myCallBadge = inCall ? `<span style="font-size:0.75rem; color:#10b981; margin-left:4px;" title="Em chamada">${isMicMuted ? '🔇' : '🎙️'}</span>` : '';
  const myChip = document.createElement('div');
  myChip.className = 'peer-chip is-me';
  myChip.innerHTML = `
    <span class="peer-chip-avatar" style="background:${getNicknameColor(myNickname)};"></span>
    <span>${escapeHtml(myNickname)} (Você) ${myCallBadge}</span>
  `;
  participantsChips.appendChild(myChip);

  // 2. Add Connected/Connecting Peers
  connectedPeers.forEach((peer, peerId) => {
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

    const peerMedia = peerMediaStates.get(peerId);
    let callIcon = '';
    if (peerMedia && peerMedia.inCall) {
      callIcon = `<span style="font-size:0.75rem; color:#10b981; margin-left:4px;" title="Em chamada">${peerMedia.isMuted ? '🔇' : '🎙️'}</span>`;
    }

    const chip = document.createElement('div');
    chip.className = 'peer-chip';
    chip.innerHTML = `
      <span class="peer-chip-avatar" style="background:${getNicknameColor(peer.nickname)};"></span>
      <span>${escapeHtml(peer.nickname)} ${badgeHtml} ${callIcon}</span>
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

  if (isP2P) {
    // ── High-throughput P2P pipeline ─────────────────────────────────────
    // Keep the DataChannel buffer filled between BUFFER_LOW and BUFFER_HIGH.
    // Setting bufferedAmountLowThreshold means onbufferedamountlow fires while
    // there's still headroom, eliminating the stall-to-zero bottleneck.
    const dc = targetPeer.dc;
    dc.bufferedAmountLowThreshold = BUFFER_LOW;

    await new Promise((resolve, reject) => {
      let cancelled = false;

      function pump() {
        if (cancelled) return;
        // Fill the buffer up to BUFFER_HIGH in one tight loop
        while (offset < file.size && dc.bufferedAmount < BUFFER_HIGH) {
          const end = Math.min(offset + chunkSize, file.size);
          dc.send(fileBuffer.slice(offset, end));
          offset = end;
          updateTransferProgress(offset, file.size);
        }
        if (offset >= file.size) {
          resolve();
        } else {
          // Buffer is full — wait for it to drain to BUFFER_LOW, then pump again
          dc.onbufferedamountlow = () => {
            dc.onbufferedamountlow = null;
            pump();
          };
        }
      }

      pump();
    });

    // Restore default threshold
    targetPeer.dc.bufferedAmountLowThreshold = 0;

  } else {
    // ── Relay mode: chunked over MQTT ────────────────────────────────────
    while (offset < file.size) {
      const chunk = fileBuffer.slice(offset, offset + chunkSize);
      const chunkArray = Array.from(new Uint8Array(chunk));
      sendSignalingMessage(currentSignalingTopic, {
        type: 'RELAY_CHUNK',
        targetPeerId: targetPeerId,
        fromPeerId: myPeerId,
        chunk: chunkArray
      });
      offset += chunk.byteLength;
      updateTransferProgress(offset, file.size);
      await new Promise(r => setTimeout(r, 2));
    }
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
    } catch (e) { }
  });
  connectedPeers.clear();
  pendingIceCandidates.clear();
  peerRemoteStreams.clear();
  peerMediaStates.clear();
  peerAudioMeters.forEach(m => { try { m.stop(); } catch (e) { } });
  peerAudioMeters.clear();

  if (mqttClient) {
    mqttClient.end();
    mqttClient = null;
  }
}

// =========================================================================
// GOOGLE MEET STYLE VIDEO & AUDIO CONFERENCE ENGINE
// =========================================================================

function getInitials(name) {
  if (!name) return 'G';
  const parts = name.trim().split(/\s+/);
  if (parts.length >= 2) {
    return (parts[0][0] + parts[1][0]).toUpperCase();
  }
  return name.trim().substring(0, 2).toUpperCase();
}

// Join the Video/Audio Call
async function joinCall(options = { video: true }) {
  if (inCall) return;

  const preferVideo = options.video !== false;

  try {
    // Attempt to acquire video + audio
    try {
      localStream = await navigator.mediaDevices.getUserMedia({
        audio: {
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true
        },
        video: preferVideo ? {
          width: { ideal: 1280 },
          height: { ideal: 720 },
          facingMode: 'user'
        } : false
      });
      isVideoOff = !preferVideo;
    } catch (err) {
      console.warn('Falha ao obter vídeo e áudio juntos, tentando apenas áudio:', err);
      // Fallback: audio only
      localStream = await navigator.mediaDevices.getUserMedia({
        audio: {
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true
        },
        video: false
      });
      isVideoOff = true;
    }

    inCall = true;
    isMicMuted = false;

    // Attach stream to local preview
    if (localVideo) {
      localVideo.srcObject = localStream;
      localVideo.classList.add('video-mirrored');
    }

    // Set audio track state
    const audioTrack = localStream.getAudioTracks()[0];
    if (audioTrack) {
      audioTrack.enabled = !isMicMuted;
    }

    // Set video track state
    const videoTrack = localStream.getVideoTracks()[0];
    if (videoTrack) {
      videoTrack.enabled = !isVideoOff;
    }

    // Replace tracks in all connected peers' transceivers
    connectedPeers.forEach(peer => {
      if (peer.audioTransceiver && audioTrack) {
        try { peer.audioTransceiver.sender.replaceTrack(audioTrack); } catch (e) { }
      }
      if (peer.videoTransceiver && videoTrack && !isVideoOff) {
        try { peer.videoTransceiver.sender.replaceTrack(videoTrack); } catch (e) { }
      }
    });

    // Setup Local Audio Meter for Speaking Indicator
    if (localStream.getAudioTracks().length > 0) {
      if (localAudioMeter) {
        try { localAudioMeter.stop(); } catch (e) { }
      }
      localAudioMeter = setupAudioMeter(localStream, (isSpeaking) => {
        if (tileLocal) tileLocal.classList.toggle('is-speaking', isSpeaking && !isMicMuted);
      });
    }

    // Update UI elements
    updateLocalMediaUI();
    if (callLobbyBanner) callLobbyBanner.classList.add('hidden');
    if (videoConferenceSection) videoConferenceSection.classList.remove('hidden');

    updateVideoGridCount();
    renderParticipantsList();
    broadcastMediaState();

    appendSystemMessage('🎥 Você entrou na chamada de vídeo e áudio.');
  } catch (err) {
    console.error('Erro ao acessar microfone/câmera:', err);
    alert('Não foi possível acessar a câmera ou microfone. Verifique as permissões de mídia do navegador.');
  }
}

// Leave the Video/Audio Call
function leaveCall() {
  if (!inCall && !localStream) return;

  inCall = false;

  // Stop screen sharing if active
  if (isScreenSharing) {
    stopScreenShare(false);
  }

  // Stop local stream tracks
  if (localStream) {
    localStream.getTracks().forEach(track => {
      try { track.stop(); } catch (e) { }
    });
    localStream = null;
  }

  if (localVideo) {
    localVideo.srcObject = null;
  }

  // Stop audio meter
  if (localAudioMeter) {
    try { localAudioMeter.stop(); } catch (e) { }
    localAudioMeter = null;
  }
  if (tileLocal) tileLocal.classList.remove('is-speaking');

  // Clear tracks from transceivers in all connected peers
  connectedPeers.forEach(peer => {
    if (peer.audioTransceiver) {
      try { peer.audioTransceiver.sender.replaceTrack(null); } catch (e) { }
    }
    if (peer.videoTransceiver) {
      try { peer.videoTransceiver.sender.replaceTrack(null); } catch (e) { }
    }
  });

  // Reset UI
  if (videoConferenceSection) videoConferenceSection.classList.add('hidden');
  if (callLobbyBanner && inRoom) callLobbyBanner.classList.remove('hidden');

  updateVideoGridCount();
  renderParticipantsList();
  broadcastMediaState();

  appendSystemMessage('📞 Você saiu da chamada.');
}

// Toggle Microphone (Mute / Unmute)
function toggleMic() {
  if (!inCall) {
    joinCall({ video: true });
    return;
  }

  if (!localStream) return;
  const audioTracks = localStream.getAudioTracks();
  if (audioTracks.length === 0) return;

  isMicMuted = !isMicMuted;
  audioTracks.forEach(t => { t.enabled = !isMicMuted; });

  if (tileLocal && isMicMuted) {
    tileLocal.classList.remove('is-speaking');
  }

  updateLocalMediaUI();
  renderParticipantsList();
  broadcastMediaState();
}

// Toggle Video (Camera On / Off)
async function toggleVideo() {
  if (!inCall) {
    joinCall({ video: true });
    return;
  }

  if (!localStream) return;

  if (isScreenSharing) {
    stopScreenShare(true);
    return;
  }

  const videoTracks = localStream.getVideoTracks();
  if (videoTracks.length > 0) {
    isVideoOff = !isVideoOff;
    videoTracks.forEach(t => { t.enabled = !isVideoOff; });

    // Replace track or set to null in transceivers
    const vTrack = isVideoOff ? null : videoTracks[0];
    connectedPeers.forEach(peer => {
      if (peer.videoTransceiver) {
        try { peer.videoTransceiver.sender.replaceTrack(vTrack); } catch (e) { }
      }
    });
  } else if (isVideoOff) {
    // If user previously joined audio-only, try to get video now
    try {
      const vStream = await navigator.mediaDevices.getUserMedia({
        video: { width: { ideal: 1280 }, height: { ideal: 720 }, facingMode: 'user' }
      });
      const newVTrack = vStream.getVideoTracks()[0];
      localStream.addTrack(newVTrack);
      isVideoOff = false;

      connectedPeers.forEach(peer => {
        if (peer.videoTransceiver) {
          try { peer.videoTransceiver.sender.replaceTrack(newVTrack); } catch (e) { }
        }
      });
    } catch (err) {
      console.warn('Falha ao ativar câmera:', err);
      alert('Não foi possível acessar a câmera.');
      isVideoOff = true;
    }
  }

  updateLocalMediaUI();
  broadcastMediaState();
}

// Toggle Screen Sharing (Google Meet style presentation)
async function toggleScreenShare() {
  if (!inCall) {
    await joinCall({ video: true });
  }

  if (isScreenSharing) {
    stopScreenShare(true);
  } else {
    try {
      screenStream = await navigator.mediaDevices.getDisplayMedia({
        video: true,
        audio: true
      });
      isScreenSharing = true;

      const screenTrack = screenStream.getVideoTracks()[0];
      screenTrack.onended = () => {
        stopScreenShare(true);
      };

      // Replace video track in transceivers with screen track
      connectedPeers.forEach(peer => {
        if (peer.videoTransceiver) {
          try { peer.videoTransceiver.sender.replaceTrack(screenTrack); } catch (e) { }
        }
      });

      // Update local preview with screen stream
      if (localVideo) {
        localVideo.srcObject = screenStream;
        localVideo.classList.remove('video-mirrored');
      }

      updateLocalMediaUI();
      broadcastMediaState();
    } catch (err) {
      console.log('Compartilhamento de tela cancelado:', err);
    }
  }
}

// Stop Screen Share and restore camera
function stopScreenShare(notify = true) {
  if (!isScreenSharing && !screenStream) return;
  isScreenSharing = false;

  if (screenStream) {
    screenStream.getTracks().forEach(t => {
      try { t.stop(); } catch (e) { }
    });
    screenStream = null;
  }

  const camTrack = localStream ? localStream.getVideoTracks()[0] : null;
  const activeCamTrack = (!isVideoOff && camTrack) ? camTrack : null;

  connectedPeers.forEach(peer => {
    if (peer.videoTransceiver) {
      try { peer.videoTransceiver.sender.replaceTrack(activeCamTrack); } catch (e) { }
    }
  });

  if (localVideo && localStream) {
    localVideo.srcObject = localStream;
    localVideo.classList.add('video-mirrored');
  }

  updateLocalMediaUI();
  if (notify) {
    broadcastMediaState();
  }
}

// Web Audio API: Speaking Level Analyzer
function setupAudioMeter(stream, onLevelChange) {
  try {
    const AudioCtx = window.AudioContext || window.webkitAudioContext;
    if (!AudioCtx) return { stop: () => { } };

    const audioContext = new AudioCtx();
    const source = audioContext.createMediaStreamSource(stream);
    const analyser = audioContext.createAnalyser();
    analyser.fftSize = 256;
    analyser.smoothingTimeConstant = 0.5;
    source.connect(analyser);

    const dataArray = new Uint8Array(analyser.frequencyBinCount);
    let isSpeaking = false;
    let animId = null;

    function checkLevel() {
      analyser.getByteFrequencyData(dataArray);
      let sum = 0;
      for (let i = 0; i < dataArray.length; i++) {
        sum += dataArray[i];
      }
      const avg = sum / dataArray.length;
      const speakingNow = avg > 14;
      if (speakingNow !== isSpeaking) {
        isSpeaking = speakingNow;
        onLevelChange(isSpeaking);
      }
      animId = requestAnimationFrame(checkLevel);
    }
    checkLevel();

    return {
      stop: () => {
        if (animId) cancelAnimationFrame(animId);
        try { audioContext.close(); } catch (e) { }
      }
    };
  } catch (e) {
    return { stop: () => { } };
  }
}

// Broadcast Current Media State to all peers
function broadcastMediaState() {
  const state = {
    type: 'MEDIA_STATE',
    peerId: myPeerId,
    nickname: myNickname,
    inCall: inCall,
    isMuted: isMicMuted,
    isVideoOff: isVideoOff,
    isScreenSharing: isScreenSharing
  };
  broadcastAppMessage(state);
}

// Handle Remote Peer Media State
function handlePeerMediaState(fromPeerId, state) {
  const oldState = peerMediaStates.get(fromPeerId);
  peerMediaStates.set(fromPeerId, state);

  const peer = connectedPeers.get(fromPeerId);
  const peerNick = state.nickname || (peer && peer.nickname) || 'Um participante';

  if (!oldState || !oldState.inCall) {
    if (state.inCall) {
      appendSystemMessage(`🎥 <strong>${escapeHtml(peerNick)}</strong> entrou na chamada de vídeo.`);
    }
  } else if (oldState.inCall && !state.inCall) {
    appendSystemMessage(`📞 <strong>${escapeHtml(peerNick)}</strong> saiu da chamada de vídeo.`);
  }

  updateRemoteVideoTile(fromPeerId);
  renderParticipantsList();
}

// Handle Incoming Remote Track from WebRTC
function handleRemoteTrack(peerId, event) {
  let stream = peerRemoteStreams.get(peerId);
  if (!stream) {
    stream = new MediaStream();
    peerRemoteStreams.set(peerId, stream);
  }

  if (!stream.getTracks().some(t => t.id === event.track.id)) {
    stream.addTrack(event.track);
  }

  if (event.track.kind === 'audio') {
    if (peerAudioMeters.has(peerId)) {
      try { peerAudioMeters.get(peerId).stop(); } catch (e) { }
    }
    const meter = setupAudioMeter(stream, (isSpeaking) => {
      const tile = document.getElementById(`tile_${peerId}`);
      const mediaState = peerMediaStates.get(peerId);
      const isMuted = mediaState ? mediaState.isMuted : false;
      if (tile) tile.classList.toggle('is-speaking', isSpeaking && !isMuted);
    });
    peerAudioMeters.set(peerId, meter);
  }

  event.track.onmute = () => updateRemoteVideoTile(peerId);
  event.track.onunmute = () => updateRemoteVideoTile(peerId);
  event.track.onended = () => updateRemoteVideoTile(peerId);

  updateRemoteVideoTile(peerId);
}

// Update Local Media UI Buttons and Badges
function updateLocalMediaUI() {
  if (localTileName) localTileName.textContent = `${myNickname} (Você)`;
  if (localAvatarName) localAvatarName.textContent = `${myNickname} (Você)`;
  if (localAvatarInitial) localAvatarInitial.textContent = getInitials(myNickname);
  if (localAvatarCircle) localAvatarCircle.style.background = getNicknameColor(myNickname);

  // Mic state
  if (btnMeetMic) {
    btnMeetMic.classList.toggle('is-muted', isMicMuted);
    const micOn = btnMeetMic.querySelector('.icon-on');
    const micOff = btnMeetMic.querySelector('.icon-off');
    if (micOn) micOn.classList.toggle('hidden', isMicMuted);
    if (micOff) micOff.classList.toggle('hidden', !isMicMuted);
    btnMeetMic.title = isMicMuted ? 'Ativar microfone (Ctrl+D)' : 'Desativar microfone (Ctrl+D)';
  }

  if (localTileMicIcon) {
    localTileMicIcon.classList.toggle('muted', isMicMuted);
    const micOn = localTileMicIcon.querySelector('.icon-mic-on');
    const micOff = localTileMicIcon.querySelector('.icon-mic-off');
    if (micOn) micOn.classList.toggle('hidden', isMicMuted);
    if (micOff) micOff.classList.toggle('hidden', !isMicMuted);
    localTileMicIcon.title = isMicMuted ? 'Microfone desativado' : 'Microfone ativo';
  }

  // Camera state
  const isCamDisabled = isVideoOff && !isScreenSharing;
  if (btnMeetCam) {
    btnMeetCam.classList.toggle('is-off', isCamDisabled);
    const camOn = btnMeetCam.querySelector('.icon-on');
    const camOff = btnMeetCam.querySelector('.icon-off');
    if (camOn) camOn.classList.toggle('hidden', isCamDisabled);
    if (camOff) camOff.classList.toggle('hidden', !isCamDisabled);
    btnMeetCam.title = isCamDisabled ? 'Ativar câmera (Ctrl+E)' : 'Desativar câmera (Ctrl+E)';
  }

  if (localTileCamIcon) {
    localTileCamIcon.classList.toggle('off', isCamDisabled);
    const camOn = localTileCamIcon.querySelector('.icon-cam-on');
    const camOff = localTileCamIcon.querySelector('.icon-cam-off');
    if (camOn) camOn.classList.toggle('hidden', isCamDisabled);
    if (camOff) camOff.classList.toggle('hidden', !isCamDisabled);
    localTileCamIcon.title = isCamDisabled ? 'Câmera desligada' : 'Câmera ativa';
  }

  if (localAvatarPlaceholder) {
    localAvatarPlaceholder.classList.toggle('hidden', !isCamDisabled);
  }

  // Screen share state
  if (btnMeetScreen) {
    btnMeetScreen.classList.toggle('is-active', isScreenSharing);
    btnMeetScreen.title = isScreenSharing ? 'Parar apresentação de tela' : 'Apresentar agora / Compartilhar tela';
  }

  if (localScreenBadge) {
    localScreenBadge.classList.toggle('hidden', !isScreenSharing);
  }

  if (localVideo) {
    localVideo.classList.toggle('video-mirrored', !isScreenSharing);
  }
}

// Update Remote Peer Video Tile in the Grid
function updateRemoteVideoTile(peerId) {
  const peer = connectedPeers.get(peerId);
  const mediaState = peerMediaStates.get(peerId) || { inCall: false, isMuted: false, isVideoOff: true, isScreenSharing: false };
  const stream = peerRemoteStreams.get(peerId);
  const peerNick = (peer && peer.nickname) || mediaState.nickname || 'Ghost Peer';

  let tile = document.getElementById(`tile_${peerId}`);

  // If remote peer is not in call, remove tile and audio element
  if (!mediaState.inCall) {
    if (tile) tile.remove();
    const audioEl = document.getElementById(`remoteAudio_${peerId}`);
    if (audioEl) audioEl.remove();
    updateVideoGridCount();
    return;
  }

  // If tile doesn't exist, create it dynamically
  if (!tile) {
    tile = document.createElement('div');
    tile.className = 'video-tile remote-tile';
    tile.id = `tile_${peerId}`;
    tile.innerHTML = `
      <video id="video_${peerId}" class="video-element" autoplay playsinline></video>
      <div class="avatar-placeholder" id="avatar_${peerId}">
        <div class="avatar-circle" style="background: ${getNicknameColor(peerNick)}">
          <span>${getInitials(peerNick)}</span>
        </div>
        <span class="avatar-name">${escapeHtml(peerNick)}</span>
        <span class="avatar-status">Câmera desativada</span>
      </div>
      <div class="tile-overlay-gradient"></div>
      <div class="tile-top-badges">
        <span class="tile-badge screen-badge hidden" id="screenBadge_${peerId}">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="3" width="20" height="14" rx="2" ry="2"></rect><line x1="8" y1="21" x2="16" y2="21"></line><line x1="12" y1="17" x2="12" y2="21"></line></svg>
          Tela compartilhada
        </span>
      </div>
      <div class="tile-bottom-bar">
        <span class="tile-name-tag">
          <span id="name_${peerId}">${escapeHtml(peerNick)}</span>
        </span>
        <div class="tile-status-icons">
          <span class="tile-status-icon mic-status" id="micIcon_${peerId}" title="Microfone ativo">
            <svg class="icon-mic-on" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"></path><path d="M19 10v2a7 7 0 0 1-14 0v-2"></path><line x1="12" y1="19" x2="12" y2="23"></line><line x1="8" y1="23" x2="16" y2="23"></line></svg>
            <svg class="icon-mic-off hidden" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M9 9v3a3 3 0 0 0 5.12 2.12M15 9.34V4a3 3 0 0 0-5.94-.6"></path><path d="M17 16.95A7 7 0 0 1 5 12v-2m14 0v2a7 7 0 0 1-.11 1.23"></path><line x1="12" y1="19" x2="12" y2="23"></line><line x1="8" y1="23" x2="16" y2="23"></line></svg>
          </span>
          <span class="tile-status-icon cam-status" id="camIcon_${peerId}" title="Câmera ativa">
            <svg class="icon-cam-on" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="23 7 16 12 23 17 23 7"></polygon><rect x="1" y="5" width="15" height="14" rx="2" ry="2"></rect></svg>
            <svg class="icon-cam-off hidden" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="1" y1="1" x2="23" y2="23"></line><path d="M21 21l-3.6-2.4"></path><path d="M16 16V5a2 2 0 0 0-2-2H7.82"></path><polygon points="23 7 16 12 23 17 23 7"></polygon><rect x="1" y="5" width="15" height="14" rx="2" ry="2"></rect></svg>
          </span>
        </div>
      </div>
    `;
    if (videoGrid) videoGrid.appendChild(tile);
  }

  // Attach stream to remote video element
  const videoEl = document.getElementById(`video_${peerId}`);
  if (videoEl && stream && videoEl.srcObject !== stream) {
    videoEl.srcObject = stream;
  }

  // Also ensure audio is playing in audio container for guaranteed playback
  if (stream && remoteAudiosContainer) {
    let audioEl = document.getElementById(`remoteAudio_${peerId}`);
    if (!audioEl) {
      audioEl = document.createElement('audio');
      audioEl.id = `remoteAudio_${peerId}`;
      audioEl.autoplay = true;
      audioEl.playsInline = true;
      remoteAudiosContainer.appendChild(audioEl);
    }
    if (audioEl.srcObject !== stream) {
      audioEl.srcObject = stream;
    }
  }

  // Update nickname tag
  const nameEl = document.getElementById(`name_${peerId}`);
  if (nameEl) nameEl.textContent = peerNick;

  // Update Avatar placeholder vs live video
  const avatarEl = document.getElementById(`avatar_${peerId}`);
  const hasLiveVideoTrack = stream && stream.getVideoTracks().some(t => t.enabled && t.readyState === 'live');
  const showAvatar = mediaState.isVideoOff || !hasLiveVideoTrack;

  if (avatarEl) {
    avatarEl.classList.toggle('hidden', !showAvatar);
  }

  // Update Screen badge
  const screenBadge = document.getElementById(`screenBadge_${peerId}`);
  if (screenBadge) {
    screenBadge.classList.toggle('hidden', !mediaState.isScreenSharing);
  }

  // Update Mic icon
  const micIcon = document.getElementById(`micIcon_${peerId}`);
  if (micIcon) {
    const isMuted = mediaState.isMuted;
    micIcon.classList.toggle('muted', isMuted);
    micIcon.querySelector('.icon-mic-on').classList.toggle('hidden', isMuted);
    micIcon.querySelector('.icon-mic-off').classList.toggle('hidden', !isMuted);
    micIcon.title = isMuted ? 'Participante com microfone mutado' : 'Microfone ativo';
  }

  // Update Cam icon
  const camIcon = document.getElementById(`camIcon_${peerId}`);
  if (camIcon) {
    const isCamOff = mediaState.isVideoOff;
    camIcon.classList.toggle('off', isCamOff);
    camIcon.querySelector('.icon-cam-on').classList.toggle('hidden', isCamOff);
    camIcon.querySelector('.icon-cam-off').classList.toggle('hidden', !isCamOff);
    camIcon.title = isCamOff ? 'Câmera desativada' : 'Câmera ativa';
  }

  updateVideoGridCount();
}

// Update Active Call Badges and Dynamic Grid Sizing
function updateVideoGridCount() {
  const activePeersInCall = Array.from(peerMediaStates.values()).filter(s => s.inCall).length;
  const totalInCall = (inCall ? 1 : 0) + activePeersInCall;

  if (videoGrid) {
    videoGrid.setAttribute('data-count', String(Math.max(1, totalInCall)));
  }
  if (videoParticipantsCount) {
    videoParticipantsCount.textContent = totalInCall === 1 ? '1 participante' : `${totalInCall} participantes`;
  }

  // Update top bar call badge
  if (callActiveBadge && callButtonText && btnToggleCallHeader) {
    if (totalInCall > 0) {
      callActiveBadge.textContent = String(totalInCall);
      callActiveBadge.classList.remove('hidden');
      callButtonText.textContent = inCall ? 'Em Chamada' : 'Entrar na Chamada';
      btnToggleCallHeader.classList.toggle('in-call', inCall);
    } else {
      callActiveBadge.classList.add('hidden');
      callButtonText.textContent = 'Entrar na Chamada';
      btnToggleCallHeader.classList.remove('in-call');
    }
  }
}

