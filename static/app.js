/**
 * GhostDrop - Direct & Secure File Sharing and Real-time Chat
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
const BUFFER_LOW = 2 * 1024 * 1024;     // 2 MB: resume sending below this

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

// Mobile Room Switcher Elements
const mobileRoomSwitcher = document.getElementById('mobileRoomSwitcher');
const btnSwitchChat = document.getElementById('btnSwitchChat');
const btnSwitchFiles = document.getElementById('btnSwitchFiles');
const chatPanel = document.getElementById('chatPanel');
const filesPanel = document.getElementById('filesPanel');
const chatUnreadDot = document.getElementById('chatUnreadDot');
const filesUnreadDot = document.getElementById('filesUnreadDot');
let activeMobilePanel = 'chat';

function switchMobilePanel(panel) {
  activeMobilePanel = panel;
  if (!btnSwitchChat || !btnSwitchFiles || !chatPanel || !filesPanel) return;
  if (panel === 'chat') {
    btnSwitchChat.classList.add('active');
    btnSwitchFiles.classList.remove('active');
    chatPanel.classList.add('mobile-active');
    filesPanel.classList.remove('mobile-active');
    if (chatUnreadDot) chatUnreadDot.classList.add('hidden');
  } else {
    btnSwitchFiles.classList.add('active');
    btnSwitchChat.classList.remove('active');
    filesPanel.classList.add('mobile-active');
    chatPanel.classList.remove('mobile-active');
    if (filesUnreadDot) filesUnreadDot.classList.add('hidden');
  }
}

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

function getNicknameColor(nick) {
  const palette = [
    '#fb7185', '#f43f5e', '#e11d48', '#fda4af',
    '#f59e0b', '#fb923c', '#ec4899', '#f87171'
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
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function formatBytes(bytes, decimals = 2) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

function formatTime() {
  const d = new Date();
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
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

// Random Room Code Generation
function generateRandomCode() {
  const n1 = Math.floor(100 + Math.random() * 900);
  const n2 = Math.floor(100 + Math.random() * 900);
  return `${n1}-${n2}`;
}

function formatCode(val) {
  const digits = val.replace(/\D/g, '').substring(0, 6);
  if (digits.length > 3) {
    return `${digits.slice(0, 3)}-${digits.slice(3)}`;
  }
  return digits;
}

function cleanCode(val) {
  return val.replace(/\D/g, '');
}

// URL Parameter Handling
function checkUrlParams() {
  const params = new URLSearchParams(window.location.search);
  const code = params.get('room') || params.get('receive');
  if (code) {
    const formatted = formatCode(code);
    roomCodeInput.value = formatted;
    enterSelectedRoom();
  }
}

// Cryptography Helpers (PBKDF2 + AES-GCM-256)
async function deriveKey(codeStr) {
  const enc = new TextEncoder();
  const rawKey = enc.encode('ghostdrop-p2p-' + codeStr);
  const keyMaterial = await crypto.subtle.importKey(
    'raw',
    rawKey,
    { name: 'PBKDF2' },
    false,
    ['deriveKey']
  );
  const salt = enc.encode('ghostdrop-mesh-salt-2026');
  return crypto.subtle.deriveKey(
    {
      name: 'PBKDF2',
      salt: salt,
      iterations: 100000,
      hash: 'SHA-256'
    },
    keyMaterial,
    { name: 'AES-GCM', length: 256 },
    false,
    ['encrypt', 'decrypt']
  );
}

async function encryptPayload(key, obj) {
  const enc = new TextEncoder();
  const data = enc.encode(JSON.stringify(obj));
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const encrypted = await crypto.subtle.encrypt(
    { name: 'AES-GCM', iv },
    key,
    data
  );
  return {
    iv: Array.from(iv),
    payload: Array.from(new Uint8Array(encrypted))
  };
}

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

  // Mobile room panel tabs
  if (btnSwitchChat) {
    btnSwitchChat.addEventListener('click', () => switchMobilePanel('chat'));
  }
  if (btnSwitchFiles) {
    btnSwitchFiles.addEventListener('click', () => switchMobilePanel('files'));
  }

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

  // In-Room QR Code Toggle
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
    if (confirm('Deseja sair desta sala?')) {
      leaveRoom();
    }
  });

  // Panic button
  panicButton.addEventListener('click', () => {
    if (confirm('Isso irá encerrar o executável e destruir todos os buffers e sessões da memória. Continuar?')) {
      resetAllConnections();
      fetch('/api/shutdown', { method: 'POST' }).finally(() => {
        document.body.innerHTML = `
          <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100vh;background:#0a0608;color:#fda4af;font-family:sans-serif;text-align:center;padding:20px;">
            <h1 style="color:#fff1f2;margin-bottom:12px;">Sessão Encerrada</h1>
            <p>O executável foi finalizado e todos os buffers foram destruídos da memória.</p>
            <p style="margin-top:8px;font-size:0.85rem;color:#9f7580;">Você pode fechar esta aba com segurança.</p>
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

async function enterSelectedRoom() {
  const code = cleanCode(roomCodeInput.value);
  if (code.length !== 6) {
    alert('Por favor, informe um código de sala válido com 6 dígitos (ex: 849-210).');
    return;
  }

  activeSessionRoomCode = formatCode(code);
  connectStatusBanner.classList.remove('hidden');
  connectStatusText.textContent = `Entrando na sala #${activeSessionRoomCode}...`;

  encryptionKey = await deriveKey(code);
  currentSignalingTopic = `ghostdrop/room/${code}`;

  setupMqttSignaling(currentSignalingTopic);
}

function setupMqttSignaling(topic) {
  if (mqttClient) {
    try { mqttClient.end(); } catch (e) { }
    mqttClient = null;
  }

  const broker = MQTT_BROKERS[currentBrokerIndex];
  console.log(`[Signal] Conectando ao broker ${broker}`);
  const clientId = 'gd_' + myPeerId;

  mqttClient = mqtt.connect(broker, {
    clientId: clientId,
    clean: true,
    connectTimeout: 5000,
    reconnectPeriod: 2000
  });

  mqttClient.on('connect', () => {
    console.log(`[Signal] Conectado ao broker MQTT. Inscrevendo no tópico: ${topic}`);
    mqttClient.subscribe(topic, { qos: 0 }, (err) => {
      if (!err) {
        enterRoomUI(activeSessionRoomCode);

        // Announce presence in the room
        sendSignalingMessage(topic, {
          type: 'ANNOUNCE',
          peerId: myPeerId,
          nickname: myNickname
        });

        // Periodic presence heartbeat
        clearInterval(heartbeatTimer);
        heartbeatTimer = setInterval(() => {
          if (inRoom) {
            sendSignalingMessage(topic, {
              type: 'HEARTBEAT',
              peerId: myPeerId,
              nickname: myNickname
            });
          }
        }, 8000);
      } else {
        alert('Erro ao conectar à sala. Tente novamente.');
      }
    });
  });

  mqttClient.on('error', (err) => {
    console.warn(`[Signal] Erro no broker ${broker}:`, err);
    currentBrokerIndex = (currentBrokerIndex + 1) % MQTT_BROKERS.length;
  });

  mqttClient.on('message', async (recvTopic, message) => {
    if (recvTopic !== topic) return;
    try {
      const encrypted = JSON.parse(message.toString());
      const payload = await decryptPayload(encryptionKey, encrypted);
      if (!payload) return;

      // Ignore messages sent by ourselves
      if (payload.fromPeerId === myPeerId || payload.peerId === myPeerId) return;

      handleSignalingMessage(payload);
    } catch (e) {
      console.warn('[Signal] Erro ao decifrar mensagem de sinal:', e);
    }
  });
}

async function sendSignalingMessage(topic, msgObj) {
  if (!mqttClient || !mqttClient.connected || !encryptionKey) return;
  msgObj.fromPeerId = myPeerId;
  msgObj.timestamp = Date.now();
  const encrypted = await encryptPayload(encryptionKey, msgObj);
  mqttClient.publish(topic, JSON.stringify(encrypted), { qos: 0, retain: false });
}

// Broadcast application message (chat, file notifications) over direct DataChannels
function broadcastAppMessage(msgObj) {
  msgObj.fromPeerId = myPeerId;
  const raw = JSON.stringify(msgObj);
  let directCount = 0;

  connectedPeers.forEach(peer => {
    if (peer.state === 'connected' && peer.mode === 'p2p' && peer.dc && peer.dc.readyState === 'open') {
      try {
        peer.dc.send(raw);
        directCount++;
      } catch (e) { }
    } else {
      sendSignalingMessage(currentSignalingTopic, {
        type: 'RELAY_MSG',
        targetPeerId: peer.peerId,
        content: msgObj
      });
    }
  });
}

function sendPeerAppMessage(targetPeerId, msgObj) {
  msgObj.fromPeerId = myPeerId;
  const peer = connectedPeers.get(targetPeerId);
  if (peer && peer.mode === 'p2p' && peer.dc && peer.dc.readyState === 'open') {
    try {
      peer.dc.send(JSON.stringify(msgObj));
      return;
    } catch (e) { }
  }
  sendSignalingMessage(currentSignalingTopic, {
    type: 'RELAY_MSG',
    targetPeerId: targetPeerId,
    content: msgObj
  });
}

// Handle Incoming Signaling Messages
async function handleSignalingMessage(msg) {
  const senderId = msg.fromPeerId || msg.peerId;
  if (!senderId || senderId === myPeerId) return;

  // 1. ANNOUNCE / HEARTBEAT: A peer joined or refreshed in the room
  if (msg.type === 'ANNOUNCE' || msg.type === 'HEARTBEAT') {
    const isPolite = myPeerId < senderId;
    const existing = getOrCreatePeer(senderId, msg.nickname || 'Ghost', isPolite);

    if (msg.type === 'ANNOUNCE') {
      sendSignalingMessage(currentSignalingTopic, {
        type: 'ANNOUNCE_ACK',
        targetPeerId: senderId,
        fromPeerId: myPeerId,
        nickname: myNickname
      });
    }

    if (!isPolite && existing.state !== 'connected' && !existing.hasAnswer) {
      initiateOffer(senderId);
    }
  }

  // 2. ANNOUNCE_ACK: Reply from existing room members
  else if (msg.type === 'ANNOUNCE_ACK' && msg.targetPeerId === myPeerId) {
    const isPolite = myPeerId < senderId;
    const peer = getOrCreatePeer(senderId, msg.nickname || 'Ghost', isPolite);
    if (!isPolite && peer.state !== 'connected' && !peer.hasAnswer) {
      initiateOffer(senderId);
    }
  }

  // 3. WebRTC OFFER
  else if (msg.type === 'OFFER' && msg.targetPeerId === myPeerId) {
    const peer = getOrCreatePeer(senderId, msg.nickname || 'Ghost', myPeerId < senderId);
    await handleOffer(senderId, msg.sdp);
  }

  // 4. WebRTC ANSWER
  else if (msg.type === 'ANSWER' && msg.targetPeerId === myPeerId) {
    const peer = connectedPeers.get(senderId);
    if (peer) {
      await handleAnswer(senderId, msg.sdp);
    }
  }

  // 5. WebRTC ICE Candidate
  else if (msg.type === 'ICE' && msg.targetPeerId === myPeerId) {
    await handleIceCandidate(senderId, msg.candidate);
  }

  // 6. Seamless Fallback Relay Message
  else if (msg.type === 'RELAY_MSG' && (msg.targetPeerId === myPeerId || !msg.targetPeerId)) {
    onDataMessage(senderId, msg.content);
  }
}

function getOrCreatePeer(peerId, nickname, isPolite) {
  if (connectedPeers.has(peerId)) {
    const existing = connectedPeers.get(peerId);
    if (nickname && existing.nickname !== nickname) {
      existing.nickname = nickname;
      renderParticipantsList();
    }
    return existing;
  }
  return initPeerConnection(peerId, nickname, isPolite);
}

// Initialize WebRTC Peer Connection (DataChannel only)
function initPeerConnection(targetPeerId, targetNickname, isPolite) {
  const pc = new RTCPeerConnection(STUN_SERVERS);
  const dc = pc.createDataChannel('ghostdrop', { negotiated: true, id: 0 });
  dc.binaryType = 'arraybuffer';

  const peerObj = {
    peerId: targetPeerId,
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

  // Fallback timer: if direct P2P takes more than 5s, activate relay mode seamlessly
  peerObj.relayFallbackTimer = setTimeout(() => {
    if (peerObj.state !== 'connected') {
      markConnected('relay');
      appendSystemMessage(`🛡️ Conexão estabelecida com <strong>${escapeHtml(peerObj.nickname)}</strong>.`);
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
    } else if (pc.connectionState === 'failed' || pc.connectionState === 'disconnected') {
      markConnected('relay');
    }
  };

  return peerObj;
}

async function initiateOffer(targetPeerId) {
  const peer = connectedPeers.get(targetPeerId);
  if (!peer) return;
  try {
    const offer = await peer.pc.createOffer();
    await peer.pc.setLocalDescription(offer);
    sendSignalingMessage(currentSignalingTopic, {
      type: 'OFFER',
      targetPeerId: targetPeerId,
      fromPeerId: myPeerId,
      nickname: myNickname,
      sdp: offer
    });
  } catch (err) {
    console.warn(`[WebRTC] Erro ao criar oferta para ${targetPeerId}:`, err);
  }
}

async function handleOffer(targetPeerId, offerSdp) {
  const peer = connectedPeers.get(targetPeerId);
  if (!peer) return;

  try {
    const pc = peer.pc;
    const isCollision = (pc.signalingState !== 'stable') || peer.hasAnswer;

    if (isCollision && !peer.isPolite) {
      return;
    }

    await pc.setRemoteDescription(new RTCSessionDescription(offerSdp));

    // Process any queued ICE candidates
    if (pendingIceCandidates.has(targetPeerId)) {
      const candidates = pendingIceCandidates.get(targetPeerId);
      for (const c of candidates) {
        try { await pc.addIceCandidate(new RTCIceCandidate(c)); } catch (e) { }
      }
      pendingIceCandidates.delete(targetPeerId);
    }

    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
    peer.hasAnswer = true;

    sendSignalingMessage(currentSignalingTopic, {
      type: 'ANSWER',
      targetPeerId: targetPeerId,
      fromPeerId: myPeerId,
      nickname: myNickname,
      sdp: answer
    });
  } catch (err) {
    console.warn(`[WebRTC] Erro ao processar oferta de ${targetPeerId}:`, err);
  }
}

async function handleAnswer(targetPeerId, answerSdp) {
  const peer = connectedPeers.get(targetPeerId);
  if (!peer) return;
  try {
    const pc = peer.pc;
    if (pc.signalingState === 'have-local-offer') {
      await pc.setRemoteDescription(new RTCSessionDescription(answerSdp));
      peer.hasAnswer = true;

      // Process any queued ICE candidates
      if (pendingIceCandidates.has(targetPeerId)) {
        const candidates = pendingIceCandidates.get(targetPeerId);
        for (const c of candidates) {
          try { await pc.addIceCandidate(new RTCIceCandidate(c)); } catch (e) { }
        }
        pendingIceCandidates.delete(targetPeerId);
      }
    }
  } catch (err) {
    console.warn(`[WebRTC] Erro ao processar resposta de ${targetPeerId}:`, err);
  }
}

async function handleIceCandidate(targetPeerId, candidateObj) {
  const peer = connectedPeers.get(targetPeerId);
  if (!peer || !peer.pc.remoteDescription || !peer.pc.remoteDescription.type) {
    if (!pendingIceCandidates.has(targetPeerId)) {
      pendingIceCandidates.set(targetPeerId, []);
    }
    pendingIceCandidates.get(targetPeerId).push(candidateObj);
    return;
  }
  try {
    await peer.pc.addIceCandidate(new RTCIceCandidate(candidateObj));
  } catch (err) { }
}

// Data Channel Handlers
function setupDataChannelListeners(targetPeerId, channel, onConnectedCallback) {
  channel.onopen = () => {
    const peer = connectedPeers.get(targetPeerId);
    if (onConnectedCallback) onConnectedCallback('p2p');

    try {
      channel.send(JSON.stringify({
        type: 'PEER_INFO',
        peerId: myPeerId,
        nickname: myNickname
      }));
    } catch (e) { }

    const peerNick = peer ? peer.nickname : 'Participante';
    appendSystemMessage(`⚡ <strong>${escapeHtml(peerNick)}</strong> conectou-se à sala.`);
  };

  channel.onclose = () => {
    const peer = connectedPeers.get(targetPeerId);
    if (peer && peer.state === 'connected') {
      peer.mode = 'relay';
      renderParticipantsList();
    } else {
      removePeer(targetPeerId);
    }
  };

  channel.onerror = () => {
    const peer = connectedPeers.get(targetPeerId);
    if (peer) {
      peer.mode = 'relay';
      renderParticipantsList();
    }
  };

  channel.onmessage = (event) => {
    // 1. Binary chunk received
    if (event.data instanceof ArrayBuffer) {
      receivedChunks.push(event.data);
      receivedBytes += event.data.byteLength;
      if (currentReceivingMeta) {
        updateTransferProgress(receivedBytes, currentReceivingMeta.size);
      }
      return;
    }

    // 2. Text/JSON message
    try {
      const msg = JSON.parse(event.data);
      onDataMessage(targetPeerId, msg);
    } catch (e) {
      console.warn('[DataChannel] Erro ao decodificar JSON:', e);
    }
  };
}

// Handle In-App Protocol Messages (Chat, File Transfer)
function onDataMessage(fromPeerId, msg) {
  const peer = connectedPeers.get(fromPeerId);

  // 1. Peer Info
  if (msg.type === 'PEER_INFO') {
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
    if (peer && msg.nickname) {
      peer.nickname = msg.nickname;
      renderParticipantsList();
      appendSystemMessage(`✏️ <strong>${escapeHtml(msg.oldNickname)}</strong> mudou o apelido para <strong>${escapeHtml(msg.nickname)}</strong>.`);
    }
  }
  // 4. Typing Indicator
  else if (msg.type === 'TYPING') {
    handlePeerTyping(fromPeerId, msg.nickname || (peer ? peer.nickname : 'Alguém'), msg.isTyping);
  }
  // 5. File Offer from remote peer
  else if (msg.type === 'FILE_OFFER') {
    pendingIncomingOffer = {
      fileId: msg.fileId,
      name: msg.name,
      size: msg.size,
      mimeType: msg.mimeType,
      sender: msg.sender,
      senderPeerId: fromPeerId
    };
    incomingModalFileName.textContent = msg.name;
    incomingModalFileSize.textContent = formatBytes(msg.size);
    incomingModalSenderName.textContent = msg.sender || 'Alguém';
    incomingModal.classList.remove('hidden');
  }
  // 6. File Offer Accepted
  else if (msg.type === 'FILE_OFFER_ACCEPTED') {
    startSendingNextQueuedFile(fromPeerId);
  }
  // 7. File Offer Rejected
  else if (msg.type === 'FILE_OFFER_REJECTED') {
    appendSystemMessage(`❌ <strong>${escapeHtml(peer ? peer.nickname : 'Participante')}</strong> recusou o recebimento do arquivo.`);
  }
  // 8. File Metadata
  else if (msg.type === 'FILE_META') {
    currentReceivingMeta = msg;
    receivedChunks = [];
    receivedBytes = 0;
    transferStartTime = Date.now();
    lastBytes = 0;
    lastTime = Date.now();
    openTransferModal(msg.name, msg.size, 'Recebendo Arquivo...');
  }
  // 9. Relay File Chunk
  else if (msg.type === 'FILE_CHUNK_RELAY') {
    handleRelayChunk(msg);
  }
  // 10. File Transfer Completed
  else if (msg.type === 'FILE_COMPLETE') {
    finishReceivedFile(msg);
  }
  // 11. Transfer Cancelled
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
    sender: currentReceivingMeta ? currentReceivingMeta.sender : 'Participante'
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
    renderParticipantsList();
  }
}

// UI Transition: Enter Connected Room
function enterRoomUI(code) {
  inRoom = true;
  pairingSection.classList.add('hidden');
  activeRoomSection.classList.remove('hidden');
  activeRoomCode.textContent = `#${code}`;
  statusDot.className = 'status-dot green pulse';
  statusPillText.textContent = 'Conectado';

  renderParticipantsList();
  switchMobilePanel('chat');
}

// UI Transition: Leave Room
function leaveRoom() {
  inRoom = false;
  clearInterval(heartbeatTimer);
  resetAllConnections();
  switchMobilePanel('chat');
  activeRoomSection.classList.add('hidden');
  pairingSection.classList.remove('hidden');
  connectStatusBanner.classList.add('hidden');
  qrContainer.classList.add('hidden');
  statusDot.className = 'status-dot green';
  statusPillText.textContent = 'Pronto';
  chatMessages.innerHTML = `
    <div class="chat-system-msg">
      <span>🔒 Sala iniciada. Mensagens e arquivos são transferidos diretamente entre os participantes.</span>
    </div>
  `;
  sessionTransfers = [];
  renderTransfersList();
}

// Render Participants List & Counter
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
  connectedPeers.forEach((peer) => {
    const isConn = peer.state === 'connected';
    const statusText = isConn ? '' : `<small style="opacity:0.65; font-size:0.75rem; margin-left:4px; color:#fb7185;">(conectando...)</small>`;

    const chip = document.createElement('div');
    chip.className = 'peer-chip';
    chip.innerHTML = `
      <span class="peer-chip-avatar" style="background:${getNicknameColor(peer.nickname)};"></span>
      <span>${escapeHtml(peer.nickname)} ${statusText}</span>
    `;
    participantsChips.appendChild(chip);
  });
}

// Chat Functionality
function sendChatMessage() {
  const text = chatInput.value.trim();
  if (!text) return;

  const timeStr = formatTime();

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

function appendChatMessage(text, direction, timeStr = formatTime(), sender = myNickname) {
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

  if (direction === 'received' && window.innerWidth <= 768 && activeMobilePanel !== 'chat' && chatUnreadDot) {
    chatUnreadDot.classList.remove('hidden');
  }
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

  appendSystemMessage(`📤 Oferta de arquivo enviada: <strong>${escapeHtml(file.name)}</strong> (${formatBytes(file.size)}).`);
}

// Send File Stream to Specific Peer
async function startSendingNextQueuedFile(targetPeerId) {
  if (pendingSendQueue.length === 0) return;
  const file = pendingSendQueue.shift();

  const targetPeer = connectedPeers.get(targetPeerId);
  if (!targetPeer) return;

  const arrayBuffer = await file.arrayBuffer();
  const sha256 = await computeHash(arrayBuffer);

  openTransferModal(file.name, file.size, 'Enviando Arquivo...');

  if (targetPeer.mode === 'p2p' && targetPeer.dc && targetPeer.dc.readyState === 'open') {
    await sendFileOverDataChannel(targetPeer.dc, file, arrayBuffer, sha256);
  } else {
    await sendFileOverRelay(targetPeerId, file, arrayBuffer, sha256);
  }

  addTransferToHistory({
    name: file.name,
    size: file.size,
    direction: 'sent',
    sha256: sha256,
    sender: myNickname
  });
}

// Streaming Transfer over WebRTC DataChannel
async function sendFileOverDataChannel(dc, file, arrayBuffer, sha256) {
  dc.send(JSON.stringify({
    type: 'FILE_META',
    name: file.name,
    size: file.size,
    mimeType: file.type || 'application/octet-stream',
    sender: myNickname,
    sha256: sha256
  }));

  const totalBytes = arrayBuffer.byteLength;
  let offset = 0;
  transferStartTime = Date.now();
  lastBytes = 0;
  lastTime = Date.now();

  return new Promise((resolve) => {
    function sendSlice() {
      while (offset < totalBytes) {
        if (dc.bufferedAmount > BUFFER_HIGH) {
          dc.onbufferedamountlow = () => {
            dc.onbufferedamountlow = null;
            sendSlice();
          };
          return;
        }

        const chunk = arrayBuffer.slice(offset, offset + CHUNK_SIZE);
        dc.send(chunk);
        offset += chunk.byteLength;
        updateTransferProgress(offset, totalBytes);
      }

      // Finish sending
      dc.send(JSON.stringify({
        type: 'FILE_COMPLETE',
        sha256: sha256
      }));

      onSendComplete(sha256);
      resolve();
    }

    sendSlice();
  });
}

// Fallback Relay File Sending
async function sendFileOverRelay(targetPeerId, file, arrayBuffer, sha256) {
  sendPeerAppMessage(targetPeerId, {
    type: 'FILE_META',
    name: file.name,
    size: file.size,
    mimeType: file.type || 'application/octet-stream',
    sender: myNickname,
    sha256: sha256
  });

  const totalBytes = arrayBuffer.byteLength;
  let offset = 0;
  const relayChunkSize = 64 * 1024; // 64 KB for MQTT payload safety
  transferStartTime = Date.now();
  lastBytes = 0;
  lastTime = Date.now();

  while (offset < totalBytes) {
    const chunk = arrayBuffer.slice(offset, offset + relayChunkSize);
    sendPeerAppMessage(targetPeerId, {
      type: 'FILE_CHUNK_RELAY',
      chunk: Array.from(new Uint8Array(chunk))
    });
    offset += chunk.byteLength;
    updateTransferProgress(offset, totalBytes);
    await new Promise(r => setTimeout(r, 20));
  }

  sendPeerAppMessage(targetPeerId, {
    type: 'FILE_COMPLETE',
    sha256: sha256
  });

  onSendComplete(sha256);
}

function onSendComplete(sha256) {
  transferHash.textContent = '✓ ' + sha256.substring(0, 16) + '... (Concluído)';
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
  if (window.innerWidth <= 768 && activeMobilePanel !== 'files' && filesUnreadDot) {
    filesUnreadDot.classList.remove('hidden');
  }
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
  transferHash.style.color = '#fda4af';
  transferStatePill.textContent = title;
  transferStatePill.style.background = 'rgba(225, 29, 72, 0.15)';
  transferStatePill.style.color = '#fda4af';
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

  if (mqttClient) {
    mqttClient.end();
    mqttClient = null;
  }
}
