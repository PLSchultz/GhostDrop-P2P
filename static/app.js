/**
 * GhostDrop P2P v2.0 - Decentralized Multi-Peer WebRTC Mesh Engine
 * Real-time Group Chat & End-to-End Encrypted File Transfer
 */

// WebRTC Configuration
const STUN_SERVERS = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
    { urls: 'stun:stun2.l.google.com:19302' },
    { urls: 'stun:stun.cloudflare.com:3478' }
  ]
};

const MQTT_BROKERS = [
  'wss://broker.emqx.io:8084/mqtt',
  'wss://broker.hivemq.com:8884/mqtt'
];

const CHUNK_SIZE = 64 * 1024; // 64 KB per chunk for WebRTC DataChannel
const BUFFER_THRESHOLD = 2 * 1024 * 1024; // 2 MB backpressure threshold

// Local Identity State
const myPeerId = 'peer_' + Math.random().toString(36).substring(2, 9) + '_' + Date.now().toString(36);
let myNickname = '';

// Application State
let currentTab = 'send';
let pairingCode = '';
let mqttClient = null;
let currentSignalingTopic = '';
let encryptionKey = null;
let activeSessionRoomCode = '';

// Connected Peers in Mesh Map: peerId -> { pc, dc, nickname, state }
const connectedPeers = new Map();

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
const typingPeers = new Map(); // peerId -> timeoutId

// DOM Elements
const pairingSection = document.getElementById('pairingSection');
const activeRoomSection = document.getElementById('activeRoomSection');
const activeRoomCode = document.getElementById('activeRoomCode');
const participantCountText = document.getElementById('participantCountText');
const participantsChips = document.getElementById('participantsChips');
const btnDisconnect = document.getElementById('btnDisconnect');
const btnRoomCopyCode = document.getElementById('btnRoomCopyCode');
const btnRoomCopyLink = document.getElementById('btnRoomCopyLink');

const tabSend = document.getElementById('tabSend');
const tabReceive = document.getElementById('tabReceive');
const tabSecurity = document.getElementById('tabSecurity');
const tabSendBtn = document.getElementById('tabSendBtn');
const tabReceiveBtn = document.getElementById('tabReceiveBtn');
const tabSecurityBtn = document.getElementById('tabSecurityBtn');

const globalNicknameInput = document.getElementById('globalNicknameInput');
const senderPairingCode = document.getElementById('senderPairingCode');
const copyCodeBtn = document.getElementById('copyCodeBtn');
const copyLinkBtn = document.getElementById('copyLinkBtn');
const showQrBtn = document.getElementById('showQrBtn');
const qrContainer = document.getElementById('qrContainer');
const senderStatusText = document.getElementById('senderStatusText');
const btnEnterCreatedRoom = document.getElementById('btnEnterCreatedRoom');

const receiveCodeInput = document.getElementById('receiveCodeInput');
const btnConnectReceiver = document.getElementById('btnConnectReceiver');
const receiverStatusBanner = document.getElementById('receiverStatusBanner');
const receiverStatusText = document.getElementById('receiverStatusText');

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
  generatePairingCode();
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
  broadcastDataChannelMessage({
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

// Generate a random 6-digit room code
function generatePairingCode() {
  const num1 = Math.floor(100 + Math.random() * 900);
  const num2 = Math.floor(100 + Math.random() * 900);
  pairingCode = `${num1}-${num2}`;
  if (senderPairingCode) {
    senderPairingCode.textContent = pairingCode;
  }
  return pairingCode;
}

function cleanCode(code) {
  return code.replace(/[^0-9]/g, '');
}

function formatCode(digits) {
  if (digits.length <= 3) return digits;
  return digits.slice(0, 3) + '-' + digits.slice(3, 6);
}

// Tab Switching
function switchTab(tab) {
  currentTab = tab;
  [tabSend, tabReceive, tabSecurity].forEach(el => el.classList.remove('active'));
  [tabSendBtn, tabReceiveBtn, tabSecurityBtn].forEach(el => el.classList.remove('active'));

  if (tab === 'send') {
    tabSend.classList.add('active');
    tabSendBtn.classList.add('active');
  } else if (tab === 'receive') {
    tabReceive.classList.add('active');
    tabReceiveBtn.classList.add('active');
  } else if (tab === 'security') {
    tabSecurity.classList.add('active');
    tabSecurityBtn.classList.add('active');
  }
}

// Check URL Query Parameters (e.g., ?receive=849210)
function checkUrlParams() {
  const params = new URLSearchParams(window.location.search);
  const code = params.get('receive');
  if (code) {
    switchTab('receive');
    receiveCodeInput.value = formatCode(cleanCode(code));
    setTimeout(() => {
      startReceiverFlow();
    }, 400);
  }
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
      salt: enc.encode('ghostdrop_p2p_multi_mesh_salt_2026'),
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
    console.error('Falha na decriptação de sinalização:', err);
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

  // Copy Code
  copyCodeBtn.addEventListener('click', () => {
    navigator.clipboard.writeText(pairingCode).then(() => {
      const orig = copyCodeBtn.innerHTML;
      copyCodeBtn.innerHTML = '<span>✓ Copiado!</span>';
      setTimeout(() => { copyCodeBtn.innerHTML = orig; }, 2000);
    });
  });

  // Copy Link
  copyLinkBtn.addEventListener('click', () => {
    const url = `${window.location.origin}/?receive=${cleanCode(pairingCode)}`;
    navigator.clipboard.writeText(url).then(() => {
      const orig = copyLinkBtn.innerHTML;
      copyLinkBtn.innerHTML = '<span>✓ Link Copiado!</span>';
      setTimeout(() => { copyLinkBtn.innerHTML = orig; }, 2000);
    });
  });

  // Show QR code
  showQrBtn.addEventListener('click', () => {
    const isHidden = qrContainer.classList.toggle('hidden');
    if (!isHidden) {
      const qrEl = document.getElementById('qrcode');
      qrEl.innerHTML = '';
      const url = `${window.location.origin}/?receive=${cleanCode(pairingCode)}`;
      new QRCode(qrEl, {
        text: url,
        width: 180,
        height: 180,
        colorDark: "#000000",
        colorLight: "#ffffff",
        correctLevel: QRCode.CorrectLevel.M
      });
    }
  });

  // Open Created Room Button
  btnEnterCreatedRoom.addEventListener('click', () => {
    startHostFlow();
  });

  // Receiver Input Formatter
  receiveCodeInput.addEventListener('input', (e) => {
    const raw = cleanCode(e.target.value);
    e.target.value = formatCode(raw);
  });

  receiveCodeInput.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
      startReceiverFlow();
    }
  });

  btnConnectReceiver.addEventListener('click', startReceiverFlow);

  // Room Actions (In Room)
  btnRoomCopyCode.addEventListener('click', () => {
    navigator.clipboard.writeText(activeSessionRoomCode).then(() => {
      const orig = btnRoomCopyCode.innerHTML;
      btnRoomCopyCode.innerHTML = '<span>✓ Copiado!</span>';
      setTimeout(() => { btnRoomCopyCode.innerHTML = orig; }, 2000);
    });
  });

  btnRoomCopyLink.addEventListener('click', () => {
    const url = `${window.location.origin}/?receive=${cleanCode(activeSessionRoomCode)}`;
    navigator.clipboard.writeText(url).then(() => {
      const orig = btnRoomCopyLink.innerHTML;
      btnRoomCopyLink.innerHTML = '<span>✓ Link Copiado!</span>';
      setTimeout(() => { btnRoomCopyLink.innerHTML = orig; }, 2000);
    });
  });

  // Chat Form
  chatForm.addEventListener('submit', (e) => {
    e.preventDefault();
    sendChatMessage();
  });

  let typingTimer = null;
  chatInput.addEventListener('input', () => {
    broadcastDataChannelMessage({
      type: 'TYPING',
      peerId: myPeerId,
      nickname: myNickname,
      isTyping: true
    });
    clearTimeout(typingTimer);
    typingTimer = setTimeout(() => {
      broadcastDataChannelMessage({
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
      const senderPeer = connectedPeers.get(pendingIncomingOffer.senderPeerId);
      if (senderPeer && senderPeer.dc && senderPeer.dc.readyState === 'open') {
        senderPeer.dc.send(JSON.stringify({
          type: 'FILE_OFFER_ACCEPTED',
          fileId: pendingIncomingOffer.fileId,
          targetPeerId: pendingIncomingOffer.senderPeerId,
          fromPeerId: myPeerId
        }));
      }
    }
  });

  btnModalReject.addEventListener('click', () => {
    incomingModal.classList.add('hidden');
    if (pendingIncomingOffer) {
      const senderPeer = connectedPeers.get(pendingIncomingOffer.senderPeerId);
      if (senderPeer && senderPeer.dc && senderPeer.dc.readyState === 'open') {
        senderPeer.dc.send(JSON.stringify({
          type: 'FILE_OFFER_REJECTED',
          fileId: pendingIncomingOffer.fileId,
          targetPeerId: pendingIncomingOffer.senderPeerId,
          fromPeerId: myPeerId
        }));
      }
    }
    pendingIncomingOffer = null;
  });

  // Cancel Transfer
  btnCancelTransfer.addEventListener('click', () => {
    if (confirm('Deseja cancelar a transferência em andamento?')) {
      broadcastDataChannelMessage({ type: 'TRANSFER_CANCELLED' });
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
// MULTI-PEER WEBRTC MESH & SIGNALING
// =========================================================================

// Start Host Room
async function startHostFlow() {
  const rawCode = cleanCode(pairingCode);
  activeSessionRoomCode = formatCode(rawCode);
  await joinMeshRoom(rawCode);
  enterRoom(activeSessionRoomCode);
}

// Start Receiver Flow
async function startReceiverFlow() {
  const codeVal = cleanCode(receiveCodeInput.value);
  if (codeVal.length < 6) {
    alert('Digite um código de 6 dígitos válido.');
    return;
  }

  receiverStatusBanner.classList.remove('hidden');
  receiverStatusText.textContent = 'Conectando à sala...';
  activeSessionRoomCode = formatCode(codeVal);

  await joinMeshRoom(codeVal);
  enterRoom(activeSessionRoomCode);
}

// Join Mesh Room over encrypted MQTT
async function joinMeshRoom(rawCode) {
  encryptionKey = await deriveKey(rawCode);
  currentSignalingTopic = `ghostdrop/p2p/${rawCode}`;

  return new Promise((resolve) => {
    connectSignaling(currentSignalingTopic, async (message) => {
      const decrypted = await decryptPayload(encryptionKey, message);
      if (!decrypted) return;
      handleSignalingMessage(decrypted);
    }, () => {
      // Announce arrival to all peers in room
      sendSignalingMessage(currentSignalingTopic, {
        type: 'PEER_JOINED',
        fromPeerId: myPeerId,
        nickname: myNickname
      });
      resolve();
    });
  });
}

// Handle Incoming Signaling Messages in Mesh
async function handleSignalingMessage(msg) {
  // Ignore own announcements
  if (msg.fromPeerId === myPeerId) return;

  const senderId = msg.fromPeerId;
  const senderNick = msg.nickname || 'Ghost Peer';

  // 1. New peer joined the room: As an existing peer, we initiate the WebRTC connection to the newcomer
  if (msg.type === 'PEER_JOINED') {
    if (!connectedPeers.has(senderId)) {
      initiatePeerConnection(senderId, senderNick);
    }
  }

  // 2. Received WebRTC SDP Offer (only process if targeted to me)
  else if (msg.type === 'SDP_OFFER' && msg.targetPeerId === myPeerId) {
    handleIncomingOffer(senderId, senderNick, msg.sdp);
  }

  // 3. Received WebRTC SDP Answer (only process if targeted to me)
  else if (msg.type === 'SDP_ANSWER' && msg.targetPeerId === myPeerId) {
    const peer = connectedPeers.get(senderId);
    if (peer && peer.pc) {
      await peer.pc.setRemoteDescription(new RTCSessionDescription(msg.sdp));
    }
  }

  // 4. Received ICE Candidate (only process if targeted to me)
  else if (msg.type === 'ICE_CANDIDATE' && msg.targetPeerId === myPeerId) {
    const peer = connectedPeers.get(senderId);
    if (peer && peer.pc && msg.candidate) {
      try {
        await peer.pc.addIceCandidate(new RTCIceCandidate(msg.candidate));
      } catch (err) {
        console.warn('Erro ao adicionar ICE candidate:', err);
      }
    }
  }
}

// Peer A (Initiator): Create PeerConnection and send Offer to Peer B
async function initiatePeerConnection(targetPeerId, targetNickname) {
  const pc = new RTCPeerConnection(STUN_SERVERS);
  const dc = pc.createDataChannel('ghostdropChannel', { ordered: true });
  dc.binaryType = 'arraybuffer';

  const peerObj = {
    pc: pc,
    dc: dc,
    nickname: targetNickname,
    isInitiator: true
  };
  connectedPeers.set(targetPeerId, peerObj);

  setupDataChannelListeners(targetPeerId, dc);

  pc.onicecandidate = (event) => {
    if (event.candidate) {
      sendSignalingMessage(currentSignalingTopic, {
        type: 'ICE_CANDIDATE',
        targetPeerId: targetPeerId,
        fromPeerId: myPeerId,
        candidate: event.candidate
      });
    }
  };

  pc.onconnectionstatechange = () => {
    if (pc.connectionState === 'disconnected' || pc.connectionState === 'failed' || pc.connectionState === 'closed') {
      removePeer(targetPeerId);
    }
  };

  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);

  sendSignalingMessage(currentSignalingTopic, {
    type: 'SDP_OFFER',
    targetPeerId: targetPeerId,
    fromPeerId: myPeerId,
    nickname: myNickname,
    sdp: offer
  });
}

// Peer B (Receiver): Handle Offer from Peer A and send Answer
async function handleIncomingOffer(fromPeerId, fromNickname, offerSdp) {
  const pc = new RTCPeerConnection(STUN_SERVERS);

  const peerObj = {
    pc: pc,
    dc: null,
    nickname: fromNickname,
    isInitiator: false
  };
  connectedPeers.set(fromPeerId, peerObj);

  pc.ondatachannel = (event) => {
    const dc = event.channel;
    dc.binaryType = 'arraybuffer';
    peerObj.dc = dc;
    setupDataChannelListeners(fromPeerId, dc);
  };

  pc.onicecandidate = (event) => {
    if (event.candidate) {
      sendSignalingMessage(currentSignalingTopic, {
        type: 'ICE_CANDIDATE',
        targetPeerId: fromPeerId,
        fromPeerId: myPeerId,
        candidate: event.candidate
      });
    }
  };

  pc.onconnectionstatechange = () => {
    if (pc.connectionState === 'disconnected' || pc.connectionState === 'failed' || pc.connectionState === 'closed') {
      removePeer(fromPeerId);
    }
  };

  await pc.setRemoteDescription(new RTCSessionDescription(offerSdp));
  const answer = await pc.createAnswer();
  await pc.setLocalDescription(answer);

  sendSignalingMessage(currentSignalingTopic, {
    type: 'SDP_ANSWER',
    targetPeerId: fromPeerId,
    fromPeerId: myPeerId,
    nickname: myNickname,
    sdp: answer
  });
}

// Setup DataChannel Listeners for a Specific Peer
function setupDataChannelListeners(peerId, channel) {
  channel.onopen = () => {
    // Send my info to this peer
    channel.send(JSON.stringify({
      type: 'PEER_INFO',
      peerId: myPeerId,
      nickname: myNickname
    }));

    const peer = connectedPeers.get(peerId);
    const peerNick = peer ? peer.nickname : 'Participante';
    appendSystemMessage(`👤 <strong>${escapeHtml(peerNick)}</strong> conectou-se à sala.`);
    renderParticipantsList();
  };

  channel.onclose = () => {
    removePeer(peerId);
  };

  channel.onerror = (err) => {
    console.error(`Erro no canal P2P com ${peerId}:`, err);
  };

  channel.onmessage = (event) => {
    handleDataChannelMessage(peerId, event);
  };
}

// Handle DataChannel Messages (Chat, Nickname, Files)
async function handleDataChannelMessage(fromPeerId, event) {
  if (typeof event.data === 'string') {
    let msg;
    try {
      msg = JSON.parse(event.data);
    } catch (e) {
      return;
    }

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
      const blob = new Blob(receivedChunks, { type: currentReceivingMeta.mimeType || 'application/octet-stream' });
      const arrayBuffer = await blob.arrayBuffer();
      const calculatedHash = await computeHash(arrayBuffer);

      if (msg.sha256 && calculatedHash.toLowerCase() === msg.sha256.toLowerCase()) {
        transferHash.textContent = '✓ ' + calculatedHash.substring(0, 16) + '... (Válido)';
        transferHash.style.color = '#10b981';
      } else {
        transferHash.textContent = calculatedHash.substring(0, 16) + '...';
      }

      // Download file to disk
      downloadBlob(blob, currentReceivingMeta.name);

      // Add to session history
      addTransferToHistory({
        name: currentReceivingMeta.name,
        size: currentReceivingMeta.size,
        direction: 'received',
        sha256: calculatedHash,
        sender: currentReceivingMeta.sender || 'Peer'
      });

      progressBarFill.style.width = '100%';
      transferPercent.textContent = '100%';
      transferStatePill.textContent = 'Concluído!';
      transferStatePill.style.background = 'rgba(16, 185, 129, 0.2)';
      transferStatePill.style.color = '#6ee7b7';
      btnCloseTransferModal.classList.remove('hidden');
      btnCancelTransfer.classList.add('hidden');
    }
    // 10. Cancel Transfer
    else if (msg.type === 'TRANSFER_CANCELLED') {
      alert('A transferência foi cancelada.');
      transferOverlay.classList.add('hidden');
    }
  } else if (event.data instanceof ArrayBuffer) {
    // Binary file chunk received
    receivedChunks.push(event.data);
    receivedBytes += event.data.byteLength;
    if (currentReceivingMeta) {
      updateTransferProgress(receivedBytes, currentReceivingMeta.size);
    }
  }
}

// Remove Peer & Clean Up Connection
function removePeer(peerId) {
  const peer = connectedPeers.get(peerId);
  if (peer) {
    appendSystemMessage(`🚪 <strong>${escapeHtml(peer.nickname)}</strong> desconectou-se da sala.`);
    try {
      if (peer.dc) peer.dc.close();
      if (peer.pc) peer.pc.close();
    } catch (e) {}
    connectedPeers.delete(peerId);
    renderParticipantsList();
  }
}

// Broadcast Message Across All Active DataChannels
function broadcastDataChannelMessage(msgObj) {
  const jsonStr = JSON.stringify(msgObj);
  connectedPeers.forEach(peer => {
    if (peer.dc && peer.dc.readyState === 'open') {
      peer.dc.send(jsonStr);
    }
  });
}

// Ephemeral MQTT Connection
function connectSignaling(topic, onMessageCallback, onConnectCallback) {
  if (mqttClient) {
    mqttClient.end();
  }

  const brokerUrl = MQTT_BROKERS[0];
  mqttClient = mqtt.connect(brokerUrl, {
    clientId: 'ghostdrop_' + Math.random().toString(16).substring(2, 10),
    clean: true,
    connectTimeout: 5000,
    reconnectPeriod: 2000
  });

  mqttClient.on('connect', () => {
    mqttClient.subscribe(topic, { qos: 0 }, (err) => {
      if (!err && onConnectCallback) {
        onConnectCallback();
      }
    });
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
function enterRoom(code) {
  pairingSection.classList.add('hidden');
  activeRoomSection.classList.remove('hidden');
  activeRoomCode.textContent = `#${code}`;
  statusDot.className = 'status-dot green pulse';
  statusPillText.textContent = 'Sala Multi-Peer Ativa';

  renderParticipantsList();
}

// UI Transition: Leave Room
function leaveRoom() {
  resetAllConnections();
  activeRoomSection.classList.add('hidden');
  pairingSection.classList.remove('hidden');
  statusDot.className = 'status-dot green';
  statusPillText.textContent = 'E2EE DTLS Pronto';
  chatMessages.innerHTML = `
    <div class="chat-system-msg">
      <span>🔒 Conexão segura estabelecida. Mensagens e arquivos são transmitidos diretamente entre os nós P2P.</span>
    </div>
  `;
  sessionTransfers = [];
  renderTransfersList();
  generatePairingCode();
}

// Render Participants List & Counter
function renderParticipantsList() {
  const count = connectedPeers.size + 1; // +1 for self
  participantCountText.textContent = count === 1 ? '1 participante (você)' : `${count} participantes`;

  participantsChips.innerHTML = '';

  // 1. Add Myself
  const myChip = document.createElement('div');
  myChip.className = 'peer-chip is-me';
  myChip.innerHTML = `
    <span class="peer-chip-avatar" style="background:${getNicknameColor(myNickname)};"></span>
    <span>${escapeHtml(myNickname)} (Você)</span>
  `;
  participantsChips.appendChild(myChip);

  // 2. Add Connected Peers
  connectedPeers.forEach(peer => {
    const chip = document.createElement('div');
    chip.className = 'peer-chip';
    chip.innerHTML = `
      <span class="peer-chip-avatar" style="background:${getNicknameColor(peer.nickname)};"></span>
      <span>${escapeHtml(peer.nickname)}</span>
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

  // Send over all open data channels
  broadcastDataChannelMessage({
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
      clearTimeout(typingPeers.get(peerId));
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
  if (connectedPeers.size === 0) {
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

  broadcastDataChannelMessage({
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
  if (!targetPeer || !targetPeer.dc || targetPeer.dc.readyState !== 'open') {
    return;
  }

  openTransferModal(file.name, file.size, `Enviando para ${targetPeer.nickname}...`);

  const fileBuffer = await file.arrayBuffer();
  const sha256 = await computeHash(fileBuffer);

  targetPeer.dc.send(JSON.stringify({
    type: 'FILE_START',
    name: file.name,
    size: file.size,
    mimeType: file.type || 'application/octet-stream',
    sha256: sha256,
    sender: myNickname
  }));

  transferStartTime = Date.now();
  lastBytes = 0;
  lastTime = Date.now();

  let offset = 0;
  while (offset < file.size) {
    // Flow control backpressure
    if (targetPeer.dc.bufferedAmount > BUFFER_THRESHOLD) {
      await new Promise(resolve => {
        targetPeer.dc.onbufferedamountlow = () => {
          targetPeer.dc.onbufferedamountlow = null;
          resolve();
        };
      });
    }

    const chunk = fileBuffer.slice(offset, offset + CHUNK_SIZE);
    targetPeer.dc.send(chunk);
    offset += chunk.byteLength;

    updateTransferProgress(offset, file.size);
  }

  targetPeer.dc.send(JSON.stringify({
    type: 'FILE_END',
    sha256: sha256
  }));

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

  if (mqttClient) {
    mqttClient.end();
    mqttClient = null;
  }
}
