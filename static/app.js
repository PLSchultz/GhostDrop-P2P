/**
 * GhostDrop P2P v1.2 - Interactive Real-Time Chat & Bidirectional File Transfer Engine
 */

// Configuration
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

const CHUNK_SIZE = 64 * 1024; // 64 KB per chunk for optimal WebRTC throughput
const BUFFER_THRESHOLD = 2 * 1024 * 1024; // 2 MB backpressure threshold

// Application State
let currentTab = 'send';
let pairingCode = '';
let isHost = false;
let mqttClient = null;
let peerConnection = null;
let dataChannel = null;
let encryptionKey = null;
let activeSessionRoomCode = '';

// Session transfers history
let sessionTransfers = [];
let pendingSendQueue = [];
let pendingIncomingOffer = null;

// Receiving state
let currentReceivingMeta = null;
let receivedChunks = [];
let receivedBytes = 0;
let transferStartTime = 0;
let lastBytes = 0;
let lastTime = 0;

// Typing indicator state
let typingTimeout = null;

// DOM Elements
const pairingSection = document.getElementById('pairingSection');
const activeRoomSection = document.getElementById('activeRoomSection');
const activeRoomCode = document.getElementById('activeRoomCode');
const btnDisconnect = document.getElementById('btnDisconnect');

const tabSend = document.getElementById('tabSend');
const tabReceive = document.getElementById('tabReceive');
const tabSecurity = document.getElementById('tabSecurity');
const tabSendBtn = document.getElementById('tabSendBtn');
const tabReceiveBtn = document.getElementById('tabReceiveBtn');
const tabSecurityBtn = document.getElementById('tabSecurityBtn');

const senderPairingCode = document.getElementById('senderPairingCode');
const copyCodeBtn = document.getElementById('copyCodeBtn');
const showQrBtn = document.getElementById('showQrBtn');
const qrContainer = document.getElementById('qrContainer');
const senderStatusText = document.getElementById('senderStatusText');

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
const incomingModalFileCount = document.getElementById('incomingModalFileCount');
const btnModalAccept = document.getElementById('btnModalAccept');
const btnModalReject = document.getElementById('btnModalReject');

// Initialize
document.addEventListener('DOMContentLoaded', () => {
  setupEventListeners();
  checkUrlParams();
  generatePairingCode();
  startHostSignaling();
});

// Format byte size helper
function formatBytes(bytes, decimals = 2) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

// Format current time HH:MM
function formatTime() {
  const d = new Date();
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

// Generate a random 6-digit code (e.g. "749-312")
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

// Check URL query parameters (e.g., ?receive=849210)
function checkUrlParams() {
  const params = new URLSearchParams(window.location.search);
  const code = params.get('receive');
  if (code) {
    switchTab('receive');
    receiveCodeInput.value = formatCode(cleanCode(code));
    setTimeout(() => {
      startReceiverFlow();
    }, 500);
  }
}

// Cryptography: Derive AES-256 Key from Pairing Code
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
      salt: enc.encode('ghostdrop_p2p_zero_trace_salt_2026'),
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
    console.error('Falha na decriptação:', err);
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
  // Copy code
  copyCodeBtn.addEventListener('click', () => {
    navigator.clipboard.writeText(pairingCode).then(() => {
      const orig = copyCodeBtn.innerHTML;
      copyCodeBtn.innerHTML = '<span>✓ Copiado!</span>';
      setTimeout(() => { copyCodeBtn.innerHTML = orig; }, 2000);
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

  // Chat Form
  chatForm.addEventListener('submit', (e) => {
    e.preventDefault();
    sendChatMessage();
  });

  chatInput.addEventListener('input', () => {
    if (dataChannel && dataChannel.readyState === 'open') {
      dataChannel.send(JSON.stringify({ type: 'TYPING', isTyping: true }));
      clearTimeout(typingTimeout);
      typingTimeout = setTimeout(() => {
        if (dataChannel && dataChannel.readyState === 'open') {
          dataChannel.send(JSON.stringify({ type: 'TYPING', isTyping: false }));
        }
      }, 1500);
    }
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
    if (dataChannel && dataChannel.readyState === 'open' && pendingIncomingOffer) {
      dataChannel.send(JSON.stringify({
        type: 'FILE_OFFER_ACCEPTED',
        fileId: pendingIncomingOffer.fileId
      }));
    }
  });

  btnModalReject.addEventListener('click', () => {
    incomingModal.classList.add('hidden');
    if (dataChannel && dataChannel.readyState === 'open' && pendingIncomingOffer) {
      dataChannel.send(JSON.stringify({
        type: 'FILE_OFFER_REJECTED',
        fileId: pendingIncomingOffer.fileId
      }));
    }
    pendingIncomingOffer = null;
  });

  // Cancel Transfer
  btnCancelTransfer.addEventListener('click', () => {
    if (confirm('Deseja realmente cancelar a transferência em andamento?')) {
      if (dataChannel && dataChannel.readyState === 'open') {
        dataChannel.send(JSON.stringify({ type: 'TRANSFER_CANCELLED' }));
      }
      transferOverlay.classList.add('hidden');
    }
  });

  btnCloseTransferModal.addEventListener('click', () => {
    transferOverlay.classList.add('hidden');
  });

  // Disconnect button
  btnDisconnect.addEventListener('click', () => {
    if (confirm('Deseja desconectar do parceiro e retornar à tela inicial?')) {
      resetConnection();
      leaveRoom();
    }
  });

  // Panic button
  panicButton.addEventListener('click', () => {
    if (confirm('Isso irá encerrar o aplicativo, limpar todos os dados em memória e fechar a conexão. Continuar?')) {
      resetConnection();
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

// SENDER / HOST SIGNALING
async function startHostSignaling() {
  isHost = true;
  encryptionKey = await deriveKey(pairingCode);
  const rawCode = cleanCode(pairingCode);
  const topic = `ghostdrop/p2p/${rawCode}`;

  connectSignaling(topic, async (message) => {
    const decrypted = await decryptPayload(encryptionKey, message);
    if (!decrypted) return;

    if (decrypted.type === 'PEER_JOINED') {
      senderStatusText.textContent = 'Parceiro detectado! Estabelecendo túnel WebRTC...';
      createPeerOffer(topic);
    } else if (decrypted.type === 'SDP_ANSWER') {
      senderStatusText.textContent = 'Resposta SDP recebida. Conectando P2P...';
      await peerConnection.setRemoteDescription(new RTCSessionDescription(decrypted.sdp));
    } else if (decrypted.type === 'ICE_CANDIDATE') {
      if (decrypted.candidate && peerConnection) {
        await peerConnection.addIceCandidate(new RTCIceCandidate(decrypted.candidate));
      }
    }
  });
}

// RECEIVER SIGNALING
async function startReceiverFlow() {
  const codeVal = cleanCode(receiveCodeInput.value);
  if (codeVal.length < 6) {
    alert('Digite um código de 6 dígitos válido.');
    return;
  }

  isHost = false;
  receiverStatusBanner.classList.remove('hidden');
  receiverStatusText.textContent = 'Conectando ao canal de sinalização...';
  encryptionKey = await deriveKey(codeVal);

  const topic = `ghostdrop/p2p/${codeVal}`;
  activeSessionRoomCode = formatCode(codeVal);

  connectSignaling(topic, async (message) => {
    const decrypted = await decryptPayload(encryptionKey, message);
    if (!decrypted) return;

    if (decrypted.type === 'SDP_OFFER') {
      receiverStatusText.textContent = 'Oferta de conexão recebida! Respondendo...';
      createPeerAnswer(topic, decrypted.sdp);
    } else if (decrypted.type === 'ICE_CANDIDATE') {
      if (decrypted.candidate && peerConnection) {
        await peerConnection.addIceCandidate(new RTCIceCandidate(decrypted.candidate));
      }
    }
  }, () => {
    receiverStatusText.textContent = 'Aguardando confirmação do parceiro...';
    sendSignalingMessage(topic, { type: 'PEER_JOINED' });
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

// Send Encrypted Signaling Message
async function sendSignalingMessage(topic, data) {
  if (!mqttClient || !encryptionKey) return;
  const encrypted = await encryptPayload(encryptionKey, data);
  mqttClient.publish(topic, JSON.stringify(encrypted), { qos: 0, retain: false });
}

// WebRTC Offer
async function createPeerOffer(topic) {
  resetPeerConnection();
  peerConnection = new RTCPeerConnection(STUN_SERVERS);

  dataChannel = peerConnection.createDataChannel('ghostdropChannel', {
    ordered: true
  });
  dataChannel.binaryType = 'arraybuffer';
  setupDataChannel(dataChannel);

  peerConnection.onicecandidate = (event) => {
    if (event.candidate) {
      sendSignalingMessage(topic, {
        type: 'ICE_CANDIDATE',
        candidate: event.candidate
      });
    }
  };

  const offer = await peerConnection.createOffer();
  await peerConnection.setLocalDescription(offer);

  sendSignalingMessage(topic, {
    type: 'SDP_OFFER',
    sdp: offer
  });
}

// WebRTC Answer
async function createPeerAnswer(topic, offerSdp) {
  resetPeerConnection();
  peerConnection = new RTCPeerConnection(STUN_SERVERS);

  peerConnection.ondatachannel = (event) => {
    dataChannel = event.channel;
    dataChannel.binaryType = 'arraybuffer';
    setupDataChannel(dataChannel);
  };

  peerConnection.onicecandidate = (event) => {
    if (event.candidate) {
      sendSignalingMessage(topic, {
        type: 'ICE_CANDIDATE',
        candidate: event.candidate
      });
    }
  };

  await peerConnection.setRemoteDescription(new RTCSessionDescription(offerSdp));
  const answer = await peerConnection.createAnswer();
  await peerConnection.setLocalDescription(answer);

  sendSignalingMessage(topic, {
    type: 'SDP_ANSWER',
    sdp: answer
  });
}

// Setup DataChannel Listeners
function setupDataChannel(channel) {
  channel.onopen = () => {
    activeSessionRoomCode = isHost ? pairingCode : (activeSessionRoomCode || receiveCodeInput.value);
    enterRoom(activeSessionRoomCode);
  };

  channel.onclose = () => {
    appendSystemMessage('⚠️ Conexão P2P com o peer foi encerrada.');
    statusDot.className = 'status-dot yellow';
    statusPillText.textContent = 'Desconectado';
  };

  channel.onerror = (err) => {
    console.error('Erro no canal P2P:', err);
  };

  channel.onmessage = handleChannelMessage;
}

// UI Transition: Enter Connected Room
function enterRoom(code) {
  pairingSection.classList.add('hidden');
  activeRoomSection.classList.remove('hidden');
  activeRoomCode.textContent = `#${code}`;
  statusDot.className = 'status-dot green';
  statusPillText.textContent = 'Conectado P2P';

  // Disconnect MQTT signaling since WebRTC is now directly connected
  if (mqttClient) {
    mqttClient.end();
    mqttClient = null;
  }
}

// UI Transition: Leave Room
function leaveRoom() {
  activeRoomSection.classList.add('hidden');
  pairingSection.classList.remove('hidden');
  statusDot.className = 'status-dot green';
  statusPillText.textContent = 'E2EE DTLS Pronto';
  chatMessages.innerHTML = `
    <div class="chat-system-msg">
      <span>🔒 Conexão segura estabelecida. Mensagens e arquivos são transmitidos diretamente P2P.</span>
    </div>
  `;
  sessionTransfers = [];
  renderTransfersList();
  generatePairingCode();
  startHostSignaling();
}

// Handle Incoming DataChannel Messages (Chat & File Control)
async function handleChannelMessage(event) {
  if (typeof event.data === 'string') {
    const msg = JSON.parse(event.data);

    // 1. Chat Message
    if (msg.type === 'CHAT') {
      appendChatMessage(msg.text, 'received', msg.time);
    }
    // 2. Typing Indicator
    else if (msg.type === 'TYPING') {
      if (msg.isTyping) {
        typingIndicator.classList.remove('hidden');
      } else {
        typingIndicator.classList.add('hidden');
      }
    }
    // 3. File Offer (Bidirectional: Received from peer)
    else if (msg.type === 'FILE_OFFER') {
      pendingIncomingOffer = msg;
      incomingModalFileName.textContent = msg.name;
      incomingModalFileSize.textContent = formatBytes(msg.size);
      incomingModalFileCount.textContent = '1 arquivo';
      incomingModal.classList.remove('hidden');
    }
    // 4. File Offer Accepted by Peer
    else if (msg.type === 'FILE_OFFER_ACCEPTED') {
      startSendingNextQueuedFile();
    }
    // 5. File Offer Rejected by Peer
    else if (msg.type === 'FILE_OFFER_REJECTED') {
      alert('O parceiro recusou o arquivo enviado.');
      pendingSendQueue.shift();
    }
    // 6. File Start Stream
    else if (msg.type === 'FILE_START') {
      currentReceivingMeta = msg;
      receivedChunks = [];
      receivedBytes = 0;
      transferStartTime = Date.now();
      lastBytes = 0;
      lastTime = Date.now();

      openTransferModal(msg.name, msg.size, 'Recebendo...');
    }
    // 7. File End Stream (Assembly & Verification)
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
        sha256: calculatedHash
      });

      progressBarFill.style.width = '100%';
      transferPercent.textContent = '100%';
      transferStatePill.textContent = 'Concluído!';
      transferStatePill.style.background = 'rgba(16, 185, 129, 0.2)';
      transferStatePill.style.color = '#6ee7b7';
      btnCloseTransferModal.classList.remove('hidden');
      btnCancelTransfer.classList.add('hidden');
    }
    // 8. Cancel Transfer
    else if (msg.type === 'TRANSFER_CANCELLED') {
      alert('A transferência foi cancelada pelo parceiro.');
      transferOverlay.classList.add('hidden');
    }
  } else if (event.data instanceof ArrayBuffer) {
    // Binary file chunk
    receivedChunks.push(event.data);
    receivedBytes += event.data.byteLength;
    if (currentReceivingMeta) {
      updateTransferProgress(receivedBytes, currentReceivingMeta.size);
    }
  }
}

// Send Chat Message
function sendChatMessage() {
  const text = chatInput.value.trim();
  if (!text) return;

  if (dataChannel && dataChannel.readyState === 'open') {
    const timeStr = formatTime();
    dataChannel.send(JSON.stringify({
      type: 'CHAT',
      text: text,
      time: timeStr
    }));
    appendChatMessage(text, 'sent', timeStr);
    chatInput.value = '';
    chatInput.focus();
  }
}

function appendChatMessage(text, direction, timeStr = formatTime()) {
  const bubble = document.createElement('div');
  bubble.className = `chat-bubble ${direction}`;
  
  const textEl = document.createElement('div');
  textEl.textContent = text;
  
  const timeEl = document.createElement('div');
  timeEl.className = 'chat-time';
  timeEl.textContent = timeStr;

  bubble.appendChild(textEl);
  bubble.appendChild(timeEl);
  chatMessages.appendChild(bubble);
  chatMessages.scrollTop = chatMessages.scrollHeight;
}

function appendSystemMessage(text) {
  const msg = document.createElement('div');
  msg.className = 'chat-system-msg';
  msg.innerHTML = `<span>${text}</span>`;
  chatMessages.appendChild(msg);
  chatMessages.scrollTop = chatMessages.scrollHeight;
}

// Queue Files to Send (Bidirectional)
function queueFilesToSend(files) {
  if (!dataChannel || dataChannel.readyState !== 'open') {
    alert('Conexão P2P não está ativa.');
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

  dataChannel.send(JSON.stringify({
    type: 'FILE_OFFER',
    fileId: 'file_' + Date.now(),
    name: file.name,
    size: file.size,
    mimeType: file.type || 'application/octet-stream'
  }));

  appendSystemMessage(`📤 Oferta de arquivo enviada: <strong>${file.name}</strong> (${formatBytes(file.size)}). Aguardando parceiro aceitar...`);
}

// Send File Stream
async function startSendingNextQueuedFile() {
  if (pendingSendQueue.length === 0) return;
  const file = pendingSendQueue.shift();

  openTransferModal(file.name, file.size, 'Enviando...');

  const fileBuffer = await file.arrayBuffer();
  const sha256 = await computeHash(fileBuffer);

  dataChannel.send(JSON.stringify({
    type: 'FILE_START',
    name: file.name,
    size: file.size,
    mimeType: file.type || 'application/octet-stream',
    sha256: sha256
  }));

  transferStartTime = Date.now();
  lastBytes = 0;
  lastTime = Date.now();

  let offset = 0;
  while (offset < file.size) {
    // Flow control backpressure
    if (dataChannel.bufferedAmount > BUFFER_THRESHOLD) {
      await new Promise(resolve => {
        dataChannel.onbufferedamountlow = () => {
          dataChannel.onbufferedamountlow = null;
          resolve();
        };
      });
    }

    const chunk = fileBuffer.slice(offset, offset + CHUNK_SIZE);
    dataChannel.send(chunk);
    offset += chunk.byteLength;

    updateTransferProgress(offset, file.size);
  }

  dataChannel.send(JSON.stringify({
    type: 'FILE_END',
    sha256: sha256
  }));

  addTransferToHistory({
    name: file.name,
    size: file.size,
    direction: 'sent',
    sha256: sha256
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

  // If more files queued, offer next
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
          <div class="transfer-item-name">${item.name}</div>
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

// Download file blob directly to user's downloads folder
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

// Reset connections
function resetConnection() {
  if (dataChannel) {
    dataChannel.close();
    dataChannel = null;
  }
  if (peerConnection) {
    peerConnection.close();
    peerConnection = null;
  }
  if (mqttClient) {
    mqttClient.end();
    mqttClient = null;
  }
}

function resetPeerConnection() {
  if (peerConnection) {
    peerConnection.close();
    peerConnection = null;
  }
}
