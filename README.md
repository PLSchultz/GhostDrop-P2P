# 👻 GhostDrop P2P — Transferência Segura Sem Rastros

**GhostDrop P2P** é um aplicativo de transferência de arquivos Ponto-a-Ponto (P2P) ultra-rápido, seguro e privado, projetado para funcionar em qualquer computador com **Windows** como um único executável (`.exe`), **sem necessidade de instalar nada nem configurar portas de roteador**.

---

## 🌟 Principais Recursos

1. **Compartilhamento Direto P2P (WebRTC DataChannel)**:
   - Os arquivos são transmitidos diretamente da placa de rede do remetente para a do destinatário via túnel SCTP/DTLS.
   - Os arquivos **nunca** passam nem são salvos em nenhum servidor ou nuvem.
2. **Funciona em Redes Diferentes (Cross-Network / Internet)**:
   - Equipado com travessia automática de NAT por meio de servidores STUN públicos (Google e Cloudflare).
   - Funciona entre computadores em Wi-Fi diferentes, 4G/5G ou locais remotos sem redirecionamento de portas (port forwarding).
3. **Pareamento Ultra-Fácil**:
   - Gera um código numérico amigável de 6 dígitos (Ex: `749-312`).
   - Suporte a **QR Code** para leitura por dispositivos com câmera.
   - Suporte a links diretos para conexão em 1 clique.
4. **Zero Rastros (Stealth & Privacidade)**:
   - **Sinalização Efêmera**: Utiliza brokers MQTT em memória (RAM-only). As mensagens de pareamento têm retenção desativada (`retain: false`) e são destruídas no instante em que são entregues.
   - **Criptografia E2EE de Sinal**: Os metadados e ofertas SDP são criptografados com **AES-256-GCM** com chave derivada por **PBKDF2** diretamente no navegador a partir do código de pareamento.
   - **Botão de Pânico / Limpeza**: Encerra o processo local e remove os buffers da memória RAM com 1 clique.
5. **Portabilidade Máxima (.EXE Único)**:
   - Compilado em um único binário executável (`GhostDrop-P2P.exe`).
   - Sem dependência de Python instalado, sem dependência de DLLs externas, sem instalador.
   - Basta dar duplo-clique para rodar.

---

## 🚀 Como Executar

### Opção 1: Executável Pronto (`.exe`)
Basta entrar na pasta `dist/` e dar dois cliques no arquivo:
```
dist\GhostDrop-P2P.exe
```
O aplicativo abrirá automaticamente a interface moderna no seu navegador padrão.

### Opção 2: Modo Linha de Comando (CLI / Opções Avançadas)
```powershell
# Executar em uma porta específica
.\dist\GhostDrop-P2P.exe --port 9000

# Executar sem abrir o navegador automaticamente
.\dist\GhostDrop-P2P.exe --no-browser
```

### Opção 3: Executar a partir do código fonte (Python)
```powershell
python main.py
```

---

## 🔨 Como Compilar Novamente (`build.bat`)

Para gerar um novo arquivo `.exe` a qualquer momento, execute o script:
```powershell
.\build.bat
```
O executável final será salvo na pasta `dist/GhostDrop-P2P.exe`.

---

## 🔒 Arquitetura de Segurança

| Camada | Tecnologia | Função |
| :--- | :--- | :--- |
| **Transporte de Dados** | WebRTC DataChannels | Envio P2P direto entre máquinas com DTLS 1.3 |
| **Travessia de NAT** | STUN (Google & Cloudflare) | Descoberta de IP público e abertura de portas NAT |
| **Sinalização** | Ephemeral WSS MQTT | Troca de SDP e ICE sem armazenamento ou log |
| **Criptografia de Sinal** | AES-256-GCM + PBKDF2 | Proteção criptográfica de ponta a ponta do código de pareamento |
| **Integridade** | SHA-256 Checksum | Verificação automática de integridade bit a bit após o download |
