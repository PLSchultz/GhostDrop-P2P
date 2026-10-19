# TODO � GhostDrop-P2P Refactor

## Backend (main.py)
- [x] Add structured logging (logging module) with INFO/ERROR levels.
- [x] Add type hints and improve docstrings (Google/NumPy style).
- [x] Replace os.path usage with pathlib.Path.
- [x] Centralize argument parsing in a dedicated function.
- [x] Create /api/health endpoint returning JSON status.
- [x] Implement graceful shutdown using httpd.shutdown() instead of os._exit.
- [x] Externalize configuration (port, STUN servers) to config.yaml.
- [x] Add version constant __version__ and display on startup.
- [x] Add robust error handling for HTTP routes (try/except, proper status codes).

## Video & Audio Conference (Google Meet Style)
- [x] Integrate WebRTC MediaStream transceivers (Audio + Video) into mesh connections.
- [x] Build Google Meet style floating control toolbar (Mute, Camera Toggle, Screen Sharing, Hangup).
- [x] Implement dynamic Video Grid with automatic layout adaptation for 1 to N participants.
- [x] Add Avatar placeholder when video is off, showing custom nickname initials and dynamic colors.
- [x] Implement Web Audio API Analyser for real-time speaking indicators (pulsing emerald glow ring).
- [x] Synchronize media state across peers via DataChannel and signaling fallback.
- [x] Add keyboard shortcuts (Ctrl+D for Mic, Ctrl+E for Camera).
- [x] Add Theater Mode and Collapsible Panel controls.

## Frontend (static/app.js ? modules)
- [ ] Split into modules: utils.js, crypto.js, 
tc.js, ui.js.
- [ ] Create entry point pp.js that imports the modules (	ype=" module\).
- [ ] Move all global variables into a namespace or module scope.
- [ ] Implement CSS variables and dark-mode / glassmorphism in heme.css.
- [ ] Add micro-animations and toast feedback in nimations.css.
- [ ] Configure ESLint & Prettier for the project.
- [ ] Write unit tests for critical functions using Jest.
- [ ] Ensure accessibility (ARIA attributes, contrast, focus styles).
- [ ] Refactor DOM manipulation into ui.js (participants list, modals, toasts).
- [ ] Refactor WebRTC signalling, ICE handling, fallback-relay into 
tc.js.
- [x] Extract cryptographic helpers (deriveKey, encrypt/decrypt, hash) into crypto.js.
- [x] Move generic helpers (formatBytes, formatTime, generateRandomCode, etc.) into utils.js.

## CI / Build
- [x] Add requirements.txt / pyproject.toml.
- [x] Update build.bat to use version from manifest.
- [ ] Create GitHub Actions workflow for lint, test, and build.

## Documentation
- [x] Expand README with contribution guide, architecture diagram (Mermaid), and API reference.
- [ ] Generate changelog automatically from tags.

---
*Prioridade:* alta para logging e modulariza��o, m�dia para CI e documenta��o.
## Proximos passos (modularizacao frontend)
- [ ] Refatorar app.js para importar helpers de utils.js (remover duplicatas).
- [ ] Refatorar app.js para importar helpers de crypto.js (remover duplicatas).
- [ ] Criar ui.js com toda a manipulacao de DOM (participants list, modals, toasts, chat bubbles).
- [ ] Criar rtc.js com WebRTC signaling, ICE handling e fallback relay.
- [ ] Criar entry point app.js que importa os modulos (type=module).
- [ ] Mover todas as variaveis globais para namespace ou module scope.
