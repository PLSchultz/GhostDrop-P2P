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
- [ ] Extract cryptographic helpers (deriveKey, encrypt/decrypt, hash) into crypto.js.
- [ ] Move generic helpers (formatBytes, formatTime, generateRandomCode, etc.) into utils.js.

## CI / Build
- [ ] Add 
equirements.txt / pyproject.toml.
- [ ] Update uild.bat to use version from manifest.
- [ ] Create GitHub Actions workflow for lint, test, and build.

## Documentation
- [ ] Expand README with contribution guide, architecture diagram (Mermaid), and API reference.
- [ ] Generate changelog automatically from tags.

---
*Prioridade:* alta para logging e modulariza��o, m�dia para CI e documenta��o.
