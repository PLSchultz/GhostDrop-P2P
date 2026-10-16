#!/usr/bin/env python3
"""
GhostDrop P2P - Executavel Standalone de Transferencia Segura Sem Rastros

Servidor local que hospeda a interface WebRTC P2P no navegador do usuario.
Zero persistencia, zero telemetria, zero rastros.
"""

from __future__ import annotations

import json
import logging
import socket
import sys
import threading
import webbrowser
import argparse
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from typing import Optional

import yaml  # PyYAML

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
__version__ = "1.1.0"

# ---------------------------------------------------------------------------
# Encoding - seguro para Windows CMD / PowerShell
# ---------------------------------------------------------------------------
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# ---------------------------------------------------------------------------
# Base paths (suporta execucao normal e PyInstaller single-file .exe)
# ---------------------------------------------------------------------------
if getattr(sys, "frozen", False):
    BASE_DIR: Path = Path(getattr(sys, "_MEIPASS", Path(__file__).parent))
else:
    BASE_DIR = Path(__file__).parent.resolve()

STATIC_DIR: Path = BASE_DIR / "static"
CONFIG_FILE: Path = BASE_DIR / "config.yaml"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ghostdrop")


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    """Load configuration from config.yaml if it exists.

    Returns:
        dict: Parsed YAML content, or an empty dict on failure.
    """
    if CONFIG_FILE.exists():
        try:
            with CONFIG_FILE.open("r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}
            logger.info("Configuracao carregada de %s", CONFIG_FILE)
            return data
        except Exception as exc:
            logger.error("Falha ao ler config.yaml: %s", exc)
    return {}


# ---------------------------------------------------------------------------
# Network helpers
# ---------------------------------------------------------------------------

def get_local_ip() -> str:
    """Determine the machine LAN IP address.

    Returns:
        str: LAN IP, or 127.0.0.1 as fallback.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"


def find_available_port(start_port: int = 49152, max_attempts: int = 100) -> int:
    """Find a free local TCP port, starting from start_port.

    Args:
        start_port: First port to try.
        max_attempts: How many consecutive ports to probe.

    Returns:
        int: First available port found, or 8080 as a last resort.
    """
    for port in range(start_port, start_port + max_attempts):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("0.0.0.0", port))
                return port
        except OSError:
            continue
    return 8080


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------

class GhostDropHandler(SimpleHTTPRequestHandler):
    """Custom HTTP handler that serves the static SPA and exposes JSON APIs."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

    def do_GET(self) -> None:
        if self.path == "/api/info":
            self._send_json({"lan_ip": get_local_ip(), "port": self.server.server_port})
            return

        if self.path == "/api/health":
            self._send_json({"status": "ok", "version": __version__})
            return

        try:
            super().do_GET()
        except Exception as exc:
            logger.error("Erro ao servir GET %s: %s", self.path, exc)
            self.send_error(500, "Internal server error")

    def do_POST(self) -> None:
        if self.path == "/api/shutdown":
            self._send_json({"status": "shutting_down"})
            _schedule_shutdown(self.server)
            return

        self.send_error(404, "Endpoint not found")

    def _send_json(self, payload: dict) -> None:
        """Serialize payload as JSON and write a 200 response."""
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def end_headers(self) -> None:
        """Inject no-cache headers to prevent browser caching (zero traces)."""
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def log_message(self, format: str, *args) -> None:
        # Redirect HTTP access log to the structured logger at DEBUG level
        logger.debug(format, *args)


# ---------------------------------------------------------------------------
# Shutdown helper
# ---------------------------------------------------------------------------

def _schedule_shutdown(server: HTTPServer) -> None:
    """Request a graceful server shutdown from a background thread.

    Uses HTTPServer.shutdown() instead of os._exit so the finally block
    in main() runs normally and resources are freed cleanly.
    """
    def _run() -> None:
        import time
        time.sleep(0.4)
        logger.info("Encerrando servidor via requisicao /api/shutdown ...")
        server.shutdown()

    threading.Thread(target=_run, daemon=True).start()


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description=f"GhostDrop P2P v{__version__} - Transferencia de Arquivos Sem Rastros"
    )
    parser.add_argument("--port", type=int, default=None, help="Porta para o servidor local")
    parser.add_argument("--host", default=None, help="Endereco de escuta (padrao: 0.0.0.0)")
    parser.add_argument("--no-browser", action="store_true", help="Nao abrir o navegador automaticamente")
    parser.add_argument("--log-level", default=None, help="Nivel de log: DEBUG, INFO, WARNING, ERROR")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Start GhostDrop P2P server."""
    config = _load_config()
    args = _parse_args()

    # Resolve effective settings: CLI > config.yaml > defaults
    log_level_str: str = args.log_level or config.get("log_level", "INFO")
    log_level: int = getattr(logging, log_level_str.upper(), logging.INFO)
    logging.getLogger().setLevel(log_level)
    logger.setLevel(log_level)

    raw_port: Optional[int] = args.port or config.get("port") or None
    port: int = int(raw_port) if raw_port else find_available_port()

    raw_host = args.host or config.get("host", "0.0.0.0")
    host: str = str(raw_host).strip()

    try:
        httpd = HTTPServer((host, port), GhostDropHandler)
    except Exception as exc:
        logger.error("Falha ao iniciar na porta %d: %s", port, exc)
        sys.exit(1)

    lan_ip = get_local_ip()
    local_url = f"http://127.0.0.1:{port}"
    lan_url = f"http://{lan_ip}:{port}"

    print("=" * 60)
    print(f"   GhostDrop P2P v{__version__} - Transferencia Segura Sem Rastros")
    print("=" * 60)
    logger.info("Servidor local ativo em: %s", local_url)
    if lan_ip != "127.0.0.1":
        logger.info("Acesso na rede local (Wi-Fi): %s", lan_url)
    logger.info("Modo: Zero-Storage / WebRTC P2P Direto + Fallback E2EE")
    logger.info("Pressione Ctrl+C ou clique em 'Encerrar & Limpar' para sair.")
    print("=" * 60)

    if not args.no_browser:
        threading.Timer(0.6, lambda: webbrowser.open(local_url)).start()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt recebido - encerrando ...")
    finally:
        httpd.server_close()
        logger.info("Concluido. Zero rastros deixados.")


if __name__ == "__main__":
    main()
