#!/usr/bin/env python3
"""
GhostDrop P2P - Executável Standalone de Transferência Segura Sem Rastros
"""

import sys
import os
import socket
import threading
import webbrowser
import argparse
from http.server import HTTPServer, SimpleHTTPRequestHandler
import mimetypes

# Configure UTF-8 encoding safely for Windows command prompt
try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

# Determine base path (supports both normal run and PyInstaller single-file .exe)
if getattr(sys, 'frozen', False):
    # PyInstaller bundle extraction temp directory
    BASE_DIR = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

STATIC_DIR = os.path.join(BASE_DIR, 'static')


def find_available_port(start_port=49152, max_attempts=100):
    """Find a free local TCP port."""
    for port in range(start_port, start_port + max_attempts):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('127.0.0.1', port))
                return port
        except OSError:
            continue
    return 8080


class GhostDropHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=STATIC_DIR, **kwargs)

    def do_POST(self):
        if self.path == '/api/shutdown':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(b'{"status":"shutting_down"}')
            
            # Trigger server shutdown in a separate thread
            def kill_server():
                import time
                time.sleep(0.5)
                os._exit(0)
            
            threading.Thread(target=kill_server, daemon=True).start()
            return

        self.send_error(404, "Endpoint not found")

    def end_headers(self):
        # Prevent any caching in browser to leave zero persistent traces
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        super().end_headers()

    def log_message(self, format, *args):
        # Silent logger for stealth & zero console pollution
        pass


def main():
    parser = argparse.ArgumentParser(description="GhostDrop P2P - Transferencia de Arquivos Sem Rastros")
    parser.add_argument("--port", type=int, default=None, help="Porta para o servidor local")
    parser.add_argument("--no-browser", action="store_true", help="Nao abrir o navegador automaticamente")
    parser.add_argument("--host", default="127.0.0.1", help="Endereco de escuta (padrao: 127.0.0.1)")
    args = parser.parse_args()

    port = args.port or find_available_port()
    host = args.host

    server_address = (host, port)
    try:
        httpd = HTTPServer(server_address, GhostDropHandler)
    except Exception as e:
        print(f"[ERRO] Falha ao iniciar na porta {port}: {e}")
        sys.exit(1)

    url = f"http://{host}:{port}"
    print("=" * 60)
    print("   [GhostDrop P2P] - Transferencia Segura Sem Rastros")
    print("=" * 60)
    print(f"[*] Servidor local ativo em: {url}")
    print("[*] Modo: Zero-Storage / WebRTC P2P Direto")
    print("[*] Pressione Ctrl+C ou clique em 'Encerrar & Limpar' na interface para sair.")
    print("=" * 60)

    if not args.no_browser:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[!] Encerrando GhostDrop e liberando memória...")
    finally:
        httpd.server_close()
        print("[+] Concluído. Zero rastros deixados.")


if __name__ == '__main__':
    main()
