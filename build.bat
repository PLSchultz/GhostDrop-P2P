@echo off
title Compilando GhostDrop P2P Standalone
echo ===================================================
echo     Compilando GhostDrop P2P para Executavel .EXE
echo ===================================================
echo.

REM Verifica se PyInstaller esta instalado
python -m pip show pyinstaller >nul 2>&1
if %errorlevel% neq 0 (
    echo [!] Instalando PyInstaller...
    python -m pip install pyinstaller
)

echo [*] Gerando executavel standalone único (sem dependencias externas)...
python -m PyInstaller --noconfirm --onefile --console ^
  --name "GhostDrop-P2P" ^
  --add-data "static;static" ^
  main.py

if %errorlevel% equ 0 (
    echo.
    echo ===================================================
    echo  [+] COMPILACAO CONCLUIDA COM SUCESSO!
    echo  [+] Executavel gerado em: dist\GhostDrop-P2P.exe
    echo ===================================================
) else (
    echo.
    echo [!] Erro durante a compilacao.
)

pause

