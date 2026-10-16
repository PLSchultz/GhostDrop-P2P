@echo off
title Compilando GhostDrop P2P Standalone

REM -----------------------------------------------------------------------
REM Ler versao do main.py (__version__ = "x.y.z")
REM -----------------------------------------------------------------------
for /f "tokens=3 delims= " %%v in ('python -c "import main; print(main.__version__)"') do set APP_VERSION=%%v
if "%APP_VERSION%"=="" set APP_VERSION=dev

echo ===================================================
echo     Compilando GhostDrop P2P v%APP_VERSION% para Executavel .EXE
echo ===================================================
echo.

REM Verifica se PyInstaller esta instalado
python -m pip show pyinstaller >nul 2>&1
if %errorlevel% neq 0 (
    echo [!] Instalando PyInstaller...
    python -m pip install pyinstaller
)

REM Instalar dependencias do projeto
echo [*] Instalando dependencias (requirements.txt)...
python -m pip install -r requirements.txt -q

echo [*] Gerando executavel standalone unico (sem dependencias externas)...
python -m PyInstaller --noconfirm --onefile --console ^
  --name "GhostDrop-P2P-v%APP_VERSION%" ^
  --add-data "static;static" ^
  --add-data "config.yaml;." ^
  main.py

if %errorlevel% equ 0 (
    echo.
    echo ===================================================
    echo  [+] COMPILACAO CONCLUIDA COM SUCESSO!
    echo  [+] Executavel: dist\GhostDrop-P2P-v%APP_VERSION%.exe
    echo ===================================================
) else (
    echo.
    echo [!] Erro durante a compilacao.
)

pause