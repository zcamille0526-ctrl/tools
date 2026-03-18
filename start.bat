@echo off
title Textbook Tool
echo.
echo  ========================================
echo    Textbook Knowledge Tool - Starting...
echo  ========================================
echo.

:: ---- Check Python ----
python --version >nul 2>&1
if errorlevel 1 (
    python3 --version >nul 2>&1
    if errorlevel 1 (
        echo  [ERROR] Python not found!
        echo.
        echo  Please install Python:
        echo  1. Go to https://www.python.org/downloads/
        echo  2. Click "Download Python 3.x.x"
        echo  3. IMPORTANT: Check "Add Python to PATH" at the bottom
        echo  4. After install, double-click this file again
        echo.
        pause
        exit /b 1
    )
    set PYTHON=python3
) else (
    set PYTHON=python
)

echo  [OK] Python found:
%PYTHON% --version

:: ---- Check dependencies ----
%PYTHON% -c "import pdfplumber, openpyxl" >nul 2>&1
if errorlevel 1 (
    echo.
    echo  [INFO] First run - installing dependencies...
    %PYTHON% -m pip install pdfplumber openpyxl -q
    if errorlevel 1 (
        echo  [ERROR] Install failed. Please run manually:
        echo    pip install pdfplumber openpyxl
        pause
        exit /b 1
    )
    echo  [OK] Dependencies installed
)

:: ---- Start server ----
echo.
echo  [START] Launching server...
echo  Browser will open: http://localhost:8686
echo.

cd /d "%~dp0"

start "" cmd /c "timeout /t 2 /nobreak >nul && start http://localhost:8686"

%PYTHON% app.py
pause
