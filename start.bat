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
%PYTHON% -c "import pdfplumber, openpyxl, multipart" >nul 2>&1
if errorlevel 1 (
    echo.
    echo  [INFO] First run - installing dependencies...
    %PYTHON% -m pip install pdfplumber openpyxl multipart -q
    if errorlevel 1 (
        echo  [ERROR] Install failed. Please run manually:
        echo    pip install pdfplumber openpyxl multipart
        pause
        exit /b 1
    )
    echo  [OK] Dependencies installed
)

:: ---- Check optional OCR dependencies ----
%PYTHON% -c "import aip, pdf2image, PIL" >nul 2>&1
if errorlevel 1 (
    echo  [INFO] OCR dependencies not found (baidu-aip, pdf2image, Pillow)
    echo  [INFO] Scanned PDF support will be disabled.
    echo  [INFO] To enable, run: pip install baidu-aip pdf2image Pillow
)

:: ---- Start server ----
echo.
echo  [START] Launching server...
echo  Browser will open: http://localhost:8788
echo.

cd /d "%~dp0"

start /min "" cmd /c "timeout /t 2 /nobreak >nul && start http://localhost:8788"

%PYTHON% app.py
if errorlevel 1 (
    echo.
    echo  [ERROR] Server exited unexpectedly!
    echo  Please check the error message above.
)
pause
