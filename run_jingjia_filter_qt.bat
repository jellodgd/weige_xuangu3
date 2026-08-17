@echo off
setlocal

cd /d "%~dp0"
chcp 65001 >nul
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

echo ========================================
echo MiniQMT jingjia filter Qt launcher
echo Workdir: %cd%
echo Time: %date% %time%
echo ========================================
echo.

where python >nul 2>nul
if errorlevel 1 (
    echo [ERROR] python was not found in PATH. Please check: python --version
    pause
    exit /b 1
)

python --version
echo.

python ".\jingjia_filter_qt.py"
set EXIT_CODE=%ERRORLEVEL%

echo.
echo ========================================
echo Finished with exit code: %EXIT_CODE%
echo ========================================
pause

exit /b %EXIT_CODE%
