@echo off
REM NYC Taxi Capstone Pipeline Launcher

cd /d "%~dp0"

echo ================================================
echo   NYC Taxi Capstone - Starting ETL Pipeline
echo ================================================

if exist ".venv\Scripts\activate.bat" (
    call ".venv\Scripts\activate.bat"
)

python -m pipeline.pipeline

echo.
echo ================================================
echo   Pipeline finished. Check logs\ for details.
echo ================================================
pause