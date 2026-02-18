@echo off
setlocal

for /f "tokens=2-4 delims=/ " %%a in ('date /t') do set D=%%c%%a%%b
for /f "tokens=1-2 delims=: " %%a in ('time /t') do set T=%%a%%b
set LOG_FILE=logs\run_%D%_%T%.log

if not exist logs mkdir logs

call :run 2>&1 | tee %LOG_FILE%
exit /b %ERRORLEVEL%

:run
echo === Run started: %date% %time% ===

echo --- uv sync ---
uv sync || exit /b 1

echo --- 01_cohort.py ---
uv run python Code\01_cohort.py || exit /b 1

echo --- 02_table1.py ---
uv run python Code\02_table1.py || exit /b 1

echo --- 03_ase_visualizations.py ---
uv run python Code\03_ase_visualizations.py || exit /b 1

echo === Run finished: %date% %time% ===
exit /b 0
