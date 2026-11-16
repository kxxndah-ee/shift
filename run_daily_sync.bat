@echo off
chcp 65001 > nul
cd /d "%~dp0"
echo [Shift] 일일 인사 발령 자동 수집 파이프라인 가동...
.venv\Scripts\python.exe daily_sync.py --once
echo [Shift] 수집 및 DB 동기화가 완료되었습니다.
