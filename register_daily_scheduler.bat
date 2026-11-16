@echo off
chcp 65001 > nul
cd /d "%~dp0"
set PYTHON_PATH=%~dp0.venv\Scripts\python.exe
set SCRIPT_PATH=%~dp0daily_sync.py

echo ============================================================
echo [Shift] Windows 작업 스케줄러 일일 자동 수집 등록기
echo 매일 오전 06:30에 자동으로 신규 인사 발령을 수집하여 DB에 적재합니다.
echo ============================================================

schtasks /create /tn "ShiftMediaPersonnelDailySync" /tr "\"%PYTHON_PATH%\" \"%SCRIPT_PATH%\" --once" /sc daily /st 06:30 /f

if %ERRORLEVEL% EQU 0 (
    echo [성공] 작업 스케줄러 등록이 완료되었습니다.
    echo * 등록 확인: schtasks /query /tn "ShiftMediaPersonnelDailySync"
    echo * 즉시 실행: schtasks /run /tn "ShiftMediaPersonnelDailySync"
) else (
    echo [안내] 권한 부족 시 관리자 권한 명령 프롬프트에서 다시 실행해 주세요.
)
pause
