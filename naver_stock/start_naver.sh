#!/bin/bash
# (set -e: 중간에 오류 발생 시 즉시 스크립트 중단)
set -e

# --- 설정 (VENV_DIR 경로 수정) ---
VENV_DIR=".venv"
LOG_DIR="logs"
PYTHON_SCRIPT="main.py"
LOG_PREFIX="naver_crawler"
# ---

# 1. 가상환경 폴더 확인
if [ ! -d "$VENV_DIR" ]; then
    echo "Error: Virtual environment '$VENV_DIR' not found."
    echo "Please run ./setup.sh first."
    exit 1
fi

# 2. 가상환경 활성화 (경로 수정)
source "$VENV_DIR/bin/activate"
echo "Virtual environment activated."

# 3. 로그 디렉터리가 없으면 생성
mkdir -p $LOG_DIR

# 4. YYYYMMDD-HHMMSS 형식의 타임스탬프 생성
TIMESTAMP=$(date +'%Y%m%d-%H%M%S')

# 5. 타임스탬프를 포함한 로그 파일 경로 설정
LOG_FILE="${LOG_DIR}/${LOG_PREFIX}_${TIMESTAMP}.log"

# 6. 스크립트 실행
echo "=========================================="
echo "Naver Stock Discussion Crawler"
echo "=========================================="
echo "Python script: $PYTHON_SCRIPT"
echo "Log file: $LOG_FILE"
echo ""
echo "Date range options:"
echo "  1. Default: Last 3 days (today - 2 days to today)"
echo "  2. Custom: ./start_naver.sh 2025-01-01 2025-01-07"
echo "=========================================="
echo ""

# 가상환경의 python 사용
# 명령줄 인자가 있으면 전달, 없으면 기본값(최근 3일) 사용
if [ $# -ge 2 ]; then
    echo "Using custom date range: $1 ~ $2"
    python $PYTHON_SCRIPT "$1" "$2" 2>&1 | tee $LOG_FILE
else
    echo "Using default date range (last 3 days)"
    python $PYTHON_SCRIPT 2>&1 | tee $LOG_FILE
fi

echo ""
echo "=========================================="
echo "Script execution finished."
echo "Log saved to: $LOG_FILE"
echo "=========================================="