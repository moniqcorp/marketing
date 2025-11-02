#!/bin/bash
# (set -e: 중간에 오류 발생 시 즉시 스크립트 중단)
set -e

# --- 설정 (VENV_DIR 경로 수정) ---
VENV_DIR=".venv"
REQUIREMENTS_FILE="requirements.txt"
# ---

echo "Starting environment setup..."

# 1. uv가 설치되어 있는지 확인
command -v uv >/dev/null 2>&1 || { echo >&2 "Error: uv is not installed. Please install it first (e.g., pip install uv)"; exit 1; }

# 2. requirements.txt 파일 확인
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    echo "Error: $REQUIREMENTS_FILE not found."
    echo "Please create it first (e.g., pip freeze > requirements.txt)"
    exit 1
fi

# 3. naver 디렉터리가 없으면 생성 (경로 확보)
mkdir -p naver

# 4. .venv 가상환경이 없다면 uv로 새로 생성
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment at $VENV_DIR..."
    uv venv $VENV_DIR
else
    echo "Virtual environment already exists at $VENV_DIR."
fi

# 5. 가상환경 활성화 (이 스크립트 내에서만 활성화됨)
source "$VENV_DIR/bin/activate"

# 6. requirements.txt를 기준으로 패키지 설치 (동기화)
echo "Syncing dependencies from $REQUIREMENTS_FILE..."
uv pip sync $REQUIREMENTS_FILE --quiet

echo "----------------------------------------"
echo "✅ Setup complete. Environment is ready."
echo "You can now run the bot using ./start_bot.sh"
echo "----------------------------------------"