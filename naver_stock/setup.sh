#!/bin/bash
# (set -e: 중간에 오류 발생 시 즉시 스크립트 중단)
set -e

# --- 설정 (VENV_DIR 경로 수정) ---
VENV_DIR=".venv"
REQUIREMENTS_FILE="requirements.txt"
# ---

echo "Starting environment setup using 'venv'..."

# 1. python3가 있는지 확인
command -v python3 >/dev/null 2>&1 || { echo >&2 "Error: python3 is not installed."; exit 1; }

# 2. requirements.txt 파일 확인
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    echo "Error: $REQUIREMENTS_FILE not found."
    exit 1
fi

# 3. (⭐️경로 수정⭐️) python3의 venv 모듈로 가상환경 생성 (루트 폴더에)
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment at $VENV_DIR using 'python3 -m venv'..."
    python3 -m venv $VENV_DIR
else
    echo "Virtual environment already exists at $VENV_DIR."
fi

# 4. 가상환경 활성화 (이 스크립트 내에서만 활성화됨)
echo "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# 5. (⭐️uv 제거⭐️) venv 안의 pip를 업그레이드
echo "Upgrading 'pip' inside the virtual environment..."
pip install --upgrade pip

# 6. (⭐️uv 제거⭐️) pip를 이용해 requirements.txt 설치
echo "Installing dependencies from $REQUIREMENTS_FILE using 'pip'..."
pip install -r $REQUIREMENTS_FILE

echo "----------------------------------------"
echo "✅ Setup complete. Environment is ready."
echo "You can now run the bot using ./start_naver.sh"
echo "----------------------------------------"

