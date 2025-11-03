# Marketing

## 📂 프로젝트 구조
```
├── app/
│   ├── common/
│   │   ├── request_function.py   # 스크래핑 라이브러리 래퍼 모듈
│   │   └── logger.py             # 로깅 시스템 설정
│   ├── models/                   # Pydantic 데이터 모델
│   └── routers/                  # API 엔드포인트 라우터
├── log/                          # 로그 파일 저장 디렉토리 (Git 추적 제외)
├── .gitignore
├── main.py                       # FastAPI 애플리케이션 진입점
├── requirements.txt              # Python 의존성 목록
└── README.md
```

## 🚀 로컬 환경에서 실행하기 (Local Setup)

### Prerequisites

- Python 3.12.12 ([`pyenv`](https://github.com/pyenv/pyenv) 사용을 권장합니다.)
- Git

### 설치 및 실행 순서

1.  **저장소 클론:**
    ```sh
    git clone https://github.com/moniqcorp/marketing.git
    cd toss-scraping
    ```

2.  **파이썬 버전 설정:**
    (`pyenv`를 사용하는 경우)
    ```sh
    pyenv local 3.12.12
    ```

3.  **가상환경 생성 및 활성화:**
    ```sh
    python3.12 -m venv venv
    source venv/bin/activate
    ```

4.  **의존성 라이브러리 설치:**
    ```sh
    pip install -r requirements.txt
    ```

5.  **Playwright 브라우저 설치:**
    ```sh
    playwright install --with-deps
    ```

6.  **FastAPI 서버 실행:**
    ```sh
    uvicorn main:app --host 0.0.0.0 --port 8003 --reload
    ```
