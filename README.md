# Stock Market Data Collection API

주식 시장 데이터 수집 서버 (Toss, Naver 증권 크롤러)

## 📋 목차

- [프로젝트 구조](#프로젝트-구조)
- [요구사항](#요구사항)
- [로컬 개발 환경 설정](#로컬-개발-환경-설정)
- [Docker로 실행](#docker로-실행)
- [API 사용법](#api-사용법)
- [GCS 설정](#gcs-설정)

---

## 📁 프로젝트 구조

```
marketing/
├── app/
│   ├── common/              # 공통 모듈
│   │   ├── logger.py        # 로깅 시스템
│   │   ├── errors.py        # 커스텀 에러
│   │   ├── gcs_uploader.py  # GCS 업로드 (재사용 가능)
│   │   └── request_function.py
│   └── routers/             # API 라우터
│       ├── toss/            # Toss 증권 크롤러
│       └── naver/           # Naver 증권 크롤러
├── credentials/             # GCP 인증 키
├── main.py                  # FastAPI 진입점
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── .env                     # 환경 변수
```

---

## 📦 요구사항

### 로컬 개발
- Python 3.12+
- pip

### Docker
- Docker Desktop
- Docker Compose

---

## 🚀 로컬 개발 환경 설정

### 1. 가상환경 생성 및 활성화

```bash
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate   # Windows
```

### 2. 의존성 설치

```bash
pip install -r requirements.txt
```

### 3. Playwright 브라우저 설치

```bash
playwright install firefox chromium
```

### 4. 환경 변수 설정

`.env` 파일을 확인하고 필요한 값을 설정하세요:

```bash
# GCS Configuration
GCS_BUCKET_NAME=your-bucket-name
GCS_CREDENTIALS_PATH=./credentials/your-key.json

# Crawler Configuration
MAX_THREADS=5
MAX_RETRIES=3
REQUEST_DELAY=0.1
```

### 5. 서버 실행

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### 6. API 문서 확인

브라우저에서 다음 주소로 접속:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

---

## 🐳 Docker로 실행

### 1. Docker 이미지 빌드 및 실행

```bash
docker-compose up -d --build
```

### 2. 로그 확인

```bash
docker-compose logs -f api
```

### 3. 컨테이너 중지

```bash
docker-compose down
```

### 4. 완전 삭제 (볼륨 포함)

```bash
docker-compose down -v
```

---

## 📡 API 사용법

### Naver 증권 토론 게시물 수집

**엔드포인트:** `POST /api/naver/discussions/manual`

**요청 예시:**

```bash
curl -X POST "http://localhost:8000/api/naver/discussions/manual" \
  -H "Content-Type: application/json" \
  -d '{
    "stock_code": "005930",
    "stock_name": "삼성전자",
    "max_posts": 50
  }'
```

**응답 예시:**

```json
{
  "code": 200,
  "message": "네이버 토론 게시물 수집 및 업로드 완료",
  "stock_code": "005930",
  "total_discussions": 50,
  "partitions": 3,
  "parquet_urls": [
    "gs://bucket/marketing/stock_discussion/dt=2025-11-15/005930_2025-11-15.parquet",
    "gs://bucket/marketing/stock_discussion/dt=2025-11-14/005930_2025-11-14.parquet",
    "gs://bucket/marketing/stock_discussion/dt=2025-11-13/005930_2025-11-13.parquet"
  ]
}
```

### Toss 증권 댓글 수집

**엔드포인트:** `POST /api/toss/post-comments/manual`

**요청 예시:**

```bash
curl -X POST "http://localhost:8000/api/toss/post-comments/manual" \
  -H "Content-Type: application/json" \
  -d '{
    "start": "2025/11/01T00:00:00",
    "end": "2025/11/15T23:59:59",
    "stock_code": "KR7005930003",
    "corp_name": "삼성전자"
  }'
```

---

## ☁️ GCS 설정

### 1. GCP 서비스 계정 키 생성

1. [GCP Console](https://console.cloud.google.com/) 접속
2. IAM & Admin > Service Accounts
3. 서비스 계정 생성 및 키 다운로드 (JSON)
4. `credentials/` 폴더에 저장

### 2. 권한 설정

서비스 계정에 다음 권한 부여:
- `Storage Object Admin` (또는 `Storage Object Creator`)

### 3. 환경 변수 설정

`.env` 파일에 다음 추가:

```bash
GCS_BUCKET_NAME=your-bucket-name
GCS_CREDENTIALS_PATH=./credentials/your-service-account-key.json
```

---

## 🧪 테스트

### Swagger UI로 테스트

1. 서버 실행 후 http://localhost:8000/docs 접속
2. `/api/naver/discussions/manual` 또는 `/api/toss/post-comments/manual` 선택
3. "Try it out" 클릭
4. 파라미터 입력 후 "Execute" 클릭

### curl로 테스트

```bash
# Naver 크롤러 테스트
curl -X POST "http://localhost:8000/api/naver/discussions/manual" \
  -H "Content-Type: application/json" \
  -d '{"stock_code": "005930", "max_posts": 10}'

# Toss 크롤러 테스트
curl -X POST "http://localhost:8000/api/toss/post-comments/manual" \
  -H "Content-Type: application/json" \
  -d '{"stock_code": "KR7005930003", "corp_name": "삼성전자", "start": "2025/11/01T00:00:00", "end": "2025/11/15T23:59:59"}'
```

---

## 📊 데이터 저장 형식

### GCS 저장 구조 (Hive Partition)

```
gs://bucket-name/
└── marketing/
    └── stock_discussion/
        ├── dt=2025-11-15/
        │   ├── 005930_2025-11-15.parquet
        │   └── KR7005930003_2025-11-15.parquet
        ├── dt=2025-11-14/
        │   └── 005930_2025-11-14.parquet
        └── ...
```

### Parquet 스키마

| 컬럼명 | 타입 | 설명 |
|--------|------|------|
| stock_code | string | 종목 코드 |
| isin_code | string | ISIN 코드 |
| stock_name | string | 종목명 |
| comment_id | int | 게시물/댓글 ID |
| author_name | string | 작성자 |
| date | string | 작성일시 (YYYY-MM-DD HH:MM:SS) |
| content | string | 내용 |
| likes_count | int | 좋아요 수 |
| dislikes_count | int | 싫어요 수 |
| comment_data | string | 댓글 데이터 (JSON) |
| dt | string | 파티션 키 (YYYY-MM-DD) |
| source | string | 출처 (naver/toss) |

---

## 🔧 트러블슈팅

### Playwright 브라우저 설치 실패

```bash
# 수동 설치
playwright install firefox chromium
playwright install-deps
```

### GCS 업로드 실패

1. 서비스 계정 키 경로 확인
2. GCS 버킷 권한 확인
3. 환경 변수 확인

### Docker 빌드 느림

```bash
# 캐시 없이 재빌드
docker-compose build --no-cache
```

---

## 📝 라이선스

Internal Use Only

---

## 👥 Authors

Moniq Team
