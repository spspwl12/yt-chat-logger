# YouTube Live Chat Logger & Viewer

YouTube 라이브 스트리밍의 실시간 채팅을 수집하고 데이터베이스에 저장하며, 웹 브라우저를 통해 직관적으로 채팅 로그를 검색하고 조회할 수 있는 서비스입니다.

## 🚀 주요 기능

- **실시간 채팅 수집:** `youtube-chat` 패키지를 활용하여 여러 YouTube 라이브 방송의 채팅을 동시에 수집합니다.
- **데이터베이스 저장 (MySQL):** 수집한 채팅 메시지를 MySQL 데이터베이스에 안정적으로 보관합니다.
- **형태소 분석 및 Full-Text 검색:** `mecab`을 사용하여 채팅 내용을 형태소 단위로 분석(토큰화)하고, 이를 바탕으로 빠르고 정확한 자연어 검색(Full-Text Search)을 제공합니다.
- **데이터 용량 최적화:** `lz-string`을 사용하여 채팅 메시지 데이터를 UTF-16 형식으로 압축 저장하여 DB 용량 부담을 최소화합니다.
- **장애 대응 및 자동 재연결:** DB 연결이 끊어지거나 YouTube 라이브 스트리밍 소켓 연결이 불안정할 경우, 자동으로 백그라운드 재연결을 시도하여 누락을 방지합니다.
- **직관적인 웹 UI 뷰어:**
  - 무한 스크롤(위/아래 양방향)을 통한 매끄러운 과거 및 최신 채팅 조회 지원.
  - 방송 ID, 닉네임, 채팅 내용 등 다중 조건 검색 지원.
  - 검색어에 `superchat` 입력 시 슈퍼챗 메시지만 모아서 볼 수 있는 필터 기능.
  - 닉네임 더블 클릭 시 해당 유저의 채팅만 필터링하는 빠른 검색 지원.
  - 메시지 내용 더블 클릭 시 해당 시점의 위아래 채팅 문맥을 손쉽게 파악할 수 있는 포커스 기능.

## 🛠 사용 기술 및 라이브러리

### Backend (Node.js)
- `express`: 웹 서버 및 API 라우팅
- `youtube-chat`: YouTube 실시간 채팅 수집기
- `mysql2`: MySQL 데이터베이스 연동 및 쿼리
- `lz-string`: 메시지 데이터 스토리지 최적화(압축 로직)
- `mecab-ya.js` (Custom): 한국어 형태소 분석기 래퍼, 검색 가능한 토큰 생성용
- `crypto`, `fs`, `path`: Node.js 내장 모듈

### Frontend (HTML/CSS/JS)
- 프레임워크 없는 순수 Vanilla HTML, CSS, JavaScript로 가볍게 구현
- `lz-string`: 압축된 메시지 데이터 클라이언트단 압축 해제 렌더링 지원

## ⚙️ 설치 및 설정 방법

### 1. 사전 요구 사항
- **Node.js**: v14+ 이상 권장
- **MySQL (또는 MariaDB)**: Full-Text 인덱스를 지원하는 버전
- **Mecab**: 자연어 형태소 분석 처리를 위해 서버 환경에 다운로드 및 `mecab-ya.js` 정상 연동 필요.

### 2. 데이터베이스 셋업
MySQL 데이터베이스를 생성하고, 저장할 테이블을 아래 예시 쿼리를 참고하여 구성합니다.
(※ `index.js`에 설정된 기본값 기준 `DATA` 스키마 및 `youtube_chat2` 테이블입니다.)

```sql
CREATE DATABASE DATA DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE DATA;

CREATE TABLE youtube_chat2 (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    sid VARCHAR(50),
    channel VARCHAR(100),    -- 방송/채널 ID
    author VARCHAR(255),     -- 작성자 닉네임
    authorAlt VARCHAR(255),  
    authorId VARCHAR(100),   -- 작성자 고유 채널 ID
    authorThumb TEXT,        -- 프로필 이미지 URL
    message MEDIUMTEXT,      -- lz-string 압축 데이터 (UTF-16)
    msgdata TEXT,            -- Mecab 형태소 분석 토큰화 데이터
    flag INT,                -- 상태 플래그 (1:멤버십, 2:모더레이터, 4:소유자, 8:인증, 16:슈퍼챗)
    timestamp VARCHAR(50),   -- 시간
    FULLTEXT INDEX ft_msgdata (msgdata) /* 검색 속도 최적화를 위한 Full-Text Index */
) ENGINE=InnoDB;
```

### 3. 환경 설정 및 설치
```bash
# 저장소 클론 및 디렉토리 이동 후 패키지 설치
npm install
```

설치 후 `index.js` 상단의 DB 연동 정보를 운영 환경에 맞게 수정합니다:
```javascript
const DB_HOST = "127.0.0.1";
const DB_USER = "root";
const DB_PASS = "";
const DB_SCHEMA = "DATA";
```

### 4. 서버 실행
```bash
node index.js
```
서버가 정상적으로 실행되면 브라우저에서 `http://localhost:3000` 로 접속하여 뷰어를 확인할 수 있습니다.
*수집 중인 방송 목록 정보는 로컬 루트 폴더 내 `data.json` 파일로 파일 시스템에 자동 저장 및 유지됩니다.*

## 📡 API 엔드포인트 목록

- `GET /` : 웹 기반 채팅 뷰어 페이지 제공
- `GET /create/:id` : 새로운 YouTube 라이브 방송 ID를 등록하여 채팅 데이터 수집 시작
- `GET /delete/:id` : 수집 중인 YouTube 방송 ID의 연결을 종료하고 목록에서 삭제
- `GET /data` : 과거 채팅 로그 가져오기 (파라미터: `channel`, `search`, `start` ID)
- `GET /udata` : 새로운 채팅 로그 가져오기 (파라미터: `channel`, `search`, `start` ID)
