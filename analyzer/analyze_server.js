const express = require('express');
const mysql = require('mysql2/promise');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const os = require('os');
const { Worker } = require('worker_threads');

const app = express();
const PORT = 3001;

const DB_HOST = "127.0.0.1";
const DB_USER = "root";
const DB_PASS = "";
const DB_SCHEMA = "DATA";

const CACHE_FILE = path.join(__dirname, 'style_cache.gz');
let cachedData = null;
let isAnalyzing = false;
let buildProgress = '';
let termDF = null; // term → 사용 유저 수 (희귀 용어 판별용)

// ═══════════════════════════════════════════════════════════════
// Worker 스레드 풀 관리
// ═══════════════════════════════════════════════════════════════
const WORKER_COUNT = Math.max(2, Math.min(os.cpus().length - 1, 6));
const WORKER_PATH = path.join(__dirname, 'analyze_worker.js');

function createWorker() {
    return new Worker(WORKER_PATH);
}

function buildTermDF() {
    if (!cachedData) return;
    const df = new Map();
    for (const u of Object.values(cachedData.users)) {
        if (u.st) {
            for (let i = 0; i < u.st.length; i++) df.set(u.st[i], (df.get(u.st[i]) || 0) + 1);
            if (u.it) for (let i = 0; i < u.it.length; i++) df.set(u.it[i], (df.get(u.it[i]) || 0) + 1);
        } else if (u.v) {
            for (const term in u.v) df.set(term, (df.get(term) || 0) + 1);
            if (u.iv) for (const term in u.iv) df.set(term, (df.get(term) || 0) + 1);
        }
    }
    termDF = df;
    cachedData._userCount = Object.keys(cachedData.users).length;
    console.log(`[TermDF] ${df.size.toLocaleString()}개 용어 인덱스 구축 완료`);
}

// ═══════════════════════════════════════════════════════════════
// 관심사 분석용 상수 (서버측에서도 유지 — API에서 사용)
// ═══════════════════════════════════════════════════════════════
const PARTICLES = new Set([
    '이', '가', '은', '는', '을', '를', '에', '의', '와', '과', '로', '으로',
    '도', '만', '부터', '까지', '야', '아', '요', '여', '네', '죠', '고', '며',
    '면', '서', '께', '한테', '에게', '에서', '라', '든', '랑', '이랑', '하'
]);
const SKIP_WORDS = new Set([
    '그래', '네', '응', '아니', '뭐', '거', '것', '수', '때', '안', '못',
    '더', '좀', '잘', '왜', '진짜', 'ㄹㅇ', '아', '어', '오', '이거', '저거',
    '그거', '여기', '거기', '저기', '우리', '나', '너', '제', '내'
]);

async function buildCache() {
    if (isAnalyzing) return;
    isAnalyzing = true;
    const t0 = Date.now();

    const pool = mysql.createPool({
        host: DB_HOST, user: DB_USER, password: DB_PASS, database: DB_SCHEMA,
        waitForConnections: true, connectionLimit: WORKER_COUNT + 2, queueLimit: 0
    });

    try {
        // ═══════════════════════════════════════════════════════════════
        // STEP 1: SQL GROUP BY로 유효 유저를 한방에 추출
        // ═══════════════════════════════════════════════════════════════
        buildProgress = '[1/5] DB에서 헤비 유저 추출 중 (SQL GROUP BY)...';
        console.log('\n' + buildProgress);

        const [countRows] = await pool.execute(`
            SELECT u.authorId, COUNT(*) as cnt
            FROM youtube_chat3 c
            JOIN youtube_user_names n ON c.nid = n.nid
            JOIN youtube_users u ON n.uid = u.uid
            GROUP BY u.authorId
            HAVING cnt >= 30
        `);

        const validMap = new Map();
        for (const r of countRows) {
            validMap.set(r.authorId, { authorName: '', msgCount: Number(r.cnt) });
        }
        console.log(`  => ${validMap.size.toLocaleString()}명 추출 완료 (${((Date.now() - t0) / 1000).toFixed(1)}초 경과)`);

        // ═══════════════════════════════════════════════════════════════
        // STEP 2: nid→authorId 매핑 테이블을 메모리에 구축
        // ═══════════════════════════════════════════════════════════════
        buildProgress = '[2/5] 유저 매핑 테이블 구축 중...';
        console.log('\n' + buildProgress);

        const [uidRows] = await pool.execute('SELECT uid, authorId FROM youtube_users');
        const uidToAuthorId = new Map();
        for (const r of uidRows) uidToAuthorId.set(r.uid, r.authorId);

        const [nidRows] = await pool.execute('SELECT nid, uid, author FROM youtube_user_names');
        const nidLookup = new Map();      // nid → { authorId, author }
        for (const r of nidRows) {
            const authorId = uidToAuthorId.get(r.uid);
            if (authorId && validMap.has(authorId)) {
                nidLookup.set(r.nid, { authorId, author: r.author });
            }
        }
        uidToAuthorId.clear();
        console.log(`  => 매핑 완료! (${nidLookup.size.toLocaleString()}개 프로필)`);

        // ═══════════════════════════════════════════════════════════════
        // STEP 3: 유저 파티셔닝 + 멀티 워커 병렬 분석
        //   - authorId 해시로 유저를 워커에 고정 배정 → 병합 불필요
        //   - nidMap 전송 제거 → 메인에서 authorId 직접 전달
        // ═══════════════════════════════════════════════════════════════
        buildProgress = `[3/5] 문체+관심사 분석 중 (${WORKER_COUNT} 워커 병렬)...`;
        console.log('\n' + buildProgress);

        const workers = [];
        for (let i = 0; i < WORKER_COUNT; i++) workers.push(createWorker());

        let lastId = 0;
        const BATCH_SIZE = 200000;
        let totalProcessed = 0;

        async function fetchBatch(fromId) {
            const [rows] = await pool.execute(
                `SELECT id, nid, channel, msgdata FROM youtube_chat3 WHERE id > ? ORDER BY id ASC LIMIT ${BATCH_SIZE}`,
                [fromId]
            );
            return rows;
        }

        // 더블 버퍼링
        let prefetchPromise = fetchBatch(lastId);

        // authorId → 워커 인덱스 해시
        function workerHash(authorId) {
            let h = 0;
            for (let i = 0; i < authorId.length; i++) h = ((h << 5) - h + authorId.charCodeAt(i)) | 0;
            return ((h % WORKER_COUNT) + WORKER_COUNT) % WORKER_COUNT;
        }

        while (true) {
            const rows = await prefetchPromise;
            if (rows.length === 0) break;

            lastId = rows[rows.length - 1].id;
            prefetchPromise = fetchBatch(lastId);

            // 유저별 워커 라우팅 — 워커 버킷에 [authorId, channel, msgdata] 직접 전달
            const buckets = Array.from({ length: WORKER_COUNT }, () => []);
            for (let i = 0; i < rows.length; i++) {
                const r = rows[i];
                const lookup = nidLookup.get(r.nid);
                if (!lookup) continue;
                const info = validMap.get(lookup.authorId);
                if (info) info.authorName = lookup.author;
                buckets[workerHash(lookup.authorId)].push([lookup.authorId, r.channel, r.msgdata]);
            }

            // 각 워커에 자기 버킷만 전송 (nidMap 전송 없음!)
            const workerPromises = [];
            for (let w = 0; w < WORKER_COUNT; w++) {
                if (buckets[w].length === 0) continue;
                workerPromises.push(new Promise((resolve) => {
                    workers[w].once('message', (msg) => { if (msg.type === 'progress') resolve(); });
                    workers[w].postMessage({ type: 'batch', rows: buckets[w] });
                }));
            }

            await Promise.all(workerPromises);
            totalProcessed += rows.length;
            buildProgress = `[3/5] 문체+관심사 분석: ${totalProcessed.toLocaleString()}건`;
            process.stdout.write(`\r  ... ${totalProcessed.toLocaleString()} 건 처리됨`);
        }

        nidLookup.clear();
        console.log(`\n  => 분석 완료 (${((Date.now() - t0) / 1000).toFixed(1)}초 경과)`);

        // ═══════════════════════════════════════════════════════════════
        // STEP 4: 워커 내 로컬 DF + TF-IDF 계산 + 스트리밍 결과 수집
        //   해시 파티셔닝 → 로컬 DF ≈ 글로벌 DF/N (DF 전송 불필요!)
        //   워커가 200명씩 chunk 전송, 메인은 raw term 데이터 미보유
        // ═══════════════════════════════════════════════════════════════
        buildProgress = '[4/5] TF-IDF 벡터 생성 중 (워커 내 계산)...';
        console.log('\n' + buildProgress);

        const exportUsers = {};
        let totalUsers = 0;

        // 워커별 순차 처리 (한 워커씩 결과 수집 → 메모리 피크 최소화)
        for (let w = 0; w < WORKER_COUNT; w++) {
            await new Promise((resolve) => {
                workers[w].on('message', (msg) => {
                    if (msg.type === 'chunk') {
                        for (const authorId in msg.data) {
                            const vec = msg.data[authorId];
                            const info = validMap.get(authorId);
                            exportUsers[authorId] = {
                                n: info ? info.authorName : '',
                                c: info ? info.msgCount : 0,
                                r: vec.r, v: vec.v,
                                ir: vec.ir, iv: vec.iv
                            };
                        }
                    } else if (msg.type === 'done') {
                        totalUsers += msg.userCount;
                        workers[w].terminate();
                        resolve();
                    }
                });
                workers[w].postMessage({
                    type: 'computeTFIDF',
                    maxStyle: 300, maxInterest: 200
                });
            });
            console.log(`  => 워커 ${w + 1}/${WORKER_COUNT} 완료 (누적 ${Object.keys(exportUsers).length}명)`);
        }

        validMap.clear();

        // ═══════════════════════════════════════════════════════════════
        // STEP 5: NDJSON + gzip 스트리밍 저장
        //   JSON.stringify 한 방 → V8 문자열 제한(~512MB) 초과 방지
        //   유저 1명당 1줄씩 스트리밍 → 메모리 사용 최소
        // ═══════════════════════════════════════════════════════════════
        buildProgress = '[5/5] 캐시 스트리밍 저장 중...';
        console.log('\n' + buildProgress);

        const userCount = Object.keys(exportUsers).length;
        await streamSaveCache(exportUsers, totalProcessed);

        // cachedData에 로드 (이미 메모리에 있으므로 그대로 활용)
        cachedData = {
            users: exportUsers,
            totalAnalyzed: totalProcessed,
            lastUpdated: new Date().toISOString()
        };
        buildTermDF();

        const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
        const fileSizeMB = (fs.statSync(CACHE_FILE).size / 1024 / 1024).toFixed(1);
        console.log(`\n=> 완료! ${userCount}명 유저 벡터 생성 (${fileSizeMB}MB, ${elapsed}초 소요)`);
        buildProgress = '';

    } catch (e) {
        console.error("캐시 빌드 에러:", e);
        buildProgress = '에러 발생: ' + e.message;
    } finally {
        await pool.end();
        isAnalyzing = false;
    }
}

// ═══════════════════════════════════════════════════════════════
// NDJSON 스트리밍 저장/로드
//   1줄: 메타데이터 {"totalAnalyzed":N,"lastUpdated":"..."}
//   이후: 유저 1명당 1줄 {"i":"authorId","d":{벡터}}
// ═══════════════════════════════════════════════════════════════
function streamSaveCache(exportUsers, totalProcessed) {
    return new Promise((resolve, reject) => {
        const gz = zlib.createGzip({ level: 6 });
        const ws = fs.createWriteStream(CACHE_FILE);
        gz.pipe(ws);

        gz.write(JSON.stringify({ totalAnalyzed: totalProcessed, lastUpdated: new Date().toISOString() }) + '\n');

        const ids = Object.keys(exportUsers);
        let i = 0;

        function writeChunk() {
            let ok = true;
            while (i < ids.length && ok) {
                const id = ids[i];
                ok = gz.write(JSON.stringify({ i: id, d: exportUsers[id] }) + '\n');
                i++;
            }
            if (i < ids.length) {
                gz.once('drain', writeChunk);
            } else {
                gz.end();
            }
        }

        writeChunk();
        ws.on('finish', resolve);
        ws.on('error', reject);
        gz.on('error', reject);
    });
}

function streamLoadCache() {
    return new Promise((resolve, reject) => {
        const users = {};
        let meta = null;
        const gunzip = zlib.createGunzip();
        const rs = fs.createReadStream(CACHE_FILE);
        rs.pipe(gunzip);

        let buffer = '';
        gunzip.on('data', (chunk) => {
            buffer += chunk.toString('utf8');
            let nl;
            while ((nl = buffer.indexOf('\n')) !== -1) {
                const line = buffer.slice(0, nl);
                buffer = buffer.slice(nl + 1);
                if (line.length === 0) continue;
                const obj = JSON.parse(line);
                if (obj.totalAnalyzed !== undefined) {
                    meta = obj;
                } else {
                    const d = obj.d;
                    // 호환성 처리 & 메모리 최적화 변환
                    let st, sv, it, iv;
                    if (d.st) {
                        st = d.st;
                        sv = new Float32Array(d.sv);
                        it = d.it || [];
                        iv = d.iv ? new Float32Array(d.iv) : null;
                    } else { // 구버전 v 객체 포맷 변환
                        st = Object.keys(d.v);
                        sv = new Float32Array(st.length);
                        for (let j = 0; j < st.length; j++) sv[j] = d.v[st[j]];
                        
                        it = d.iv ? Object.keys(d.iv) : [];
                        iv = d.iv ? new Float32Array(it.length) : null;
                        for (let j = 0; j < it.length; j++) iv[j] = d.iv[it[j]];
                    }
                    
                    users[obj.i] = {
                        n: d.n, c: d.c,
                        r: d.r, ir: d.ir,
                        st, sv, it, iv
                    };
                }
            }
        });

        gunzip.on('end', () => {
            if (buffer.length > 0) {
                try {
                    const obj = JSON.parse(buffer);
                    if (obj.totalAnalyzed !== undefined) meta = obj;
                    else users[obj.i] = obj.d;
                } catch (e) { /* 마지막 빈 줄 무시 */ }
            }
            resolve({ users, ...(meta || { totalAnalyzed: 0, lastUpdated: new Date().toISOString() }) });
        });

        gunzip.on('error', reject);
        rs.on('error', reject);
    });
}

async function init() {
    if (fs.existsSync(CACHE_FILE)) {
        console.log("캐시 파일 로딩 중...");
        try {
            cachedData = await streamLoadCache();
            buildTermDF();
            console.log(`=> 로드 완료! (${Object.keys(cachedData.users).length}명 유저)`);
        } catch (e) {
            console.log("캐시 파일 손상 또는 구버전. 재생성합니다.", e.message);
            await buildCache();
        }
    } else {
        await buildCache();
    }
}

app.use(express.static(__dirname, { index: false }));
app.use(express.json());

// Enable CORS for API requests from the main web server (e.g., port 3000)
app.use((req, res, next) => {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});

app.get(['/', '/analyze', '/analyze.html'], (req, res) => {
    const filePath = path.join(__dirname, 'analyze.html');
    if (!fs.existsSync(filePath)) {
        return res.status(404).send(`Cannot find analyze.html at: ${filePath}`);
    }
    res.sendFile(filePath);
});

// ═══════════════════════════════════════════════════════════════
// 초고속 검색 API — 2단계 최적화
//   Phase 1: 점수만 계산 (객체 할당 0)
//   Phase 2: 상위 50명만 디테일 생성
// ═══════════════════════════════════════════════════════════════
app.get('/api/analyze', (req, res) => {
    if (!cachedData) return res.status(503).json({ error: "캐시 로딩 중입니다. 잠시 후 다시 시도하세요." });

    const target = req.query.target?.trim();
    if (!target) return res.status(400).json({ error: "target 파라미터가 필요합니다." });

    const users = cachedData.users;
    const tgt = users[target];
    if (!tgt) {
        return res.status(404).json({ error: `유저 '${target}'를 찾을 수 없습니다. (채팅 30회 이상 유저만 분석 대상)` });
    }

    const STYLE_W = Math.max(0, Math.min(1, parseFloat(req.query.ws) || 0.40));
    const INTEREST_W = Math.max(0, Math.min(1, parseFloat(req.query.wi) || 0.30));
    const RARE_W = Math.max(0, Math.min(1, parseFloat(req.query.wr) || 0.30));

    const totalUserCount = cachedData._userCount || Object.keys(users).length;
    const rareThreshold = Math.max(Math.floor(totalUserCount * 0.01), 3);

    // 타겟의 희귀 용어 세트 및 O(1) 룩업용 Map 생성
    const tgtRareSet = new Set();
    const tgtSMap = new Map();
    const tgtIMap = new Map();

    if (tgt.st) {
        for (let i = 0; i < tgt.st.length; i++) {
            const term = tgt.st[i];
            tgtSMap.set(term, tgt.sv[i]);
            if (termDF && (termDF.get(term) || 999999) <= rareThreshold) tgtRareSet.add(term);
        }
        if (tgt.it) {
            for (let i = 0; i < tgt.it.length; i++) {
                const term = tgt.it[i];
                tgtIMap.set(term, tgt.iv[i]);
                if (termDF && (termDF.get(term) || 999999) <= rareThreshold) tgtRareSet.add(term);
            }
        }
    }

    const invTgtR = tgt.r > 0 ? 1 / tgt.r : 0;
    const invTgtIR = tgt.ir > 0 ? 1 / tgt.ir : 0;
    const hasRareTerms = tgtRareSet.size > 0 && termDF;

    // ══════════════ Phase 1: 점수만 계산 (할당 최소화) ══════════════
    const TOP_K = 50;
    const heap = []; // { s: score, id: authorId }

    for (const authorId in users) {
        if (authorId === target) continue;
        const u = users[authorId];

        // -- 문체 dot product --
        let sDot = 0;
        if (u.st) {
            for (let i = 0; i < u.st.length; i++) {
                const tVal = tgtSMap.get(u.st[i]);
                if (tVal) sDot += tVal * u.sv[i];
            }
        }
        const styleSim = (sDot > 0 && u.r > 0) ? sDot * invTgtR / u.r : 0;

        // -- 관심사 dot product --
        let iDot = 0;
        if (u.it && tgtIMap.size > 0) {
            for (let i = 0; i < u.it.length; i++) {
                const tVal = tgtIMap.get(u.it[i]);
                if (tVal) iDot += tVal * u.iv[i];
            }
        }
        const interestSim = (iDot > 0 && u.ir > 0) ? iDot * invTgtIR / u.ir : 0;

        // -- 희귀 용어 점수 (경량 계산: rarity 합만) --
        let rareSim = 0;
        if (hasRareTerms && u.st) {
            let weightedSum = 0, uRareCount = 0, shared = 0;
            for (let i = 0; i < u.st.length; i++) {
                const term = u.st[i];
                const d = termDF.get(term);
                if (d !== undefined && d <= rareThreshold) {
                    uRareCount++;
                    if (tgtRareSet.has(term)) { shared++; weightedSum += 1 / Math.log2(d + 1); }
                }
            }
            if (u.it) {
                for (let i = 0; i < u.it.length; i++) {
                    const term = u.it[i];
                    const d = termDF.get(term);
                    if (d !== undefined && d <= rareThreshold) {
                        uRareCount++;
                        if (tgtRareSet.has(term)) { shared++; weightedSum += 1 / Math.log2(d + 1); }
                    }
                }
            }
            if (shared > 0) {
                const minRare = Math.min(tgtRareSet.size, uRareCount);
                if (minRare > 0) rareSim = Math.min(weightedSum / Math.sqrt(minRare), 1.0);
            }
        }

        // -- 복합 점수 --
        const hasS = styleSim > 0 ? 1 : 0;
        const hasI = interestSim > 0 ? 1 : 0;
        const hasR = rareSim > 0 ? 1 : 0;
        if (hasS + hasI + hasR === 0) continue;

        const totalW = hasS * STYLE_W + hasI * INTEREST_W + hasR * RARE_W;
        const combined = (hasS * STYLE_W * styleSim + hasI * INTEREST_W * interestSim + hasR * RARE_W * rareSim) / totalW;

        if (combined <= 0.03) continue;

        // Min-heap 유지 (크기 TOP_K)
        if (heap.length < TOP_K) {
            heap.push({ s: combined, id: authorId });
            if (heap.length === TOP_K) heapify(heap);
        } else if (combined > heap[0].s) {
            heap[0] = { s: combined, id: authorId };
            siftDown(heap, 0);
        }
    }

    // ══════════════ Phase 2: 상위 50명만 디테일 계산 ══════════════
    heap.sort((a, b) => b.s - a.s);
    const results = [];

    for (const entry of heap) {
        const authorId = entry.id;
        const u = users[authorId];

        let sDot = 0;
        const sCommon = [];
        if (u.st) {
            for (let i = 0; i < u.st.length; i++) {
                const term = u.st[i];
                const tVal = tgtSMap.get(term);
                if (tVal) {
                    const prod = tVal * u.sv[i];
                    sDot += prod;
                    sCommon.push({ t: term.replace(/_/g, ' '), s: prod });
                }
            }
        }
        const styleSim = (sDot > 0 && u.r > 0) ? sDot * invTgtR / u.r : 0;

        let iDot = 0;
        const iCommon = [];
        if (u.it) {
            for (let i = 0; i < u.it.length; i++) {
                const term = u.it[i];
                const tVal = tgtIMap.get(term);
                if (tVal) {
                    const prod = tVal * u.iv[i];
                    iDot += prod;
                    iCommon.push({ t: term.replace(/_/g, ' '), s: prod });
                }
            }
        }
        const interestSim = (iDot > 0 && u.ir > 0) ? iDot * invTgtIR / u.ir : 0;

        let rareSim = 0;
        const rareShared = [];
        if (hasRareTerms && u.st) {
            let weightedSum = 0, uRareCount = 0;
            for (let i = 0; i < u.st.length; i++) {
                const term = u.st[i];
                const d = termDF.get(term);
                if (d !== undefined && d <= rareThreshold) {
                    uRareCount++;
                    if (tgtRareSet.has(term)) {
                        const rarity = 1 / Math.log2(d + 1);
                        weightedSum += rarity;
                        rareShared.push({ t: term.replace(/_/g, ' '), rarity, df: d });
                    }
                }
            }
            if (u.it) {
                for (let i = 0; i < u.it.length; i++) {
                    const term = u.it[i];
                    const d = termDF.get(term);
                    if (d !== undefined && d <= rareThreshold) {
                        uRareCount++;
                        if (tgtRareSet.has(term)) {
                            const rarity = 1 / Math.log2(d + 1);
                            weightedSum += rarity;
                            rareShared.push({ t: term.replace(/_/g, ' '), rarity, df: d });
                        }
                    }
                }
            }
            const minRare = Math.min(tgtRareSet.size, uRareCount);
            if (minRare > 0 && weightedSum > 0) rareSim = Math.min(weightedSum / Math.sqrt(minRare), 1.0);
        }

        sCommon.sort((a, b) => b.s - a.s);
        iCommon.sort((a, b) => b.s - a.s);
        rareShared.sort((a, b) => a.df - b.df);

        const channels = [], topics = [], opinions = [];
        for (let ci = 0, len = Math.min(iCommon.length, 15); ci < len; ci++) {
            const item = iCommon[ci];
            if (item.t.startsWith('CH ')) channels.push(item.t.replace('CH ', '📺'));
            else if (item.t.startsWith('OP ')) opinions.push(item.t.replace('OP ', '').replace(' 호', '👍').replace(' 비', '👎'));
            else if (item.t.startsWith('IB ')) topics.push(item.t.replace('IB ', ''));
            else if (item.t.startsWith('I ')) topics.push(item.t.replace('I ', ''));
        }

        results.push({
            authorId,
            authorName: u.n,
            msgCount: u.c,
            similarity: entry.s,
            styleSim: Number(styleSim.toFixed(4)),
            interestSim: Number(interestSim.toFixed(4)),
            rareSim: Number(rareSim.toFixed(4)),
            topTerms: sCommon.slice(0, 7).map(x => x.t),
            topInterests: { channels: channels.slice(0, 5), topics: topics.slice(0, 8), opinions: opinions.slice(0, 5) },
            rareTerms: rareShared.slice(0, 8).map(x => ({ term: x.t, users: x.df }))
        });
    }

    res.json({
        target: { authorId: target, authorName: tgt.n, msgCount: tgt.c, rareTermCount: tgtRareSet.size },
        weights: { style: STYLE_W, interest: INTEREST_W, rare: RARE_W },
        results
    });
});

// ── Min-Heap 유틸 ──
function heapify(arr) {
    for (let i = (arr.length >> 1) - 1; i >= 0; i--) siftDown(arr, i);
}
function siftDown(arr, i) {
    const n = arr.length;
    while (true) {
        let smallest = i;
        const l = 2 * i + 1, r = 2 * i + 2;
        if (l < n && arr[l].s < arr[smallest].s) smallest = l;
        if (r < n && arr[r].s < arr[smallest].s) smallest = r;
        if (smallest === i) break;
        const tmp = arr[i]; arr[i] = arr[smallest]; arr[smallest] = tmp;
        i = smallest;
    }
}

app.post('/api/refresh', (req, res) => {
    if (isAnalyzing) return res.status(400).json({ error: "이미 분석 진행 중" });
    buildCache().catch(console.error);
    res.json({ message: "백그라운드 캐시 갱신 시작됨" });
});

app.get('/api/status', (req, res) => {
    res.json({
        isAnalyzing,
        isReady: !!cachedData,
        lastUpdated: cachedData?.lastUpdated || null,
        totalUsers: cachedData ? Object.keys(cachedData.users).length : 0,
        progress: buildProgress
    });
});

app.listen(PORT, async () => {
    console.log(`[Stylometry] http://localhost:${PORT} (${WORKER_COUNT} workers)`);
    await init();
});
