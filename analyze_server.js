const express = require('express');
const mysql = require('mysql2/promise');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

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

async function buildCache() {
    if (isAnalyzing) return;
    isAnalyzing = true;
    const t0 = Date.now();
    const CONJ_SET = new Set(['그리고','하지만','그래서','근데','그런데','왜냐면','아니면','그러니까','게다가','또','즉']);

    const pool = mysql.createPool({
        host: DB_HOST, user: DB_USER, password: DB_PASS, database: DB_SCHEMA,
        waitForConnections: true, connectionLimit: 4, queueLimit: 0
    });

    try {
        // ═══════════════════════════════════════════════════════════════
        // STEP 1: SQL GROUP BY로 유효 유저를 한방에 추출 (수초)
        //   기존: 900만건 풀스캔 → 새 방식: DB가 알아서 집계해서 결과만 보내줌
        // ═══════════════════════════════════════════════════════════════
        buildProgress = '[1/4] DB에서 헤비 유저 추출 중 (SQL GROUP BY)...';
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
        console.log(`  => ${validMap.size.toLocaleString()}명 추출 완료 (${((Date.now() - t0)/1000).toFixed(1)}초 경과)`);

        // ═══════════════════════════════════════════════════════════════
        // STEP 2: nid→authorId 매핑 테이블을 메모리에 구축 (수초)
        //   이 매핑만 있으면 2차 스캔에서 JOIN이 필요 없어짐
        // ═══════════════════════════════════════════════════════════════
        buildProgress = '[2/4] 유저 매핑 테이블 구축 중...';
        console.log('\n' + buildProgress);

        const [uidRows] = await pool.execute('SELECT uid, authorId FROM youtube_users');
        const uidToAuthorId = new Map();
        for (const r of uidRows) uidToAuthorId.set(r.uid, r.authorId);

        const [nidRows] = await pool.execute('SELECT nid, uid, author FROM youtube_user_names');
        const nidLookup = new Map(); // nid -> { authorId, author }
        for (const r of nidRows) {
            const authorId = uidToAuthorId.get(r.uid);
            if (authorId && validMap.has(authorId)) {
                nidLookup.set(r.nid, { authorId, author: r.author });
            }
        }
        uidToAuthorId.clear();
        console.log(`  => 매핑 완료! (${nidLookup.size.toLocaleString()}개 프로필)`);

        // ═══════════════════════════════════════════════════════════════
        // STEP 3: youtube_chat3 단독 스캔 (JOIN 제로! PK 스캔만!)
        //   기존: 4개 테이블 JOIN → 새 방식: chat3 하나만 읽고 메모리 매핑으로 필터
        // ═══════════════════════════════════════════════════════════════
        buildProgress = '[3/4] 형태소 분석 중 (초고속 단독 스캔)...';
        console.log('\n' + buildProgress);

        const userTerms = new Map();
        let lastId = 0;
        const BATCH_SIZE = 100000; // JOIN 없으니 배치 크기 2배로
        let totalProcessed = 0;

        while (true) {
            const [rows] = await pool.execute(
                `SELECT id, nid, msgdata FROM youtube_chat3 WHERE id > ? ORDER BY id ASC LIMIT ${BATCH_SIZE}`,
                [lastId]
            );

            if (rows.length === 0) break;

            for (const row of rows) {
                lastId = row.id;

                const lookup = nidLookup.get(row.nid);
                if (!lookup) continue; // 유효하지 않은 유저 → 스킵

                if (!row.msgdata || row.msgdata.trim() === '') continue;

                // author 이름 갱신
                const info = validMap.get(lookup.authorId);
                if (info) info.authorName = lookup.author;

                let terms = userTerms.get(lookup.authorId);
                if (!terms) {
                    terms = new Map();
                    userTerms.set(lookup.authorId, terms);
                }

                const tokens = row.msgdata.split(/\s+/);
                const msgLen = tokens.length;

                // ── 기존: 형태소 Unigram + Bigram ──
                for (let i = 0; i < tokens.length; i++) {
                    const t = tokens[i];
                    if (!t || t.length > 20) continue;
                    terms.set(t, (terms.get(t) || 0) + 1);
                    if (i < tokens.length - 1 && tokens[i + 1] && tokens[i + 1].length <= 20) {
                        const bg = t + '_' + tokens[i + 1];
                        terms.set(bg, (terms.get(bg) || 0) + 1);
                    }
                }

                // ── 신규 1: 문장 끝 패턴 (한국어 어미는 동일인 판별의 핵심) ──
                // "~임", "~ㅇㅇ", "~ㄴㄷ", "~했는데" 등 개인 고유 말투
                if (tokens.length > 0) {
                    const lastTok = tokens[tokens.length - 1];
                    if (lastTok && lastTok.length <= 10) {
                        terms.set('E_' + lastTok, (terms.get('E_' + lastTok) || 0) + 1);
                    }
                    // 마지막 2토큰 연결 (어미 패턴 강화)
                    if (tokens.length >= 2) {
                        const prev = tokens[tokens.length - 2];
                        if (prev && prev.length <= 10) {
                            terms.set('E2_' + prev + '_' + lastTok, (terms.get('E2_' + prev + '_' + lastTok) || 0) + 1);
                        }
                    }
                }

                // ── 신규 2: ㅋㅎㅠ 반복 패턴 (ㅋㅋ vs ㅋㅋㅋ vs ㅋㅋㅋㅋ 구분) ──
                const raw = row.msgdata;
                const repMatch = raw.match(/([ㅋㅎㅠㅜ])\1+/g);
                if (repMatch) {
                    for (const pat of repMatch) {
                        const key = 'R_' + pat[0] + Math.min(pat.length, 8);
                        terms.set(key, (terms.get(key) || 0) + 1);
                    }
                }

                // ── 신규 3: 글자 바이그램 (오타·축약어 습관 감지) ──
                for (const t of tokens) {
                    if (!t || t.length < 2 || t.length > 6) continue;
                    for (let ci = 0; ci < t.length - 1; ci++) {
                        const cb = 'C_' + t[ci] + t[ci + 1];
                        terms.set(cb, (terms.get(cb) || 0) + 1);
                    }
                }

                // ── 신규 4: 메시지 길이 버킷 (장문 vs 단문 습관) ──
                const lenBucket = msgLen <= 2 ? 'L_짧' : msgLen <= 6 ? 'L_중' : msgLen <= 15 ? 'L_긴' : 'L_장문';
                terms.set(lenBucket, (terms.get(lenBucket) || 0) + 1);

                // ── 신규 5: 존댓말/반말 비율 ──
                if (tokens.length > 0) {
                    const last = tokens[tokens.length - 1];
                    if (last) {
                        if (/[요니세습]$/.test(last) || /니다$/.test(last)) {
                            terms.set('S_존댓말', (terms.get('S_존댓말') || 0) + 1);
                        } else if (/[음임ㅇㅁ]$/.test(last) || /ㄴㄷ$/.test(last) || /[어아야지]$/.test(last)) {
                            terms.set('S_반말', (terms.get('S_반말') || 0) + 1);
                        }
                    }
                }

                // ── 신규 6: 접속사 사용 패턴 ──
                for (const t of tokens) {
                    if (CONJ_SET.has(t)) {
                        terms.set('J_' + t, (terms.get('J_' + t) || 0) + 1);
                    }
                }

                // ── 신규 7: 특수문자·이모티콘 빈도 ──
                const rawMsg = row.msgdata;
                const qCount = (rawMsg.match(/\?/g) || []).length;
                const exCount = (rawMsg.match(/!/g) || []).length;
                const dotCount = (rawMsg.match(/\.{2,}/g) || []).length;
                if (qCount) terms.set('P_?', (terms.get('P_?') || 0) + qCount);
                if (exCount) terms.set('P_!', (terms.get('P_!') || 0) + exCount);
                if (dotCount) terms.set('P_..', (terms.get('P_..') || 0) + dotCount);

                // ── 신규 8: 숫자 표기 습관 (아라비아 vs 한글) ──
                if (/\d/.test(rawMsg)) terms.set('N_아라비아', (terms.get('N_아라비아') || 0) + 1);
                if (/[일이삼사오육칠팔구십백천만억]/.test(rawMsg)) terms.set('N_한글', (terms.get('N_한글') || 0) + 1);

                // ── 신규 9: 자모 축약어 감지 (ㄱㄱ, ㅇㅇ, ㄹㅇ, ㅎㅇ 등) ──
                const jamoMatch = rawMsg.match(/[ㄱ-ㅎ]{2,}/g);
                if (jamoMatch) {
                    for (const jm of jamoMatch) {
                        if (jm.length <= 4) {
                            terms.set('A_' + jm, (terms.get('A_' + jm) || 0) + 1);
                        }
                    }
                }

                // ★ OOM 방지: 유저당 단어가 1200개를 넘으면 상위 700개만 보존
                if (terms.size > 1200) {
                    const sorted = [...terms.entries()].sort((a, b) => b[1] - a[1]);
                    terms.clear();
                    for (let j = 0; j < 700; j++) terms.set(sorted[j][0], sorted[j][1]);
                }
            }
            totalProcessed += rows.length;
            buildProgress = `[3/5] 형태소 분석: ${totalProcessed.toLocaleString()}건`;
            process.stdout.write(`\r  ... ${totalProcessed.toLocaleString()} 건 처리됨`);
        }
        nidLookup.clear(); // 매핑 테이블 해제
        console.log(`\n  => 형태소 분석 완료 (${((Date.now() - t0)/1000).toFixed(1)}초 경과)`);

        // ═══════════════════════════════════════════════════════════════
        // STEP 4: TF-IDF 벡터화 (유저당 상위 150 특징만 보존 → 메모리 & 속도)
        // ═══════════════════════════════════════════════════════════════
        buildProgress = '[4/5] TF-IDF 벡터 생성 중...';
        console.log('\n' + buildProgress);

        const dfMap = new Map();
        const totalUsers = userTerms.size;

        for (const terms of userTerms.values()) {
            for (const term of terms.keys()) {
                dfMap.set(term, (dfMap.get(term) || 0) + 1);
            }
        }

        // DF < 2인 단어 제거 (1명만 쓴 단어 = 노이즈)
        for (const [term, df] of dfMap.entries()) {
            if (df < 2) dfMap.delete(term);
        }

        const MAX_FEATURES = 300; // 유저당 상위 300개 특징 보존 (정확도 향상)
        const exportUsers = {};

        // ★ OOM 방지: 유저 하나 처리할 때마다 즉시 삭제하여 메모리 점진 해제
        for (const [authorId, terms] of userTerms) {
            const info = validMap.get(authorId);
            if (!info) { userTerms.delete(authorId); continue; }

            const tfidfArr = [];
            for (const [term, count] of terms.entries()) {
                const df = dfMap.get(term);
                if (!df) continue;
                const idf = Math.log(totalUsers / df) + 1;
                const tf = 1 + Math.log(count);
                tfidfArr.push({ term, val: tf * idf });
            }

            // 처리 완료 → 즉시 메모리에서 제거
            userTerms.delete(authorId);

            tfidfArr.sort((a, b) => b.val - a.val);
            const topN = tfidfArr.slice(0, MAX_FEATURES);

            let normSq = 0;
            const tfidfObj = {};
            for (const item of topN) {
                const v = Number(item.val.toFixed(4));
                tfidfObj[item.term] = v;
                normSq += v * v;
            }

            exportUsers[authorId] = {
                n: info.authorName,
                c: info.msgCount,
                r: Number(Math.sqrt(normSq).toFixed(4)),
                v: tfidfObj
            };
        }

        // 메모리 해제
        userTerms.clear();
        dfMap.clear();
        validMap.clear();

        // ═══════════════════════════════════════════════════════════════
        // STEP 4: gzip 압축 캐시 저장
        // ═══════════════════════════════════════════════════════════════
        buildProgress = '[5/5] 캐시 압축 저장 중...';
        console.log('\n' + buildProgress);

        const exportData = {
            users: exportUsers,
            totalAnalyzed: totalProcessed,
            lastUpdated: new Date().toISOString()
        };

        const jsonStr = JSON.stringify(exportData);
        const compressed = zlib.gzipSync(jsonStr);
        fs.writeFileSync(CACHE_FILE, compressed);
        cachedData = exportData;

        const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
        const sizeMB = (compressed.length / 1024 / 1024).toFixed(1);
        console.log(`\n=> 완료! ${Object.keys(exportUsers).length}명 유저 벡터 생성 (${sizeMB}MB, ${elapsed}초 소요)`);
        buildProgress = '';

    } catch (e) {
        console.error("캐시 빌드 에러:", e);
        buildProgress = '에러 발생: ' + e.message;
    } finally {
        await pool.end();
        isAnalyzing = false;
    }
}

async function init() {
    if (fs.existsSync(CACHE_FILE)) {
        console.log("캐시 파일 로딩 중...");
        try {
            const compressed = fs.readFileSync(CACHE_FILE);
            const json = zlib.gunzipSync(compressed).toString('utf8');
            cachedData = JSON.parse(json);
            console.log(`=> 로드 완료! (${Object.keys(cachedData.users).length}명 유저)`);
        } catch (e) {
            console.log("캐시 파일 손상. 재생성합니다.");
            await buildCache();
        }
    } else {
        await buildCache();
    }
}

app.use(express.static(__dirname, { index: false }));
app.use(express.json());

app.get(['/', '/analyze', '/analyze.html'], (req, res) => {
    const filePath = path.join(__dirname, 'analyze.html');
    if (!fs.existsSync(filePath)) {
        return res.status(404).send(`Cannot find analyze.html at: ${filePath}`);
    }
    res.sendFile(filePath);
});

// ═══════════════════════════════════════════════════════════════
// 초고속 검색 API (인메모리 코사인 유사도)
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

    const tgtVec = tgt.v;
    const tgtNorm = tgt.r;
    const tgtKeys = Object.keys(tgtVec);
    const results = [];

    for (const authorId in users) {
        if (authorId === target) continue;
        const u = users[authorId];
        const uVec = u.v;

        let dot = 0;
        const common = [];

        for (let i = 0; i < tgtKeys.length; i++) {
            const term = tgtKeys[i];
            const uVal = uVec[term];
            if (uVal) {
                const prod = tgtVec[term] * uVal;
                dot += prod;
                common.push({ t: term.replace(/_/g, ' '), s: prod });
            }
        }

        if (dot > 0 && tgtNorm > 0 && u.r > 0) {
            const sim = dot / (tgtNorm * u.r);
            if (sim > 0.1) {
                common.sort((a, b) => b.s - a.s);
                results.push({
                    authorId,
                    authorName: u.n,
                    msgCount: u.c,
                    similarity: sim,
                    topTerms: common.slice(0, 7).map(x => x.t)
                });
            }
        }
    }

    results.sort((a, b) => b.similarity - a.similarity);

    res.json({
        target: { authorId: target, authorName: tgt.n, msgCount: tgt.c },
        results: results.slice(0, 30)
    });
});

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
    console.log(`[Stylometry] http://localhost:${PORT}`);
    await init();
});
