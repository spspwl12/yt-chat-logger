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
let termDF = null; // term → 사용 유저 수 (희귀 용어 판별용)

function buildTermDF() {
    if (!cachedData) return;
    const df = new Map();
    for (const u of Object.values(cachedData.users)) {
        for (const term in u.v) df.set(term, (df.get(term) || 0) + 1);
        if (u.iv) for (const term in u.iv) df.set(term, (df.get(term) || 0) + 1);
    }
    termDF = df;
    console.log(`[TermDF] ${df.size.toLocaleString()}개 용어 인덱스 구축 완료`);
}

// ═══════════════════════════════════════════════════════════════
// 관심사 분석용 상수
// ═══════════════════════════════════════════════════════════════
const PARTICLES = new Set([
    '이','가','은','는','을','를','에','의','와','과','로','으로',
    '도','만','부터','까지','야','아','요','여','네','죠','고','며',
    '면','서','께','한테','에게','에서','라','든','랑','이랑','하'
]);
const OPINION_POS = new Set([
    '좋아','좋다','좋음','좋','최고','짱','맛있','사랑','추천',
    '좋아해','좋아하','대박','쩔어','쩔','미쳤','레전드','인정',
    '존맛','개맛','킹','갓','존좋','개좋','잘하','잘한'
]);
const OPINION_NEG = new Set([
    '싫어','싫다','싫음','싫','별로','구려','구림','노잼',
    '못먹','못해','최악','쓰레기','안좋','꼴불견','역겨','역겹',
    '구데기','노맛','개별로','존별로','못하','못한'
]);
const SKIP_WORDS = new Set([
    '그래','네','응','아니','뭐','거','것','수','때','안','못',
    '더','좀','잘','왜','진짜','ㄹㅇ','아','어','오','이거','저거',
    '그거','여기','거기','저기','우리','나','너','제','내'
]);

function isContentWord(token) {
    if (!token || token.length < 2 || token.length > 10) return false;
    if (PARTICLES.has(token) || SKIP_WORDS.has(token)) return false;
    if (/^[ㄱ-ㅎㅏ-ㅣ]+$/.test(token)) return false;
    if (/^\d+$/.test(token)) return false;
    if (!/[가-힣]/.test(token)) return false;
    return true;
}

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
        console.log(`  => ${validMap.size.toLocaleString()}명 추출 완료 (${((Date.now() - t0)/1000).toFixed(1)}초 경과)`);

        // ═══════════════════════════════════════════════════════════════
        // STEP 2: nid→authorId 매핑 테이블을 메모리에 구축
        // ═══════════════════════════════════════════════════════════════
        buildProgress = '[2/5] 유저 매핑 테이블 구축 중...';
        console.log('\n' + buildProgress);

        const [uidRows] = await pool.execute('SELECT uid, authorId FROM youtube_users');
        const uidToAuthorId = new Map();
        for (const r of uidRows) uidToAuthorId.set(r.uid, r.authorId);

        const [nidRows] = await pool.execute('SELECT nid, uid, author FROM youtube_user_names');
        const nidLookup = new Map();
        for (const r of nidRows) {
            const authorId = uidToAuthorId.get(r.uid);
            if (authorId && validMap.has(authorId)) {
                nidLookup.set(r.nid, { authorId, author: r.author });
            }
        }
        uidToAuthorId.clear();
        console.log(`  => 매핑 완료! (${nidLookup.size.toLocaleString()}개 프로필)`);

        // ═══════════════════════════════════════════════════════════════
        // STEP 3: youtube_chat3 스캔 (문체 + 관심사 동시 분석)
        // ═══════════════════════════════════════════════════════════════
        buildProgress = '[3/5] 문체 + 관심사 분석 중...';
        console.log('\n' + buildProgress);

        const userTerms = new Map();
        const userInterests = new Map();
        let lastId = 0;
        const BATCH_SIZE = 100000;
        let totalProcessed = 0;

        while (true) {
            const [rows] = await pool.execute(
                `SELECT id, nid, channel, msgdata FROM youtube_chat3 WHERE id > ? ORDER BY id ASC LIMIT ${BATCH_SIZE}`,
                [lastId]
            );

            if (rows.length === 0) break;

            for (const row of rows) {
                lastId = row.id;
                const lookup = nidLookup.get(row.nid);
                if (!lookup) continue;
                if (!row.msgdata || row.msgdata.trim() === '') continue;

                const info = validMap.get(lookup.authorId);
                if (info) info.authorName = lookup.author;

                let terms = userTerms.get(lookup.authorId);
                if (!terms) { terms = new Map(); userTerms.set(lookup.authorId, terms); }

                let interests = userInterests.get(lookup.authorId);
                if (!interests) { interests = new Map(); userInterests.set(lookup.authorId, interests); }

                const tokens = row.msgdata.split(/\s+/);
                const msgLen = tokens.length;

                // ── 문체: Unigram + Bigram ──
                for (let i = 0; i < tokens.length; i++) {
                    const t = tokens[i];
                    if (!t || t.length > 20) continue;
                    terms.set(t, (terms.get(t) || 0) + 1);
                    if (i < tokens.length - 1 && tokens[i + 1] && tokens[i + 1].length <= 20) {
                        const bg = t + '_' + tokens[i + 1];
                        terms.set(bg, (terms.get(bg) || 0) + 1);
                    }
                }

                // ── 문체: 문장 끝 패턴 ──
                if (tokens.length > 0) {
                    const lastTok = tokens[tokens.length - 1];
                    if (lastTok && lastTok.length <= 10) {
                        terms.set('E_' + lastTok, (terms.get('E_' + lastTok) || 0) + 1);
                    }
                    if (tokens.length >= 2) {
                        const prev = tokens[tokens.length - 2];
                        if (prev && prev.length <= 10) {
                            terms.set('E2_' + prev + '_' + lastTok, (terms.get('E2_' + prev + '_' + lastTok) || 0) + 1);
                        }
                    }
                }

                // ── 문체: ㅋㅎㅠ 반복 패턴 ──
                const raw = row.msgdata;
                const repMatch = raw.match(/([ㅋㅎㅠㅜ])\1+/g);
                if (repMatch) {
                    for (const pat of repMatch) {
                        const key = 'R_' + pat[0] + Math.min(pat.length, 8);
                        terms.set(key, (terms.get(key) || 0) + 1);
                    }
                }

                // ── 문체: 글자 바이그램 ──
                for (const t of tokens) {
                    if (!t || t.length < 2 || t.length > 6) continue;
                    for (let ci = 0; ci < t.length - 1; ci++) {
                        const cb = 'C_' + t[ci] + t[ci + 1];
                        terms.set(cb, (terms.get(cb) || 0) + 1);
                    }
                }

                // ── 문체: 메시지 길이 버킷 ──
                const lenBucket = msgLen <= 2 ? 'L_짧' : msgLen <= 6 ? 'L_중' : msgLen <= 15 ? 'L_긴' : 'L_장문';
                terms.set(lenBucket, (terms.get(lenBucket) || 0) + 1);

                // ── 문체: 존댓말/반말 비율 ──
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

                // ── 문체: 접속사 사용 패턴 ──
                for (const t of tokens) {
                    if (CONJ_SET.has(t)) terms.set('J_' + t, (terms.get('J_' + t) || 0) + 1);
                }

                // ── 문체: 특수문자·이모티콘 빈도 ──
                const qCount = (raw.match(/\?/g) || []).length;
                const exCount = (raw.match(/!/g) || []).length;
                const dotCount = (raw.match(/\.{2,}/g) || []).length;
                if (qCount) terms.set('P_?', (terms.get('P_?') || 0) + qCount);
                if (exCount) terms.set('P_!', (terms.get('P_!') || 0) + exCount);
                if (dotCount) terms.set('P_..', (terms.get('P_..') || 0) + dotCount);

                // ── 문체: 숫자 표기 습관 ──
                if (/\d/.test(raw)) terms.set('N_아라비아', (terms.get('N_아라비아') || 0) + 1);
                if (/[일이삼사오육칠팔구십백천만억]/.test(raw)) terms.set('N_한글', (terms.get('N_한글') || 0) + 1);

                // ── 문체: 자모 축약어 감지 ──
                const jamoMatch = raw.match(/[ㄱ-ㅎ]{2,}/g);
                if (jamoMatch) {
                    for (const jm of jamoMatch) {
                        if (jm.length <= 4) terms.set('A_' + jm, (terms.get('A_' + jm) || 0) + 1);
                    }
                }

                // ════════════════════════════════════════════════════════
                // 관심사(Interest) 특징 추출 (신규)
                // ════════════════════════════════════════════════════════

                // ── 관심사 1: 채널 참여 패턴 (어떤 방송을 시청하는지) ──
                if (row.channel) {
                    interests.set('CH_' + row.channel, (interests.get('CH_' + row.channel) || 0) + 1);
                }

                // ── 관심사 2: 콘텐츠 키워드 (자주 언급하는 명사/주제어) ──
                for (const t of tokens) {
                    if (isContentWord(t)) {
                        interests.set('I_' + t, (interests.get('I_' + t) || 0) + 1);
                    }
                }

                // ── 관심사 3: 선호도 패턴 ("X 좋아", "X 싫어" 등) ──
                for (let i = 0; i < tokens.length - 1; i++) {
                    const t = tokens[i];
                    const next = tokens[i + 1];
                    if (!t || !next) continue;
                    if (isContentWord(t)) {
                        if (OPINION_POS.has(next)) {
                            interests.set('OP_' + t + '_호', (interests.get('OP_' + t + '_호') || 0) + 1);
                        } else if (OPINION_NEG.has(next)) {
                            interests.set('OP_' + t + '_비', (interests.get('OP_' + t + '_비') || 0) + 1);
                        }
                    }
                }

                // ── 관심사 4: 콘텐츠 바이그램 (주제 맥락) ──
                for (let i = 0; i < tokens.length - 1; i++) {
                    if (isContentWord(tokens[i]) && isContentWord(tokens[i + 1])) {
                        const ib = 'IB_' + tokens[i] + '_' + tokens[i + 1];
                        interests.set(ib, (interests.get(ib) || 0) + 1);
                    }
                }

                // ★ OOM 방지: 문체
                if (terms.size > 1200) {
                    const sorted = [...terms.entries()].sort((a, b) => b[1] - a[1]);
                    terms.clear();
                    for (let j = 0; j < 700; j++) terms.set(sorted[j][0], sorted[j][1]);
                }
                // ★ OOM 방지: 관심사
                if (interests.size > 800) {
                    const sorted = [...interests.entries()].sort((a, b) => b[1] - a[1]);
                    interests.clear();
                    for (let j = 0; j < 400; j++) interests.set(sorted[j][0], sorted[j][1]);
                }
            }
            totalProcessed += rows.length;
            buildProgress = `[3/5] 문체+관심사 분석: ${totalProcessed.toLocaleString()}건`;
            process.stdout.write(`\r  ... ${totalProcessed.toLocaleString()} 건 처리됨`);
        }
        nidLookup.clear();
        console.log(`\n  => 문체+관심사 분석 완료 (${((Date.now() - t0)/1000).toFixed(1)}초 경과)`);

        // ═══════════════════════════════════════════════════════════════
        // STEP 4: TF-IDF 벡터화 (문체 + 관심사 각각)
        // ═══════════════════════════════════════════════════════════════
        buildProgress = '[4/5] TF-IDF 벡터 생성 중 (문체+관심사)...';
        console.log('\n' + buildProgress);

        // -- 문체 DF --
        const dfStyle = new Map();
        const totalUsers = userTerms.size;
        for (const terms of userTerms.values()) {
            for (const term of terms.keys()) dfStyle.set(term, (dfStyle.get(term) || 0) + 1);
        }
        for (const [term, df] of dfStyle.entries()) { if (df < 2) dfStyle.delete(term); }

        // -- 관심사 DF --
        const dfInterest = new Map();
        for (const interests of userInterests.values()) {
            for (const term of interests.keys()) dfInterest.set(term, (dfInterest.get(term) || 0) + 1);
        }
        for (const [term, df] of dfInterest.entries()) { if (df < 2) dfInterest.delete(term); }

        const MAX_STYLE = 300;
        const MAX_INTEREST = 200;
        const exportUsers = {};

        for (const [authorId, terms] of userTerms) {
            const info = validMap.get(authorId);
            if (!info) { userTerms.delete(authorId); userInterests.delete(authorId); continue; }

            // -- 문체 TF-IDF --
            const styleArr = [];
            for (const [term, count] of terms.entries()) {
                const df = dfStyle.get(term);
                if (!df) continue;
                styleArr.push({ term, val: (1 + Math.log(count)) * (Math.log(totalUsers / df) + 1) });
            }
            userTerms.delete(authorId);
            styleArr.sort((a, b) => b.val - a.val);
            const topStyle = styleArr.slice(0, MAX_STYLE);
            let sNormSq = 0;
            const sObj = {};
            for (const item of topStyle) {
                const v = Number(item.val.toFixed(4));
                sObj[item.term] = v;
                sNormSq += v * v;
            }

            // -- 관심사 TF-IDF --
            const interests = userInterests.get(authorId);
            let iObj = {};
            let iNormSq = 0;
            if (interests) {
                const intArr = [];
                for (const [term, count] of interests.entries()) {
                    const df = dfInterest.get(term);
                    if (!df) continue;
                    intArr.push({ term, val: (1 + Math.log(count)) * (Math.log(totalUsers / df) + 1) });
                }
                userInterests.delete(authorId);
                intArr.sort((a, b) => b.val - a.val);
                const topInt = intArr.slice(0, MAX_INTEREST);
                for (const item of topInt) {
                    const v = Number(item.val.toFixed(4));
                    iObj[item.term] = v;
                    iNormSq += v * v;
                }
            }

            exportUsers[authorId] = {
                n: info.authorName,
                c: info.msgCount,
                r: Number(Math.sqrt(sNormSq).toFixed(4)),
                v: sObj,
                ir: Number(Math.sqrt(iNormSq).toFixed(4)),
                iv: iObj
            };
        }

        userTerms.clear(); userInterests.clear();
        dfStyle.clear(); dfInterest.clear(); validMap.clear();

        // ═══════════════════════════════════════════════════════════════
        // STEP 5: gzip 압축 캐시 저장
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
        buildTermDF();

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
            buildTermDF();
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
// 초고속 검색 API (문체 + 관심사 + 희귀용어 복합 유사도)
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

    // 희귀 용어 임계값: 전체 유저의 1% 이하만 사용하는 단어를 '희귀'로 판정
    const totalUserCount = Object.keys(users).length;
    const rareThreshold = Math.max(Math.floor(totalUserCount * 0.01), 3);

    // 타겟의 희귀 용어 목록 (문체+관심사 합산)
    const tgtRareSet = new Set();
    if (termDF) {
        for (const term in tgt.v) {
            if ((termDF.get(term) || 999999) <= rareThreshold) tgtRareSet.add(term);
        }
        const tgtIv = tgt.iv || {};
        for (const term in tgtIv) {
            if ((termDF.get(term) || 999999) <= rareThreshold) tgtRareSet.add(term);
        }
    }

    const tgtSKeys = Object.keys(tgt.v);
    const tgtIKeys = Object.keys(tgt.iv || {});
    const results = [];

    for (const authorId in users) {
        if (authorId === target) continue;
        const u = users[authorId];

        // -- 문체 코사인 유사도 --
        let sDot = 0;
        const sCommon = [];
        for (let i = 0; i < tgtSKeys.length; i++) {
            const term = tgtSKeys[i];
            const uVal = u.v[term];
            if (uVal) {
                const prod = tgt.v[term] * uVal;
                sDot += prod;
                sCommon.push({ t: term.replace(/_/g, ' '), s: prod });
            }
        }
        const styleSim = (sDot > 0 && tgt.r > 0 && u.r > 0) ? sDot / (tgt.r * u.r) : 0;

        // -- 관심사 코사인 유사도 --
        let iDot = 0;
        const iCommon = [];
        const uIv = u.iv || {};
        const tgtIv = tgt.iv || {};
        for (let i = 0; i < tgtIKeys.length; i++) {
            const term = tgtIKeys[i];
            const uVal = uIv[term];
            if (uVal) {
                const prod = tgtIv[term] * uVal;
                iDot += prod;
                iCommon.push({ t: term.replace(/_/g, ' '), s: prod });
            }
        }
        const interestSim = (iDot > 0 && tgt.ir > 0 && u.ir > 0) ? iDot / (tgt.ir * u.ir) : 0;

        // -- 희귀 용어 공유 점수 (Jaccard-like) --
        let rareSim = 0;
        const rareShared = [];
        if (tgtRareSet.size > 0 && termDF) {
            let shared = 0;
            // 상대방의 희귀 용어 수
            let uRareCount = 0;
            for (const term in u.v) {
                if ((termDF.get(term) || 999999) <= rareThreshold) {
                    uRareCount++;
                    if (tgtRareSet.has(term)) {
                        shared++;
                        const df = termDF.get(term) || 1;
                        // DF가 낮을수록 가중치 높게 (2명만 쓰면 매우 강력한 시그널)
                        const rarity = 1 / Math.log2(df + 1);
                        rareShared.push({ t: term.replace(/_/g, ' '), rarity, df });
                    }
                }
            }
            if (u.iv) {
                for (const term in u.iv) {
                    if ((termDF.get(term) || 999999) <= rareThreshold) {
                        uRareCount++;
                        if (tgtRareSet.has(term)) {
                            shared++;
                            const df = termDF.get(term) || 1;
                            const rarity = 1 / Math.log2(df + 1);
                            rareShared.push({ t: term.replace(/_/g, ' '), rarity, df });
                        }
                    }
                }
            }
            const minRare = Math.min(tgtRareSet.size, uRareCount);
            if (minRare > 0 && shared > 0) {
                // 가중 Jaccard: 희귀도 가중합 / 최소 희귀 용어 수
                const weightedSum = rareShared.reduce((s, x) => s + x.rarity, 0);
                rareSim = Math.min(weightedSum / Math.sqrt(minRare), 1.0);
            }
        }

        // -- 복합 점수 --
        const hasStyle = styleSim > 0;
        const hasInterest = interestSim > 0;
        const hasRare = rareSim > 0;
        const activeCount = (hasStyle ? 1 : 0) + (hasInterest ? 1 : 0) + (hasRare ? 1 : 0);
        if (activeCount === 0) continue;

        // 활성 척도의 가중치를 재분배
        let totalW = (hasStyle ? STYLE_W : 0) + (hasInterest ? INTEREST_W : 0) + (hasRare ? RARE_W : 0);
        const combined = (
            (hasStyle ? STYLE_W * styleSim : 0) +
            (hasInterest ? INTEREST_W * interestSim : 0) +
            (hasRare ? RARE_W * rareSim : 0)
        ) / totalW;

        if (combined > 0.03) {
            sCommon.sort((a, b) => b.s - a.s);
            iCommon.sort((a, b) => b.s - a.s);
            rareShared.sort((a, b) => a.df - b.df); // DF 낮은 순 (가장 희귀한 것 먼저)

            // 관심사 공통항목을 카테고리별로 분류
            const channels = [];
            const topics = [];
            const opinions = [];
            for (const item of iCommon.slice(0, 15)) {
                if (item.t.startsWith('CH ')) channels.push(item.t.replace('CH ', '📺'));
                else if (item.t.startsWith('OP ')) opinions.push(item.t.replace('OP ', '').replace(' 호', '👍').replace(' 비', '👎'));
                else if (item.t.startsWith('IB ')) topics.push(item.t.replace('IB ', ''));
                else if (item.t.startsWith('I ')) topics.push(item.t.replace('I ', ''));
            }

            results.push({
                authorId,
                authorName: u.n,
                msgCount: u.c,
                similarity: combined,
                styleSim: Number(styleSim.toFixed(4)),
                interestSim: Number(interestSim.toFixed(4)),
                rareSim: Number(rareSim.toFixed(4)),
                topTerms: sCommon.slice(0, 7).map(x => x.t),
                topInterests: { channels: channels.slice(0, 5), topics: topics.slice(0, 8), opinions: opinions.slice(0, 5) },
                rareTerms: rareShared.slice(0, 8).map(x => ({ term: x.t, users: x.df }))
            });
        }
    }

    results.sort((a, b) => b.similarity - a.similarity);

    res.json({
        target: { authorId: target, authorName: tgt.n, msgCount: tgt.c, rareTermCount: tgtRareSet.size },
        weights: { style: STYLE_W, interest: INTEREST_W, rare: RARE_W },
        results: results.slice(0, 50)
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
