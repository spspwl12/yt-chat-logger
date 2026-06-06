// ═══════════════════════════════════════════════════════════════
// analyze_worker.js — CPU-bound 메시지 분석 워커 스레드
// 메모리 최적화: 유저 파티셔닝 + 워커 내 TF-IDF 계산
// ═══════════════════════════════════════════════════════════════
const { parentPort } = require('worker_threads');

const PARTICLES = new Set(['이', '가', '은', '는', '을', '를', '에', '의', '와', '과', '로', '으로', '도', '만', '부터', '까지', '야', '아', '요', '여', '네', '죠', '고', '며', '면', '서', '께', '한테', '에게', '에서', '라', '든', '랑', '이랑', '하']);
const OPINION_POS = new Set(['좋아', '좋다', '좋음', '좋', '최고', '짱', '맛있', '사랑', '추천', '좋아해', '좋아하', '대박', '쩔어', '쩔', '미쳤', '레전드', '인정', '존맛', '개맛', '킹', '갓', '존좋', '개좋', '잘하', '잘한']);
const OPINION_NEG = new Set(['싫어', '싫다', '싫음', '싫', '별로', '구려', '구림', '노잼', '못먹', '못해', '최악', '쓰레기', '안좋', '꼴불견', '역겨', '역겹', '구데기', '노맛', '개별로', '존별로', '못하', '못한']);
const SKIP_WORDS = new Set(['그래', '네', '응', '아니', '뭐', '거', '것', '수', '때', '안', '못', '더', '좀', '잘', '왜', '진짜', 'ㄹㅇ', '아', '어', '오', '이거', '저거', '그거', '여기', '거기', '저기', '우리', '나', '너', '제', '내']);
const CONJ_SET = new Set(['그리고', '하지만', '그래서', '근데', '그런데', '왜냐면', '아니면', '그러니까', '게다가', '또', '즉']);

const RE_REP = /([ㅋㅎㅠㅜ])\1+/g;
const RE_JAMO_ONLY = /^[ㄱ-ㅎㅏ-ㅣ]+$/;
const RE_DIGIT_ONLY = /^\d+$/;
const RE_HAS_HANGUL = /[가-힣]/;
const RE_FORMAL = /[요니세습]$/;
const RE_FORMAL2 = /니다$/;
const RE_CASUAL = /[음임ㅇㅁ]$/;
const RE_CASUAL2 = /ㄴㄷ$/;
const RE_CASUAL3 = /[어아야지]$/;
const RE_HAS_DIGIT = /\d/;
const RE_HAN_NUM = /[일이삼사오육칠팔구십백천만억]/;
const RE_JAMO_SEQ = /[ㄱ-ㅎ]{2,}/g;
const RE_DOTS = /\.{2,}/g;
const RE_WS = /\s+/;

function isContentWord(t) {
    if (!t || t.length < 2 || t.length > 10) return false;
    if (PARTICLES.has(t) || SKIP_WORDS.has(t)) return false;
    if (RE_JAMO_ONLY.test(t)) return false;
    if (RE_DIGIT_ONLY.test(t)) return false;
    if (!RE_HAS_HANGUL.test(t)) return false;
    return true;
}

function pruneMap(map, keepCount) {
    if (map.size <= keepCount) return;
    const sorted = [...map.entries()].sort((a, b) => b[1] - a[1]);
    map.clear();
    for (let i = 0; i < Math.min(keepCount, sorted.length); i++) map.set(sorted[i][0], sorted[i][1]);
}

// ═══════════════════════════════════════════════════════════════
const userTerms = new Map();    // authorId → Map<term, count>
const userInterests = new Map();
let processedCount = 0;
let pruneCounter = 0;

parentPort.on('message', (msg) => {
    switch (msg.type) {
        case 'batch':
            processBatch(msg.rows);
            processedCount += msg.rows.length;
            parentPort.postMessage({ type: 'progress', count: processedCount });
            break;

        case 'computeTFIDF': {
            // 로컬 DF 계산 (전송 불필요! 해시 파티셔닝이므로 로컬 DF ≈ 글로벌 DF/N)
            const { maxStyle, maxInterest } = msg;
            const localUsers = userTerms.size;
            if (localUsers === 0) {
                parentPort.postMessage({ type: 'done', userCount: 0 });
                break;
            }

            const logLocal = Math.log(localUsers);
            const localStyleDF = new Map();
            const localInterestDF = new Map();

            for (const terms of userTerms.values()) {
                for (const t of terms.keys()) localStyleDF.set(t, (localStyleDF.get(t) || 0) + 1);
            }
            for (const ints of userInterests.values()) {
                for (const t of ints.keys()) localInterestDF.set(t, (localInterestDF.get(t) || 0) + 1);
            }
            // df < 2 필터
            for (const [t, d] of localStyleDF) { if (d < 2) localStyleDF.delete(t); }
            for (const [t, d] of localInterestDF) { if (d < 2) localInterestDF.delete(t); }

            // TF-IDF + 스트리밍 전송
            const CHUNK_SIZE = 200;
            let chunk = {};
            let chunkCount = 0;

            for (const [authorId, terms] of userTerms) {
                const sArr = [];
                for (const [term, count] of terms) {
                    const df = localStyleDF.get(term);
                    if (!df) continue;
                    sArr.push([term, (1 + Math.log(count)) * (logLocal - Math.log(df) + 1)]);
                }
                sArr.sort((a, b) => b[1] - a[1]);
                let sNormSq = 0;
                const sObj = {};
                for (let i = 0; i < Math.min(maxStyle, sArr.length); i++) {
                    const v = Number(sArr[i][1].toFixed(4));
                    sObj[sArr[i][0]] = v;
                    sNormSq += v * v;
                }

                const ints = userInterests.get(authorId);
                let iNormSq = 0;
                const iObj = {};
                if (ints) {
                    const iArr = [];
                    for (const [term, count] of ints) {
                        const df = localInterestDF.get(term);
                        if (!df) continue;
                        iArr.push([term, (1 + Math.log(count)) * (logLocal - Math.log(df) + 1)]);
                    }
                    iArr.sort((a, b) => b[1] - a[1]);
                    for (let i = 0; i < Math.min(maxInterest, iArr.length); i++) {
                        const v = Number(iArr[i][1].toFixed(4));
                        iObj[iArr[i][0]] = v;
                        iNormSq += v * v;
                    }
                    userInterests.delete(authorId);
                }
                userTerms.delete(authorId);

                chunk[authorId] = {
                    r: Number(Math.sqrt(sNormSq).toFixed(4)),
                    v: sObj,
                    ir: Number(Math.sqrt(iNormSq).toFixed(4)),
                    iv: iObj
                };
                chunkCount++;

                if (chunkCount >= CHUNK_SIZE) {
                    parentPort.postMessage({ type: 'chunk', data: chunk });
                    chunk = {};
                    chunkCount = 0;
                }
            }
            if (chunkCount > 0) {
                parentPort.postMessage({ type: 'chunk', data: chunk });
            }
            userTerms.clear();
            userInterests.clear();
            localStyleDF.clear();
            localInterestDF.clear();
            parentPort.postMessage({ type: 'done', userCount: localUsers });
            break;
        }
    }
});

// rows = [[authorId, channel, msgdata], ...]
function processBatch(rows) {
    for (let ri = 0; ri < rows.length; ri++) {
        const authorId = rows[ri][0];
        const channel = rows[ri][1];
        const raw = rows[ri][2];
        if (!raw || raw.length === 0) continue;

        let terms = userTerms.get(authorId);
        if (!terms) { terms = new Map(); userTerms.set(authorId, terms); }
        let interests = userInterests.get(authorId);
        if (!interests) { interests = new Map(); userInterests.set(authorId, interests); }

        const tokens = raw.split(RE_WS);
        const tokLen = tokens.length;

        // ── Unigram + Bigram ──
        for (let i = 0; i < tokLen; i++) {
            const t = tokens[i];
            if (!t || t.length > 20) continue;
            terms.set(t, (terms.get(t) || 0) + 1);
            if (i < tokLen - 1) {
                const next = tokens[i + 1];
                if (next && next.length <= 20) terms.set(t + '_' + next, (terms.get(t + '_' + next) || 0) + 1);
            }
        }

        // ── 문장 끝 패턴 ──
        if (tokLen > 0) {
            const last = tokens[tokLen - 1];
            if (last && last.length <= 10) {
                terms.set('E_' + last, (terms.get('E_' + last) || 0) + 1);
                if (tokLen >= 2) {
                    const prev = tokens[tokLen - 2];
                    if (prev && prev.length <= 10) terms.set('E2_' + prev + '_' + last, (terms.get('E2_' + prev + '_' + last) || 0) + 1);
                }
            }
        }

        // ── ㅋㅎㅠ 반복 ──
        RE_REP.lastIndex = 0;
        let m;
        while ((m = RE_REP.exec(raw)) !== null) {
            const key = 'R_' + m[0][0] + Math.min(m[0].length, 8);
            terms.set(key, (terms.get(key) || 0) + 1);
        }

        // ── 글자 바이그램 ──
        for (let ti = 0; ti < tokLen; ti++) {
            const t = tokens[ti];
            if (!t || t.length < 2 || t.length > 6) continue;
            for (let ci = 0; ci < t.length - 1; ci++) {
                const cb = 'C_' + t[ci] + t[ci + 1];
                terms.set(cb, (terms.get(cb) || 0) + 1);
            }
        }

        // ── 길이 버킷 ──
        const lb = tokLen <= 2 ? 'L_짧' : tokLen <= 6 ? 'L_중' : tokLen <= 15 ? 'L_긴' : 'L_장문';
        terms.set(lb, (terms.get(lb) || 0) + 1);

        // ── 존댓말/반말 ──
        if (tokLen > 0) {
            const last = tokens[tokLen - 1];
            if (last) {
                if (RE_FORMAL.test(last) || RE_FORMAL2.test(last)) terms.set('S_존댓말', (terms.get('S_존댓말') || 0) + 1);
                else if (RE_CASUAL.test(last) || RE_CASUAL2.test(last) || RE_CASUAL3.test(last)) terms.set('S_반말', (terms.get('S_반말') || 0) + 1);
            }
        }

        // ── 접속사 ──
        for (let ti = 0; ti < tokLen; ti++) { if (CONJ_SET.has(tokens[ti])) terms.set('J_' + tokens[ti], (terms.get('J_' + tokens[ti]) || 0) + 1); }

        // ── 특수문자 빈도 ──
        let qC = 0, exC = 0;
        for (let ci = 0; ci < raw.length; ci++) { const ch = raw.charCodeAt(ci); if (ch === 63) qC++; else if (ch === 33) exC++; }
        if (qC) terms.set('P_?', (terms.get('P_?') || 0) + qC);
        if (exC) terms.set('P_!', (terms.get('P_!') || 0) + exC);
        RE_DOTS.lastIndex = 0;
        let dotC = 0;
        while (RE_DOTS.exec(raw) !== null) dotC++;
        if (dotC) terms.set('P_..', (terms.get('P_..') || 0) + dotC);

        // ── 숫자 표기 ──
        if (RE_HAS_DIGIT.test(raw)) terms.set('N_아라비아', (terms.get('N_아라비아') || 0) + 1);
        if (RE_HAN_NUM.test(raw)) terms.set('N_한글', (terms.get('N_한글') || 0) + 1);

        // ── 자모 축약어 ──
        RE_JAMO_SEQ.lastIndex = 0;
        while ((m = RE_JAMO_SEQ.exec(raw)) !== null) { if (m[0].length <= 4) terms.set('A_' + m[0], (terms.get('A_' + m[0]) || 0) + 1); }

        // ════ 관심사 ════
        if (channel) interests.set('CH_' + channel, (interests.get('CH_' + channel) || 0) + 1);
        for (let ti = 0; ti < tokLen; ti++) { if (isContentWord(tokens[ti])) interests.set('I_' + tokens[ti], (interests.get('I_' + tokens[ti]) || 0) + 1); }
        for (let i = 0; i < tokLen - 1; i++) {
            const t = tokens[i], next = tokens[i + 1];
            if (t && next && isContentWord(t)) {
                if (OPINION_POS.has(next)) interests.set('OP_' + t + '_호', (interests.get('OP_' + t + '_호') || 0) + 1);
                else if (OPINION_NEG.has(next)) interests.set('OP_' + t + '_비', (interests.get('OP_' + t + '_비') || 0) + 1);
            }
        }
        for (let i = 0; i < tokLen - 1; i++) {
            if (isContentWord(tokens[i]) && isContentWord(tokens[i + 1])) interests.set('IB_' + tokens[i] + '_' + tokens[i + 1], (interests.get('IB_' + tokens[i] + '_' + tokens[i + 1]) || 0) + 1);
        }

        // ★ OOM 방지
        pruneCounter++;
        if (pruneCounter % 500 === 0) {
            if (terms.size > 2000) pruneMap(terms, 700);
            if (interests.size > 1500) pruneMap(interests, 400);
        }
    }
}
