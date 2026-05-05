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
let termDF = null;

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
// 상수
// ═══════════════════════════════════════════════════════════════
const PARTICLES = new Set([
    '이', '가', '은', '는', '을', '를', '에', '의', '와', '과', '로', '으로',
    '도', '만', '부터', '까지', '야', '아', '요', '여', '네', '죠', '고', '며',
    '면', '서', '께', '한테', '에게', '에서', '라', '든', '랑', '이랑', '하'
]);
const OPINION_POS = new Set([
    '좋아', '좋다', '좋음', '좋', '최고', '짱', '맛있', '사랑', '추천',
    '좋아해', '좋아하', '대박', '쩔어', '쩔', '미쳤', '레전드', '인정',
    '존맛', '개맛', '킹', '갓', '존좋', '개좋', '잘하', '잘한'
]);
const OPINION_NEG = new Set([
    '싫어', '싫다', '싫음', '싫', '별로', '구려', '구림', '노잼',
    '못먹', '못해', '최악', '쓰레기', '안좋', '꼴불견', '역겨', '역겹',
    '구데기', '노맛', '개별로', '존별로', '못하', '못한'
]);
const SKIP_WORDS = new Set([
    '그래', '네', '응', '아니', '뭐', '거', '것', '수', '때', '안', '못',
    '더', '좀', '잘', '왜', '진짜', 'ㄹㅇ', '아', '어', '오', '이거', '저거',
    '그거', '여기', '거기', '저기', '우리', '나', '너', '제', '내'
]);
const CONJ_SET = new Set(['그리고', '하지만', '그래서', '근데', '그런데', '왜냐면', '아니면', '그러니까', '게다가', '또', '즉']);

// 흔한 어미/종결형 (띄어쓰기로 분리됐을 때 특징으로 감지)
const SPLIT_ENDINGS = new Set([
    '요', '죠', '네요', '네', '다', '는데', '구나', '군', '군요', '니까',
    '거든', '거든요', '라고', '라며', '입니다', '습니다', '에요', '예요', '여', '임', '음'
]);

const EMOJI_RE = /[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}\u{2B00}-\u{2BFF}\u{1F000}-\u{1F0FF}]/u;

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

    const pool = mysql.createPool({
        host: DB_HOST, user: DB_USER, password: DB_PASS, database: DB_SCHEMA,
        waitForConnections: true, connectionLimit: 4, queueLimit: 0
    });

    try {
        // ═══════════════════════════════════════════════════════════════
        // STEP 1
        // ═══════════════════════════════════════════════════════════════
        buildProgress = '[1/5] DB에서 헤비 유저 추출 중...';
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
        console.log(`  => ${validMap.size.toLocaleString()}명 추출 완료`);

        // ═══════════════════════════════════════════════════════════════
        // STEP 2
        // ═══════════════════════════════════════════════════════════════
        buildProgress = '[2/5] 유저 매핑 구축 중...';
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
        // STEP 3: 문체 + 관심사 + 서명(idiosyncrasy) 수집
        // ═══════════════════════════════════════════════════════════════
        buildProgress = '[3/5] 문체+관심사+서명 분석 중...';
        console.log('\n' + buildProgress);

        const userTerms = new Map();
        const userInterests = new Map();
        const userSigs = new Map(); // 개인 습관(띄어쓰기, 구두점 붙임 등)
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

                let sigs = userSigs.get(lookup.authorId);
                if (!sigs) { sigs = new Map(); userSigs.set(lookup.authorId, sigs); }

                const raw = row.msgdata;
                const tokens = raw.split(/\s+/).filter(Boolean);
                const msgLen = tokens.length;

                // ─────────── 문체 (기존) ───────────
                for (let i = 0; i < tokens.length; i++) {
                    const t = tokens[i];
                    if (!t || t.length > 20) continue;
                    terms.set(t, (terms.get(t) || 0) + 1);
                    if (i < tokens.length - 1 && tokens[i + 1] && tokens[i + 1].length <= 20) {
                        const bg = t + '_' + tokens[i + 1];
                        terms.set(bg, (terms.get(bg) || 0) + 1);
                    }
                }

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

                const repMatch = raw.match(/([ㅋㅎㅠㅜ])\1+/g);
                if (repMatch) {
                    for (const pat of repMatch) {
                        const key = 'R_' + pat[0] + Math.min(pat.length, 8);
                        terms.set(key, (terms.get(key) || 0) + 1);
                    }
                }

                for (const t of tokens) {
                    if (!t || t.length < 2 || t.length > 6) continue;
                    for (let ci = 0; ci < t.length - 1; ci++) {
                        const cb = 'C_' + t[ci] + t[ci + 1];
                        terms.set(cb, (terms.get(cb) || 0) + 1);
                    }
                }

                const lenBucket = msgLen <= 2 ? 'L_짧' : msgLen <= 6 ? 'L_중' : msgLen <= 15 ? 'L_긴' : 'L_장문';
                terms.set(lenBucket, (terms.get(lenBucket) || 0) + 1);

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

                for (const t of tokens) {
                    if (CONJ_SET.has(t)) terms.set('J_' + t, (terms.get('J_' + t) || 0) + 1);
                }

                const qCount = (raw.match(/\?/g) || []).length;
                const exCount = (raw.match(/!/g) || []).length;
                const dotCount = (raw.match(/\.{2,}/g) || []).length;
                if (qCount) terms.set('P_?', (terms.get('P_?') || 0) + qCount);
                if (exCount) terms.set('P_!', (terms.get('P_!') || 0) + exCount);
                if (dotCount) terms.set('P_..', (terms.get('P_..') || 0) + dotCount);

                if (/\d/.test(raw)) terms.set('N_아라비아', (terms.get('N_아라비아') || 0) + 1);
                if (/[일이삼사오육칠팔구십백천만억]/.test(raw)) terms.set('N_한글', (terms.get('N_한글') || 0) + 1);

                const jamoMatch = raw.match(/[ㄱ-ㅎ]{2,}/g);
                if (jamoMatch) {
                    for (const jm of jamoMatch) {
                        if (jm.length <= 4) terms.set('A_' + jm, (terms.get('A_' + jm) || 0) + 1);
                    }
                }

                // ─────────── 관심사 (기존) ───────────
                if (row.channel) {
                    interests.set('CH_' + row.channel, (interests.get('CH_' + row.channel) || 0) + 1);
                }
                for (const t of tokens) {
                    if (isContentWord(t)) {
                        interests.set('I_' + t, (interests.get('I_' + t) || 0) + 1);
                    }
                }
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
                for (let i = 0; i < tokens.length - 1; i++) {
                    if (isContentWord(tokens[i]) && isContentWord(tokens[i + 1])) {
                        const ib = 'IB_' + tokens[i] + '_' + tokens[i + 1];
                        interests.set(ib, (interests.get(ib) || 0) + 1);
                    }
                }

                // ════════════════════════════════════════════════════════
                // ★★★ 서명(Idiosyncrasy) 피쳐 — 동일인물 판별용 ★★★
                // ════════════════════════════════════════════════════════

                // [SIG-1] 구두점 붙임 vs 분리 습관
                //   "안녕 !" (분리형) vs "안녕!" (붙임형)
                const attachedPunct = (raw.match(/[가-힣a-zA-Z0-9][!?.,~]/g) || []).length;
                const spacedPunct = (raw.match(/\s+[!?.,~]/g) || []).length;
                if (attachedPunct) sigs.set('SG_punct_attached', (sigs.get('SG_punct_attached') || 0) + attachedPunct);
                if (spacedPunct) sigs.set('SG_punct_spaced', (sigs.get('SG_punct_spaced') || 0) + spacedPunct);

                // [SIG-2] 쉼표 뒤 띄어쓰기 습관
                const commaSpaced = (raw.match(/,\s+/g) || []).length;
                const commaNoSpace = (raw.match(/,[^\s,]/g) || []).length;
                if (commaSpaced) sigs.set('SG_comma_space', (sigs.get('SG_comma_space') || 0) + commaSpaced);
                if (commaNoSpace) sigs.set('SG_comma_nospace', (sigs.get('SG_comma_nospace') || 0) + commaNoSpace);

                // [SIG-3] 문장부호 반복 패턴 ("???", "!!!!", "ㅋㅋㅋ???")
                const multiQ = (raw.match(/\?{2,}/g) || []).length;
                const multiE = (raw.match(/!{2,}/g) || []).length;
                const mixedQE = (raw.match(/[!?]{3,}/g) || []).length;
                if (multiQ) sigs.set('SG_multi_?', (sigs.get('SG_multi_?') || 0) + multiQ);
                if (multiE) sigs.set('SG_multi_!', (sigs.get('SG_multi_!') || 0) + multiE);
                if (mixedQE) sigs.set('SG_mixed_QE', (sigs.get('SG_mixed_QE') || 0) + mixedQE);

                // [SIG-4] 종결어미 분리 습관 (예: "맛있 어요" / "먹었 어")
                //   앞 토큰 뒤에 짧은 종결어미가 따로 나오는 패턴
                for (let i = 0; i < tokens.length - 1; i++) {
                    const next = tokens[i + 1];
                    if (SPLIT_ENDINGS.has(next)) {
                        sigs.set('SG_split_' + next, (sigs.get('SG_split_' + next) || 0) + 1);
                    }
                }

                // [SIG-5] 특정 단어의 띄어쓰기 습관
                //   "ㅋ ㅋ ㅋ", "ㅎ ㅎ ㅎ" (자모 한 글자씩 띄움)
                for (let i = 0; i < tokens.length - 1; i++) {
                    if (/^[ㅋㅎㅠㅜㅡ]$/.test(tokens[i]) && /^[ㅋㅎㅠㅜㅡ]$/.test(tokens[i + 1])) {
                        sigs.set('SG_spaced_jamo', (sigs.get('SG_spaced_jamo') || 0) + 1);
                        break;
                    }
                }

                // [SIG-6] 한 글자 토큰 비율 (조사/어미를 띄어쓰는 습관 감지)
                let oneCharTok = 0;
                for (const t of tokens) if (t.length === 1 && /[가-힣]/.test(t)) oneCharTok++;
                if (oneCharTok >= 2) sigs.set('SG_many_1char', (sigs.get('SG_many_1char') || 0) + 1);

                // [SIG-7] 문장 시작 대문자/특정 토큰 습관
                if (tokens.length > 0) {
                    const first = tokens[0];
                    if (first && first.length <= 6) {
                        sigs.set('SG_start_' + first, (sigs.get('SG_start_' + first) || 0) + 1);
                    }
                }

                // [SIG-8] 이모지 사용 습관
                if (EMOJI_RE.test(raw)) {
                    sigs.set('SG_use_emoji', (sigs.get('SG_use_emoji') || 0) + 1);
                    const emojis = raw.match(/[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}]/gu) || [];
                    for (const em of emojis.slice(0, 3)) {
                        sigs.set('SG_emo_' + em, (sigs.get('SG_emo_' + em) || 0) + 1);
                    }
                }

                // [SIG-9] 영문/라틴 문자 혼용 습관
                if (/[a-zA-Z]/.test(raw)) {
                    sigs.set('SG_use_latin', (sigs.get('SG_use_latin') || 0) + 1);
                    if (/[a-zA-Z]{3,}/.test(raw)) sigs.set('SG_latin_word', (sigs.get('SG_latin_word') || 0) + 1);
                }

                // [SIG-10] 물결/틸드 사용 ("좋아요~~")
                const tilde = (raw.match(/~+/g) || []);
                if (tilde.length) {
                    sigs.set('SG_tilde', (sigs.get('SG_tilde') || 0) + tilde.length);
                    for (const tl of tilde) {
                        if (tl.length >= 2) sigs.set('SG_tilde_long', (sigs.get('SG_tilde_long') || 0) + 1);
                    }
                }

                // [SIG-11] 모음/자음 늘림 습관 ("좋아아아아", "네에에")
                const stretched = raw.match(/([가-힣])\1{2,}/g);
                if (stretched) sigs.set('SG_char_stretch', (sigs.get('SG_char_stretch') || 0) + stretched.length);

                // [SIG-12] 말줄임표 스타일 (".." vs "..." vs "…")
                if (/\.{2}(?!\.)/.test(raw)) sigs.set('SG_dot2', (sigs.get('SG_dot2') || 0) + 1);
                if (/\.{3}(?!\.)/.test(raw)) sigs.set('SG_dot3', (sigs.get('SG_dot3') || 0) + 1);
                if (/\.{4,}/.test(raw)) sigs.set('SG_dot4plus', (sigs.get('SG_dot4plus') || 0) + 1);
                if (/…/.test(raw)) sigs.set('SG_ellipsis_uni', (sigs.get('SG_ellipsis_uni') || 0) + 1);

                // [SIG-13] 괄호/따옴표 사용 습관
                if (/\(/.test(raw)) sigs.set('SG_paren', (sigs.get('SG_paren') || 0) + 1);
                if (/"/.test(raw)) sigs.set('SG_dquote', (sigs.get('SG_dquote') || 0) + 1);
                if (/'/.test(raw)) sigs.set('SG_squote', (sigs.get('SG_squote') || 0) + 1);

                // [SIG-14] 평균 토큰 길이 버킷
                if (tokens.length > 0) {
                    let tot = 0;
                    for (const t of tokens) tot += t.length;
                    const avg = tot / tokens.length;
                    const avgBucket = avg < 1.5 ? 'SG_avgL_xs' : avg < 2.5 ? 'SG_avgL_s' : avg < 4 ? 'SG_avgL_m' : 'SG_avgL_l';
                    sigs.set(avgBucket, (sigs.get(avgBucket) || 0) + 1);
                }

                // [SIG-15] 반복 감탄사 단독 사용 ("ㅋㅋㅋ", "ㅎㅎ"만 보내는 습관)
                if (tokens.length === 1 && /^[ㅋㅎㅠㅜ]+$/.test(tokens[0])) {
                    sigs.set('SG_solo_laugh', (sigs.get('SG_solo_laugh') || 0) + 1);
                }

                // OOM 방지
                if (terms.size > 1200) {
                    const sorted = [...terms.entries()].sort((a, b) => b[1] - a[1]);
                    terms.clear();
                    for (let j = 0; j < 700; j++) terms.set(sorted[j][0], sorted[j][1]);
                }
                if (interests.size > 800) {
                    const sorted = [...interests.entries()].sort((a, b) => b[1] - a[1]);
                    interests.clear();
                    for (let j = 0; j < 400; j++) interests.set(sorted[j][0], sorted[j][1]);
                }
                if (sigs.size > 400) {
                    const sorted = [...sigs.entries()].sort((a, b) => b[1] - a[1]);
                    sigs.clear();
                    for (let j = 0; j < 250; j++) sigs.set(sorted[j][0], sorted[j][1]);
                }
            }
            totalProcessed += rows.length;
            buildProgress = `[3/5] 분석: ${totalProcessed.toLocaleString()}건`;
            process.stdout.write(`\r  ... ${totalProcessed.toLocaleString()} 건 처리됨`);
        }
        nidLookup.clear();
        console.log(`\n  => 분석 완료 (${((Date.now() - t0) / 1000).toFixed(1)}초 경과)`);

        // ═══════════════════════════════════════════════════════════════
        // STEP 4: TF-IDF (문체 + 관심사 + 서명)
        // ═══════════════════════════════════════════════════════════════
        buildProgress = '[4/5] TF-IDF 벡터 생성 중...';
        console.log('\n' + buildProgress);

        const totalUsers = userTerms.size;

        // 문체 DF
        const dfStyle = new Map();
        for (const terms of userTerms.values()) {
            for (const term of terms.keys()) dfStyle.set(term, (dfStyle.get(term) || 0) + 1);
        }
        for (const [term, df] of dfStyle.entries()) { if (df < 2) dfStyle.delete(term); }

        // 관심사 DF
        const dfInterest = new Map();
        for (const interests of userInterests.values()) {
            for (const term of interests.keys()) dfInterest.set(term, (dfInterest.get(term) || 0) + 1);
        }
        for (const [term, df] of dfInterest.entries()) { if (df < 2) dfInterest.delete(term); }

        // 서명 DF
        const dfSig = new Map();
        for (const sigs of userSigs.values()) {
            for (const term of sigs.keys()) dfSig.set(term, (dfSig.get(term) || 0) + 1);
        }
        for (const [term, df] of dfSig.entries()) { if (df < 2) dfSig.delete(term); }

        const MAX_STYLE = 300;
        const MAX_INTEREST = 200;
        const MAX_SIG = 120;
        const exportUsers = {};

        for (const [authorId, terms] of userTerms) {
            const info = validMap.get(authorId);
            if (!info) {
                userTerms.delete(authorId);
                userInterests.delete(authorId);
                userSigs.delete(authorId);
                continue;
            }

            // 문체 TF-IDF
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

            // 관심사 TF-IDF
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

            // 서명 TF-IDF
            const sigs = userSigs.get(authorId);
            let dObj = {};
            let dNormSq = 0;
            if (sigs) {
                const sigArr = [];
                for (const [term, count] of sigs.entries()) {
                    const df = dfSig.get(term);
                    if (!df) continue;
                    sigArr.push({ term, val: (1 + Math.log(count)) * (Math.log(totalUsers / df) + 1) });
                }
                userSigs.delete(authorId);
                sigArr.sort((a, b) => b.val - a.val);
                const topSig = sigArr.slice(0, MAX_SIG);
                for (const item of topSig) {
                    const v = Number(item.val.toFixed(4));
                    dObj[item.term] = v;
                    dNormSq += v * v;
                }
            }

            exportUsers[authorId] = {
                n: info.authorName,
                c: info.msgCount,
                r: Number(Math.sqrt(sNormSq).toFixed(4)),
                v: sObj,
                ir: Number(Math.sqrt(iNormSq).toFixed(4)),
                iv: iObj,
                dr: Number(Math.sqrt(dNormSq).toFixed(4)),
                dv: dObj
            };
        }

        userTerms.clear(); userInterests.clear(); userSigs.clear();
        dfStyle.clear(); dfInterest.clear(); dfSig.clear(); validMap.clear();

        // ═══════════════════════════════════════════════════════════════
        // STEP 5
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
// 검색 API (문체 + 관심사 + 희귀용어 + 서명(idiosyncrasy))
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

    // 가중치 (총합이 1이 아니어도 활성 척도에 따라 자동 정규화됨)
    const STYLE_W = Math.max(0, Math.min(1, parseFloat(req.query.ws) || 0.30));
    const INTEREST_W = Math.max(0, Math.min(1, parseFloat(req.query.wi) || 0.25));
    const RARE_W = Math.max(0, Math.min(1, parseFloat(req.query.wr) || 0.20));
    const SIG_W = Math.max(0, Math.min(1, parseFloat(req.query.wd) || 0.25)); // 서명 가중치(신규)

    const totalUserCount = Object.keys(users).length;
    const rareThreshold = Math.max(Math.floor(totalUserCount * 0.01), 3);

    // 타겟 희귀 용어
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
    const tgtDKeys = Object.keys(tgt.dv || {});
    const results = [];

    for (const authorId in users) {
        if (authorId === target) continue;
        const u = users[authorId];

        // ── 문체 코사인 ──
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

        // ── 관심사 코사인 ──
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

        // ── 서명(idiosyncrasy) 코사인 ──
        let dDot = 0;
        const dCommon = [];
        const uDv = u.dv || {};
        const tgtDv = tgt.dv || {};
        const tgtDr = tgt.dr || 0;
        const uDr = u.dr || 0;
        for (let i = 0; i < tgtDKeys.length; i++) {
            const term = tgtDKeys[i];
            const uVal = uDv[term];
            if (uVal) {
                const prod = tgtDv[term] * uVal;
                dDot += prod;
                dCommon.push({ t: term.replace(/^SG_/, '').replace(/_/g, ' '), s: prod });
            }
        }
        const idioSim = (dDot > 0 && tgtDr > 0 && uDr > 0) ? dDot / (tgtDr * uDr) : 0;

        // ── 희귀 용어 공유 ──
        let rareSim = 0;
        const rareShared = [];
        if (tgtRareSet.size > 0 && termDF) {
            let shared = 0;
            let uRareCount = 0;
            for (const term in u.v) {
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
                const weightedSum = rareShared.reduce((s, x) => s + x.rarity, 0);
                rareSim = Math.min(weightedSum / Math.sqrt(minRare), 1.0);
            }
        }

        // ── 복합 점수 (활성 척도 가중치 재분배) ──
        const hasStyle = styleSim > 0;
        const hasInterest = interestSim > 0;
        const hasRare = rareSim > 0;
        const hasIdio = idioSim > 0;
        const activeCount = (hasStyle ? 1 : 0) + (hasInterest ? 1 : 0) + (hasRare ? 1 : 0) + (hasIdio ? 1 : 0);
        if (activeCount === 0) continue;

        const totalW =
            (hasStyle ? STYLE_W : 0) +
            (hasInterest ? INTEREST_W : 0) +
            (hasRare ? RARE_W : 0) +
            (hasIdio ? SIG_W : 0);

        const combined = (
            (hasStyle ? STYLE_W * styleSim : 0) +
            (hasInterest ? INTEREST_W * interestSim : 0) +
            (hasRare ? RARE_W * rareSim : 0) +
            (hasIdio ? SIG_W * idioSim : 0)
        ) / totalW;

        // ── 동일인물 강한 신호 보너스 ──
        // 문체 + 서명 + 희귀어가 동시에 높으면 동일인일 가능성이 매우 높음
        let identityBoost = 0;
        if (styleSim > 0.5 && idioSim > 0.5 && rareSim > 0.3) {
            identityBoost = 0.05;
        }
        const finalScore = Math.min(combined + identityBoost, 1.0);

        if (finalScore > 0.03) {
            sCommon.sort((a, b) => b.s - a.s);
            iCommon.sort((a, b) => b.s - a.s);
            dCommon.sort((a, b) => b.s - a.s);
            rareShared.sort((a, b) => a.df - b.df);

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
                similarity: finalScore,
                styleSim: Number(styleSim.toFixed(4)),
                interestSim: Number(interestSim.toFixed(4)),
                rareSim: Number(rareSim.toFixed(4)),
                idioSim: Number(idioSim.toFixed(4)),
                identityBoost: Number(identityBoost.toFixed(4)),
                topTerms: sCommon.slice(0, 7).map(x => x.t),
                topInterests: { channels: channels.slice(0, 5), topics: topics.slice(0, 8), opinions: opinions.slice(0, 5) },
                rareTerms: rareShared.slice(0, 8).map(x => ({ term: x.t, users: x.df })),
                idioTraits: dCommon.slice(0, 8).map(x => x.t)  // 공통 습관(예: punct attached, dot3, tilde long ...)
            });
        }
    }

    results.sort((a, b) => b.similarity - a.similarity);

    res.json({
        target: { authorId: target, authorName: tgt.n, msgCount: tgt.c, rareTermCount: tgtRareSet.size },
        weights: { style: STYLE_W, interest: INTEREST_W, rare: RARE_W, idio: SIG_W },
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