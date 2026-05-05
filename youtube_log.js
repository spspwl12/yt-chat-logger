const { LiveChat } = require("./youtube-chat");
const express = require("express");
const mysql = require("mysql2/promise");
const crypto = require("crypto");
const path = require("path");
const fs = require('fs');
const zlib = require("zlib");
const mecab = require('./mecab-ya.js');

const DATA_FILE = './data.json';
const BUFFER_FILE = './db_buffer.ndjson';

const app = express();
const PORT = 3000;

const DB_HOST = "127.0.0.1";
const DB_USER = "root";
const DB_PASS = "";
const DB_SCHEMA = "DATA";

// ─── DB 타임아웃 설정 ───
const DB_CONNECT_TIMEOUT = 60000;   // 풀에서 커넥션 얻기 타임아웃 (ms)
const DB_QUERY_TIMEOUT = 60000;    // 개별 쿼리 타임아웃 (ms)
const DB_HEALTH_INTERVAL = 5000;   // 헬스체크 간격 (ms)
const DB_BUFFER_LIMIT = 100000;     // 버퍼 최대 크기 (이 이상이면 오래된 항목 버림)

const yt = {};
const dbBuffer = loadBufferFromFile();
let dbConnected = false;
let isFlushing = false;
let poolVersion = 0;  // 풀 세대 추적: 오래된 에러가 새 풀 파괴 방지

// ─── 쓰기/읽기 풀 분리 ───
// writePool: INSERT 전용 (채팅 저장, 버퍼 flush)
// readPool:  SELECT 전용 (쿼리 엔드포인트)
// → INSERT가 몰려도 SELECT가 독립적으로 동작
let writePool = null;
let readPool = null;

// authorId -> { uid, profiles: Map<cacheKey, nid> }
const userCache = new Map();

function readData() {
    if (!fs.existsSync(DATA_FILE))
        fs.writeFileSync(DATA_FILE, JSON.stringify([]));
    return JSON.parse(fs.readFileSync(DATA_FILE));
}

function writeData(data) {
    fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
}

function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// ─── 버퍼 파일 로드 (시작 시) ───
function loadBufferFromFile() {
    try {
        if (!fs.existsSync(BUFFER_FILE)) return [];
        const content = fs.readFileSync(BUFFER_FILE, 'utf8').trim();
        if (!content) return [];
        const items = content.split('\n').map(line => {
            try {
                const obj = JSON.parse(line);
                // message(압축 데이터)는 base64로 저장했으므로 Buffer로 복원
                if (obj.message && typeof obj.message === 'string') {
                    obj.message = Buffer.from(obj.message, 'base64');
                }
                if (obj.timestamp && typeof obj.timestamp === 'string') {
                    obj.timestamp = new Date(obj.timestamp);
                }
                return obj;
            } catch { return null; }
        }).filter(Boolean);
        console.log(`[Buffer] Loaded ${items.length} items from file`);
        return items;
    } catch (err) {
        console.error('[Buffer] Failed to load buffer file:', err.message);
        return [];
    }
}

// ─── 버퍼 항목을 파일에 추가 (append) ───
function appendBufferToFile(chatData) {
    try {
        const obj = { ...chatData };
        // Buffer(압축 메시지)는 base64 문자열로 변환해서 저장
        if (Buffer.isBuffer(obj.message)) {
            obj.message = obj.message.toString('base64');
        }
        fs.appendFileSync(BUFFER_FILE, JSON.stringify(obj) + '\n');
    } catch (err) {
        console.error('[Buffer] Failed to append to file:', err.message);
    }
}

// ─── 버퍼 파일을 현재 dbBuffer 상태로 덮어쓰기 ───
function syncBufferFile() {
    try {
        if (dbBuffer.length === 0) {
            // 버퍼 비었으면 파일 삭제
            if (fs.existsSync(BUFFER_FILE)) fs.unlinkSync(BUFFER_FILE);
            return;
        }
        const lines = dbBuffer.map(item => {
            const obj = { ...item };
            if (Buffer.isBuffer(obj.message)) {
                obj.message = obj.message.toString('base64');
            }
            return JSON.stringify(obj);
        });
        fs.writeFileSync(BUFFER_FILE, lines.join('\n') + '\n');
    } catch (err) {
        console.error('[Buffer] Failed to sync buffer file:', err.message);
    }
}

function youtube_flag(e) {
    let b = 0;
    if (e.isMembership) b |= 1;
    if (e.isModerator) b |= 2;
    if (e.isOwner) b |= 4;
    if (e.isVerified) b |= 8;
    if (e.superchat) b |= 16;
    return b;
}

function NVL(e) {
    return e ?? " ";
}

// ─── 에러 요약 유틸 ───
function errSummary(err) {
    if (!err) return 'unknown error';
    const parts = [];
    if (err.status) parts.push(`status=${err.status}`);
    if (err.code) parts.push(`code=${err.code}`);
    parts.push(err.message || 'no message');
    return parts.join(' ');
}

const DB_CONN_ERROR_CODES = [
    'PROTOCOL_CONNECTION_LOST', 'ECONNRESET', 'ECONNREFUSED',
    'ETIMEDOUT', 'ENETUNREACH', 'ENOTFOUND', 'EPIPE', 'ECONNABORTED'
];

function isConnectionError(err) {
    if (!err) return false;
    const code = err.code || '';
    const msg = (err.message || '').toLowerCase();
    if (DB_CONN_ERROR_CODES.includes(code)) return true;
    if (msg.includes('connection lost') || msg.includes('cannot enqueue') ||
        msg.includes('connection closed') || msg.includes('socket hang up') ||
        msg.includes('timeout')) return true;
    if (err.fatal) return true;
    return false;
}

// ─── 타임아웃이 적용된 getConnection 래퍼 ───
function getConnectionWithTimeout(thePool, timeoutMs) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
            reject(new Error(`getConnection timeout (${timeoutMs}ms)`));
        }, timeoutMs);

        thePool.getConnection()
            .then(conn => {
                clearTimeout(timer);
                resolve(conn);
            })
            .catch(err => {
                clearTimeout(timer);
                reject(err);
            });
    });
}

async function ensureIndexes(conn) {
    const indexDefs = [
        // youtube_chat3: channel 필터용
        `ALTER TABLE youtube_chat3 ADD INDEX idx_channel (channel)`,
        // youtube_chat3: nid JOIN용
        `ALTER TABLE youtube_chat3 ADD INDEX idx_nid (nid)`,
        // youtube_chat3: channel+id 복합 (채널필터+정렬)
        `ALTER TABLE youtube_chat3 ADD INDEX idx_channel_id (channel, id)`,
        // youtube_user_names: uid+author+thumb 조회용
        `ALTER TABLE youtube_user_names ADD INDEX idx_uid_author_thumb (uid, author, thumb)`,
        // youtube_user_names: author 검색용
        `ALTER TABLE youtube_user_names ADD INDEX idx_author (author)`,
    ];
    for (const ddl of indexDefs) {
        try {
            await conn.execute(ddl);
            console.log('[Index]', ddl.substring(0, 60) + '...');
        } catch (e) {
            // Duplicate key name = 이미 존재 → 무시
            if (e.code !== 'ER_DUP_KEYNAME') {
                console.error('[Index] Failed:', e.message);
            }
        }
    }
}

// ─── 풀 생성 유틸 (쓰기/읽기 공통) ───
function makePool(label, connLimit) {
    const p = mysql.createPool({
        host: DB_HOST,
        user: DB_USER,
        password: DB_PASS,
        database: DB_SCHEMA,
        waitForConnections: true,
        connectionLimit: connLimit,
        queueLimit: 0,
        enableKeepAlive: true,
        keepAliveInitialDelay: 10000,
        connectTimeout: DB_CONNECT_TIMEOUT,
    });
    console.log(`[DB] ${label} pool created (limit=${connLimit})`);
    return p;
}

async function destroyPool(p) {
    if (!p) return;
    try {
        // 모든 활성 연결 강제 종료
        const pool = p.pool || p;
        if (pool._freeConnections) {
            for (const conn of pool._freeConnections.toArray()) {
                try { conn.destroy(); } catch {}
            }
        }
        if (pool._allConnections) {
            for (const conn of pool._allConnections.toArray()) {
                try { conn.destroy(); } catch {}
            }
        }
        await p.end().catch(() => {});
    } catch {
        // 무시
    }
}

let isCreatingPools = false;

async function createPools() {
    if (isCreatingPools) return;
    isCreatingPools = true;

    // 즉시 연결 끊김 상태로 전환 (새 풀 준비 전까지 버퍼에 저장)
    dbConnected = false;

    // 이전 풀 완전 파괴
    const oldWrite = writePool;
    const oldRead = readPool;
    writePool = null;
    readPool = null;
    await destroyPool(oldWrite);
    await destroyPool(oldRead);

    poolVersion++;
    const myVersion = poolVersion;

    writePool = makePool('Write', 8);
    readPool = makePool('Read', 4);

    try {
        const conn = await getConnectionWithTimeout(writePool, DB_CONNECT_TIMEOUT);
        // 이 사이에 새 풀이 생성됐으면 무시
        if (poolVersion !== myVersion) {
            conn.release();
            return;
        }
        try {
            await ensureIndexes(conn);
        } finally {
            conn.release();
        }
        dbConnected = true;
        console.log(`[DB] Connected (v${myVersion})`);
        if (dbBuffer.length > 0) {
            console.log(`[DB] Flushing ${dbBuffer.length} buffered items...`);
            flushDbBuffer();
        }
    } catch (err) {
        console.error(`[DB] Initial connection failed (v${myVersion}): ${err.message}`);
        if (poolVersion === myVersion) {
            dbConnected = false;
        }
    } finally {
        isCreatingPools = false;
    }
}

// ─── 풀 버전 체크 후 연결 끊김 상태로 전환 ───
function markDbDisconnected(callerVersion) {
    // 현재 풀 버전과 호출자의 버전이 일치할 때만 상태 변경
    // → 오래된 에러 콜백이 새로 생성된 풀을 파괴하는 것을 방지
    if (poolVersion === callerVersion) {
        dbConnected = false;
    }
}

async function insertChat(thePool, chatData) {
    const { sid, channel, authorId, author, authorAlt, thumb, message, msgdata, flag, timestamp } = chatData;

    // 캐시에서 uid, nid를 모두 찾으면 커넥션 1회만 사용 (INSERT만)
    let userEntry = userCache.get(authorId);
    const profileKey = author + '\0' + thumb;
    let nid = userEntry?.profiles?.get(profileKey);

    // 캐시 미스 시에만 조회 쿼리 실행
    if (!userEntry || !nid) {
        const conn = await getConnectionWithTimeout(thePool, DB_CONNECT_TIMEOUT);
        try {
            if (!userEntry) {
                await conn.execute(
                    'INSERT IGNORE INTO youtube_users (authorId) VALUES (?)',
                    [authorId]
                );
                const [uRows] = await conn.execute(
                    'SELECT uid FROM youtube_users WHERE authorId = ?', [authorId]
                );
                userEntry = { uid: uRows[0].uid, profiles: new Map() };
                userCache.set(authorId, userEntry);
            }

            if (!nid) {
                const [existing] = await conn.execute(
                    'SELECT nid FROM youtube_user_names WHERE uid = ? AND author = ? AND thumb = ?',
                    [userEntry.uid, author, thumb]
                );
                if (existing.length > 0) {
                    nid = existing[0].nid;
                } else {
                    const [ins] = await conn.execute(
                        'INSERT INTO youtube_user_names (uid, author, authorAlt, thumb, first_seen) VALUES (?, ?, ?, ?, ?)',
                        [userEntry.uid, author, authorAlt, thumb, timestamp]
                    );
                    nid = ins.insertId;
                }
                userEntry.profiles.set(profileKey, nid);
            }

            // 캐시 미스 경우: 같은 커넥션으로 INSERT까지 처리
            await conn.execute(
                'INSERT INTO youtube_chat3 (sid, channel, nid, message, msgdata, flag, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)',
                [sid, channel, nid, message, msgdata, flag, timestamp]
            );
        } finally {
            conn.release();
        }
    } else {
        // 캐시 히트: INSERT 1회만 (pool.execute로 커넥션 자동 관리)
        await thePool.execute(
            'INSERT INTO youtube_chat3 (sid, channel, nid, message, msgdata, flag, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)',
            [sid, channel, nid, message, msgdata, flag, timestamp]
        );
    }
}

async function flushDbBuffer() {
    if (isFlushing || dbBuffer.length === 0 || !dbConnected) return;
    isFlushing = true;
    const flushVersion = poolVersion;  // flush 시작 시점의 풀 버전 기억
    console.log(`[Buffer] Flushing ${dbBuffer.length} items (pool v${flushVersion})...`);

    while (dbBuffer.length > 0 && dbConnected && poolVersion === flushVersion) {
        const chatData = dbBuffer[0];
        try {
            await insertChat(writePool, chatData);
            dbBuffer.shift();
        } catch (err) {
            if (err && err.code === 'ER_DUP_ENTRY') {
                dbBuffer.shift(); // 중복 항목은 제거
                continue;
            }
            if (isConnectionError(err)) {
                console.error('[Buffer] DB connection lost during flush, pausing...');
                // 현재 풀 버전이 같을 때만 상태 변경 (레이스 방지)
                if (poolVersion === flushVersion) {
                    dbConnected = false;
                }
                break;
            }
            // 기타 에러는 로그 후 건너뜀
            console.error('[Buffer] Insert error, skipping item:', err.message);
            dbBuffer.shift();
        }
    }

    isFlushing = false;
    // flush 후 파일을 남은 버퍼와 동기화
    syncBufferFile();
    if (dbBuffer.length === 0) {
        console.log('[Buffer] All buffered items flushed successfully.');
    } else {
        console.log(`[Buffer] Flush paused, ${dbBuffer.length} items remaining.`);
    }
}

async function savedataDB(chatData) {
    // DB 연결이 끊긴 상태면 버퍼에 저장
    if (!dbConnected || !writePool) {
        bufferPush(chatData);
        return;
    }

    const myVersion = poolVersion;  // 이 시점의 풀 버전 캡처
    insertChat(writePool, chatData)
        .catch((err) => {
            if (err && err.code === 'ER_DUP_ENTRY') return;

            console.error('[DB] Insert error:', err.message);

            // 연결 에러면 상태 변경 (버전 체크로 레이스 방지)
            if (isConnectionError(err)) {
                markDbDisconnected(myVersion);
            }
            bufferPush(chatData);
        });
}

// ─── 버퍼 크기 제한 ───
function bufferPush(chatData) {
    dbBuffer.push(chatData);
    appendBufferToFile(chatData);
    // 버퍼가 너무 커지면 오래된 항목부터 버림 (메모리 보호)
    if (dbBuffer.length > DB_BUFFER_LIMIT) {
        const dropped = dbBuffer.length - DB_BUFFER_LIMIT;
        dbBuffer.splice(0, dropped);
        syncBufferFile();
        console.warn(`[Buffer] Overflow, dropped ${dropped} oldest items (limit: ${DB_BUFFER_LIMIT})`);
    }
    // 로그 스팸 방지: 100개 단위로만 출력
    if (dbBuffer.length <= 1 || dbBuffer.length % 100 === 0) {
        console.log(`[Buffer] DB offline, buffered (${dbBuffer.length} items)`);
    }
}

function tokMessage(text) {
    const tok = mecab.morphsSync(text, 'morphs');
    if (tok && tok.length > 0)
        return tok.join(' ');
    return text;
}

function searchTokMessage(text) {
    if (text === '')
        return text;

    const tok = mecab.morphsSync(text, 'morphs');

    if (!tok || tok.length <= 0)
        return text;

    const rep = (e) => e.replace(/[@^+*-/_\\]/g, '');

    const rtn = tok
        .filter(e => rep(e).length > 0)
        .map(e => `+${rep(e)}*`).join(' ');

    if (rtn.length <= 2)
        return ' ';

    return rtn;
}

async function createLive(id, reset) {
    if (!id)
        return;

    const liveChat = new LiveChat({ liveId: id });
    if (!yt[id])
        yt[id] = { obj: null, error: 0, msgerr: 0 };

    if (reset) {
        yt[id].error = 0;
        yt[id].msgerr = 0;
    }

    yt[id].obj = liveChat;

    liveChat.on("start", (liveId) => {
        console.log("[YT] Connected:", id);
    });

    liveChat.on("chat", (chatItem) => {
        yt[id].error = 0;
        yt[id].msgerr = 0;
        const jstr = JSON.stringify(chatItem);
        const sid = crypto.createHash("sha256").update(jstr).digest("hex").substring(0, 32);
        const message = { m: chatItem.message, s: chatItem.superchat };
        if (!chatItem.superchat)
            delete message.s;

        const ts = chatItem.timestamp ? new Date(chatItem.timestamp) : new Date();
        const msgJson = JSON.stringify(message);

        // 비동기 압축으로 이벤트 루프 블로킹 방지
        zlib.deflateRaw(Buffer.from(msgJson, 'utf8'), (err, compressed) => {
            if (err) {
                console.error('[YT] Compress error:', err.message);
                return;
            }
            savedataDB({
                sid,
                channel: id,
                authorId: NVL(chatItem.author?.channelId),
                author: NVL(chatItem.author?.name),
                authorAlt: NVL(chatItem.author?.thumbnail?.alt),
                thumb: NVL(chatItem.author?.thumbnail?.url),
                message: compressed,
                msgdata: tokMessage((chatItem.message || []).map(e => e.text).join(' ')),
                flag: youtube_flag(chatItem),
                timestamp: ts
            });
        });
    });

    liveChat.on("error", (err) => {
        const msg = err?.message || '';
        // 방송 자체가 없을 때만 삭제 (파싱 에러는 무시)
        if (msg === "Live Stream was not found") {
            deleteLive(id);
            console.log("[YT] Not found, deleted:", id);
            return;
        }
        // 파싱/재접속 관련 에러는 live-chat.js 내부에서 처리하므로 무시
        if (msg.includes("was not found") || msg.includes("liveChatContinuation")) {
            return;
        }
        if (err.status == 400 || err.status == 403) {
            if (++yt[id].msgerr >= 10) {
                yt[id].obj && yt[id].obj.stop();
            }
        }
        // 503(일시적 오류)은 무시, 나머지만 간략 출력
        if (err.status != 503) {
            console.error(`[YT] Error ${id}: ${errSummary(err)}`);
        }
    });

    liveChat.on("end", (reason) => {
        console.log("[YT] Disconnected:", id, reason);
        if (++yt[id].error < 5) {
            const err = yt[id].error;
            setTimeout(createLive, 1000 * (err * err), id, false);
            console.log("[YT] Reconnecting:", id, `(attempt ${err})`);
        } else {
            deleteLive(id);
            console.log("[YT] Max retries, deleted:", id);
        }
    });

    liveChat.start();
    await delay(1000);
};

function deleteLive(id) {
    if (!id)
        return;

    const data = readData();
    const filtered = data.filter(item => item.id !== id);

    if (data.length === filtered.length)
        return { message: 'Item not found', data };

    if (yt[id]) {
        yt[id].error = 99999;
        yt[id].obj && yt[id].obj.stop();
        yt[id].obj = null;
    }

    writeData(filtered);
    return { message: 'Item deleted', data };
}

// webpage
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    if (req.method === 'OPTIONS')
        return res.sendStatus(200);
    next();
});

app.get("/", (req, res) => {
    res.sendFile(path.join(__dirname, "index.html"));
});

app.get('/create/:id', (req, res) => {
    const data = readData();
    const id = req.params.id;

    if (data.find(item => item.id === id)) {
        res.json({ message: 'Already Exists', data });
        return;
    }

    data.push({ id });
    createLive(id, true);

    writeData(data);
    res.json({ message: 'Item added', data });
});

app.get('/delete/:id', (req, res) => {
    const id = req.params.id;
    res.json(deleteLive(id));
});

async function queryChat(req, res, direction) {
    const isDown = direction === "down";
    const start = parseInt(req.query.start) || (isDown ? 999999999999999 : 0);

    let filters = [];
    try { filters = JSON.parse(req.query.filters || "[]"); } catch (e) { filters = []; }

    const idConditions = [];
    const idParams = [];

    idConditions.push(isDown ? "c.id < ?" : "c.id >= ?");
    idParams.push(start);

    for (const f of filters) {
        switch (f.type) {
            case "channel":
                idConditions.push("c.channel = ?");
                idParams.push(f.value);
                break;
            case "text":
                idConditions.push("MATCH(c.msgdata) AGAINST(? IN BOOLEAN MODE)");
                idParams.push(searchTokMessage(f.value));
                break;
            case "author":
                // author는 youtube_user_names에 있으므로 서브쿼리로 nid를 먼저 찾음
                idConditions.push("c.nid IN (SELECT nid FROM youtube_user_names WHERE author = ? OR authorAlt = ?)");
                idParams.push(f.value, f.value);
                break;
            case "userId":
                // userId는 youtube_users → youtube_user_names 경유
                idConditions.push("c.nid IN (SELECT n2.nid FROM youtube_user_names n2 JOIN youtube_users u2 ON n2.uid = u2.uid WHERE u2.authorId = ?)");
                idParams.push(f.value);
                break;
            case "superchat":
                idConditions.push("(c.flag & 16) != 0");
                break;
        }
    }

    const order = isDown ? "DESC" : "ASC";

    // 2단계 쿼리: 먼저 ID만 빠르게 뽑고, 그 ID로 JOIN해서 상세 데이터 가져옴
    const idWhere = idConditions.join(" AND ").replace(/\bc\./g, 'c2.');
    const sql = `SELECT c.id, c.channel, n.author, n.authorAlt, u.authorId,
                        n.thumb AS authorThumb, c.message, c.flag, c.timestamp
                 FROM youtube_chat3 c
                 JOIN youtube_user_names n ON c.nid = n.nid
                 JOIN youtube_users u ON n.uid = u.uid
                 WHERE c.id IN (
                     SELECT sub.id FROM (
                         SELECT c2.id FROM youtube_chat3 c2
                         WHERE ${idWhere}
                         ORDER BY c2.id ${order}
                         LIMIT 100
                     ) sub
                 )
                 ORDER BY c.timestamp ${order}, c.id ${order}`;

    if (!readPool) {
        console.error('[Query] Read pool not available');
        return res.json([]);
    }

    let conn;
    try {
        conn = await getConnectionWithTimeout(readPool, DB_CONNECT_TIMEOUT);
        // 쿼리 타임아웃
        await conn.execute(`SET SESSION MAX_EXECUTION_TIME=${DB_QUERY_TIMEOUT}`);
        const [rows] = await conn.execute(sql, idParams);
        const result = rows.map(row => ({
            id: row.id,
            channel: row.channel,
            author: row.author,
            authorAlt: row.authorAlt,
            authorId: row.authorId,
            authorThumb: row.authorThumb,
            message: row.message ? row.message.toString('base64') : null,
            flag: row.flag,
            timestamp: row.timestamp
        }));
        res.json(result);
    } catch (err) {
        console.error('[Query] error:', err.message);
        if (isConnectionError(err)) {
            markDbDisconnected(poolVersion);
        }
        res.json([]);
    } finally {
        if (conn) conn.release();
    }
}

app.get('/api/user_history/:authorId', async (req, res) => {
    const authorId = req.params.authorId;
    if (!authorId) return res.status(400).json({ error: 'authorId is required' });

    if (!readPool) {
        return res.status(503).json({ error: 'Database unavailable' });
    }

    let conn;
    try {
        conn = await getConnectionWithTimeout(readPool, DB_CONNECT_TIMEOUT);
        const sql = `
            SELECT n.author, n.thumb, n.first_seen
            FROM youtube_user_names n
            JOIN youtube_users u ON n.uid = u.uid
            WHERE u.authorId = ?
            ORDER BY n.first_seen ASC
        `;
        const [rows] = await conn.execute(sql, [authorId]);
        res.json(rows);
    } catch (err) {
        console.error('[Query] User history error:', err.message);
        if (isConnectionError(err)) {
            markDbDisconnected(poolVersion);
        }
        res.status(500).json({ error: 'Database error' });
    } finally {
        if (conn) conn.release();
    }
});

app.get("/data", (req, res) => queryChat(req, res, "down"));
app.get("/udata", (req, res) => queryChat(req, res, "up"));

app.listen(PORT, () => {
    console.log(`Enter url: http://localhost:${PORT}`);
});

// ─── 프로세스 안정성: 예외로 인한 크래시 방지 ───
process.on('uncaughtException', (err) => {
    console.error('[FATAL] Uncaught exception:', err.message);
});
process.on('unhandledRejection', (reason) => {
    console.error('[FATAL] Unhandled rejection:', reason?.message || reason);
});

(async () => {
    await createPools();  // DB 연결 완료까지 대기
    const data = readData();
    for (let i = 0; i < data.length; ++i) {
        await createLive(data[i].id, true);
    }

    // ─── 헬스체크: DB 연결 끊김 시 자동 재연결 ───
    // 버퍼 유무 관계없이 동작 (쿼리 엔드포인트 복구 포함)
    setInterval(async () => {
        if (dbConnected && writePool) {
            // 연결 상태에서도 주기적으로 ping 체크 (조용한 끊김 감지)
            try {
                const conn = await getConnectionWithTimeout(writePool, DB_CONNECT_TIMEOUT);
                await conn.ping();
                conn.release();
            } catch (err) {
                console.error(`[DB] Health ping failed: ${err.message}`);
                markDbDisconnected(poolVersion);
            }
        } else if (!dbConnected) {
            console.log(`[DB] Health check: reconnecting... (${dbBuffer.length} buffered)`);
            await createPools();
        }
    }, DB_HEALTH_INTERVAL);
})();