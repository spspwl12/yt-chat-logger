const { LiveChat } = require("./youtube-chat");
const express = require("express");
const mysql = require("mysql2/promise");
const crypto = require("crypto");
const path = require("path");
const fs = require('fs');
const zlib = require("zlib");
const mecab = require('./mecab-ya.js');

const DATA_FILE = './data.json';

const app = express();
const PORT = 3000;

const DB_HOST = "127.0.0.1";
const DB_USER = "root";
const DB_PASS = "";
const DB_SCHEMA = "DATA";


const yt = {};
const dbBuffer = [];
let dbConnected = false;
let isFlushing = false;

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

let pool = null;

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
        msg.includes('connection closed') || msg.includes('socket hang up')) return true;
    if (err.fatal) return true;
    return false;
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

function createPool() {
    pool = mysql.createPool({
        host: DB_HOST,
        user: DB_USER,
        password: DB_PASS,
        database: DB_SCHEMA,
        waitForConnections: true,
        connectionLimit: 10,
        queueLimit: 0,
        enableKeepAlive: true,
        keepAliveInitialDelay: 10000,
    });
    console.log('DB Pool created.');
    dbConnected = true;

    // 풀 생성 후 인덱스 확인 + 버퍼 flush
    pool.getConnection().then(async (conn) => {
        await ensureIndexes(conn);
        conn.release();
        if (dbBuffer.length > 0) {
            console.log(`DB pool ready, flushing ${dbBuffer.length} buffered items...`);
            flushDbBuffer();
        }
    }).catch(err => {
        console.error('DB initial connection failed:', err.message);
        dbConnected = false;
    });

    return pool;
}

function connectDB() {
    if (!pool) {
        createPool();
    }
    return pool;
}

async function insertChat(thePool, chatData) {
    const { sid, channel, authorId, author, authorAlt, thumb, message, msgdata, flag, timestamp } = chatData;

    // 캐시에서 uid, nid를 모두 찾으면 커넥션 1회만 사용 (INSERT만)
    let userEntry = userCache.get(authorId);
    const profileKey = author + '\0' + thumb;
    let nid = userEntry?.profiles?.get(profileKey);

    // 캐시 미스 시에만 조회 쿼리 실행
    if (!userEntry || !nid) {
        const conn = await thePool.getConnection();
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
    if (isFlushing || dbBuffer.length === 0) return;
    isFlushing = true;
    console.log(`[Buffer] Flushing ${dbBuffer.length} items...`);

    while (dbBuffer.length > 0 && dbConnected) {
        const chatData = dbBuffer[0];
        try {
            await insertChat(connectDB(), chatData);
            dbBuffer.shift();
        } catch (err) {
            if (err && err.code === 'ER_DUP_ENTRY') {
                dbBuffer.shift(); // 중복 항목은 제거
                continue;
            }
            if (isConnectionError(err)) {
                console.error('[Buffer] DB connection lost during flush, pausing...');
                dbConnected = false;
                pool = null;
                break;
            }
            // 기타 에러는 로그 후 건너뜀
            console.error('[Buffer] Insert error, skipping item:', err.message);
            dbBuffer.shift();
        }
    }

    isFlushing = false;
    if (dbBuffer.length === 0) {
        console.log('[Buffer] All buffered items flushed successfully.');
    } else {
        console.log(`[Buffer] Flush paused, ${dbBuffer.length} items remaining.`);
    }
}

async function savedataDB(chatData) {
    // DB 연결이 끊긴 상태면 버퍼에 저장
    if (!dbConnected) {
        dbBuffer.push(chatData);
        console.log(`[Buffer] DB offline, buffered (${dbBuffer.length} items)`);
        return;
    }

    insertChat(connectDB(), chatData)
        .catch((err) => {
            if (err && err.code === 'ER_DUP_ENTRY') return;

            console.error('DB Insert Error:', err.message);

            // 연결 에러면 상태 변경 후 버퍼에 저장
            if (isConnectionError(err)) {
                dbConnected = false;
                pool = null;
            }
            dbBuffer.push(chatData);
            console.log(`[Buffer] Buffered (${dbBuffer.length} items)`);
        });
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
        console.log("Connected Youtube: ", id);
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
                console.error('Compress error:', err.message);
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
        if (err.message.includes("was not found")) {
            deleteLive(id);
            console.log("Not Found Live ID /  Delete Youtube: ", id);
            return;
        }
        if (err.status == 400 || err.status == 403) {
            if (++yt[id].msgerr >= 10) {
                yt[id].obj && yt[id].obj.stop();
            }
        }
        if (err.status != 503) {
            console.error("Error Youtube: ", id, err);
        }
    });

    liveChat.on("end", (reason) => {
        console.log("Disconnected Youtube: ", id, reason);
        if (++yt[id].error < 5) {
            const err = yt[id].error;
            setTimeout(createLive, 1000 * (err * err), id, false);
            console.log("Try Reconnect Youtube: ", id);
        } else {
            deleteLive(id);
            console.log("Error Delete Youtube: ", id);
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

    let conn;
    try {
        conn = await connectDB().getConnection();
        // 쿼리 타임아웃 10초
        await conn.execute('SET SESSION MAX_EXECUTION_TIME=10000');
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
        console.error("Query error:", err.message);
        res.json([]);
    } finally {
        if (conn) conn.release();
    }
}

app.get('/api/user_history/:authorId', async (req, res) => {
    const authorId = req.params.authorId;
    if (!authorId) return res.status(400).json({ error: 'authorId is required' });

    let conn;
    try {
        conn = await connectDB().getConnection();
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
        console.error("User history query error:", err.message);
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

(async () => {
    const data = readData();
    for (let i = 0; i < data.length; ++i) {
        await createLive(data[i].id, true);
    }
    // DB 연결 끊김 시 주기적으로 재연결 시도 및 버퍼 flush
    setInterval(() => {
        if (!dbConnected && dbBuffer.length > 0) {
            console.log(`[Buffer] Attempting DB reconnect (${dbBuffer.length} items buffered)...`);
            pool = null;
            connectDB(); // 풀 재생성 시 자동으로 flushDbBuffer 호출
        }
    }, 5000);
})();