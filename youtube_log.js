const { LiveChat } = require("./youtube-chat");
const express = require("express");
const mysql = require("mysql2/promise");
const crypto = require("crypto");
const path = require("path");
const fs = require('fs');
const LZString = require("lz-string");
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

function randomString(length) {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
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

let dbConnectionPromise = null;

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

function createConnectionPromise() {
    return mysql.createConnection({
        host: DB_HOST,
        user: DB_USER,
        password: DB_PASS,
        database: DB_SCHEMA,
    }).then(conn => {
        console.log('DB Connected successfully.');
        dbConnected = true;

        // 연결 즉시 버퍼 데이터 flush
        if (dbBuffer.length > 0) {
            console.log(`DB reconnected, flushing ${dbBuffer.length} buffered items...`);
            flushDbBuffer();
        }

        conn.on('error', (err) => {
            console.error('DB Connection Error:', err);
            if (err.code === 'PROTOCOL_CONNECTION_LOST' || err.code === 'ECONNRESET' || err.fatal) {
                console.log('DB Connection lost. Clearing cached connection...');
                dbConnected = false;
                dbConnectionPromise = null;

                // 기존 연결 객체는 파기 시도
                if (conn && conn.destroy) {
                    conn.destroy();
                }
            }
        });

        return conn;
    }).catch(err => {
        console.error('DB Connection failed. Retrying in 2 seconds...', err.message);
        dbConnected = false;
        dbConnectionPromise = null; // 실패 시 다시 시도할 수 있도록 무효화
        return new Promise((resolve, reject) => {
            setTimeout(() => {
                // 재시도 시 새 promise를 생성하여 진행
                createConnectionPromise().then(resolve).catch(reject);
            }, 2000);
        });
    });
}

function connectDB() {
    if (!dbConnectionPromise) {
        dbConnectionPromise = createConnectionPromise();
    }
    return dbConnectionPromise;
}

async function flushDbBuffer() {
    if (isFlushing || dbBuffer.length === 0) return;
    isFlushing = true;
    console.log(`[Buffer] Flushing ${dbBuffer.length} items...`);

    while (dbBuffer.length > 0 && dbConnected) {
        const item = dbBuffer[0];
        try {
            const conn = await connectDB();
            await conn.execute(`INSERT INTO youtube_chat2 (
                sid, channel, author, authorAlt,
                authorId, authorThumb, message, msgdata,
                flag, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, item.data);
            dbBuffer.shift();
        } catch (err) {
            if (err && err.code === 'ER_DUP_ENTRY') {
                dbBuffer.shift(); // 중복 항목은 제거
                continue;
            }
            if (isConnectionError(err)) {
                console.error('[Buffer] DB connection lost during flush, pausing...');
                dbConnected = false;
                dbConnectionPromise = null;
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

async function savedataDB(ids, data) {
    // DB 연결이 끊긴 상태면 버퍼에 저장
    if (!dbConnected) {
        dbBuffer.push({ ids, data });
        console.log(`[Buffer] DB offline, buffered (${dbBuffer.length} items)`);
        return;
    }

    connectDB()
        .then((conn) => {
            return conn.execute(`INSERT INTO youtube_chat2 (
                sid, channel, author, authorAlt,
                authorId, authorThumb, message, msgdata,
                flag, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, data);
        })
        .catch((err) => {
            if (err && err.code === 'ER_DUP_ENTRY') return;

            console.error('DB Insert Error:', err.message);

            // 연결 에러면 상태 변경 후 버퍼에 저장
            if (isConnectionError(err)) {
                dbConnected = false;
                dbConnectionPromise = null;
            }
            dbBuffer.push({ ids, data });
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
        const hash = crypto.createHash("sha256").update(jstr).digest("hex");
        const message = { m: chatItem.message, s: chatItem.superchat };
        if (!chatItem.superchat)
            delete message.s;
        savedataDB(randomString(50), [
            hash, id,
            NVL(chatItem.author?.name),
            NVL(chatItem.author?.thumbnail?.alt),
            NVL(chatItem.author?.channelId),
            NVL(chatItem.author?.thumbnail?.url),
            Buffer.from(LZString.compressToUTF16(JSON.stringify(message)), 'utf16le'),
            tokMessage((chatItem.message || []).map(e => e.text).join(' ')),
            youtube_flag(chatItem),
            NVL(chatItem.timestamp)
        ]);
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

    const conditions = [];
    const params = [];

    conditions.push(isDown ? "id < ?" : "id >= ?");
    params.push(start);

    for (const f of filters) {
        switch (f.type) {
            case "channel":
                conditions.push("channel = ?");
                params.push(f.value);
                break;
            case "text":
                conditions.push("MATCH(msgdata) AGAINST(? IN BOOLEAN MODE)");
                params.push(searchTokMessage(f.value));
                break;
            case "author":
                conditions.push("(author = ? OR authorAlt = ?)");
                params.push(f.value, f.value);
                break;
            case "userId":
                conditions.push("authorId = ?");
                params.push(f.value);
                break;
            case "superchat":
                conditions.push("(flag & 16) != 0");
                break;
        }
    }

    const where = conditions.join(" AND ");
    const order = isDown ? "DESC" : "ASC";
    const sql = `SELECT id, channel, author, authorAlt, authorId,
                        authorThumb, message, flag, timestamp
                 FROM youtube_chat2
                 WHERE ${where}
                 ORDER BY id ${order}
                 LIMIT 100`;

    try {
        const conn = await connectDB();
        const [rows] = await conn.execute(sql, params);
        res.json(rows);
    } catch (err) {
        console.error("Query error:", err.message);
        res.json([]);
    }
}

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
            connectDB(); // 성공 시 createConnectionPromise에서 flushDbBuffer 호출
        }
    }, 5000);
})();