const { LiveChat } = require("youtube-chat");
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
const pending = [];

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

function createConnectionPromise() {
    return mysql.createConnection({
        host: DB_HOST,
        user: DB_USER,
        password: DB_PASS,
        database: DB_SCHEMA,
    }).then(conn => {
        console.log('DB Connected successfully.');

        conn.on('error', (err) => {
            console.error('DB Connection Error:', err);
            if (err.code === 'PROTOCOL_CONNECTION_LOST' || err.code === 'ECONNRESET' || err.fatal) {
                console.log('DB Connection lost. Clearing cached connection and actively reconnecting...');
                dbConnectionPromise = null; // 기존 프로미스 무효화

                // 기존 연결 객체는 파기 시도
                if (conn && conn.destroy) {
                    conn.destroy();
                }

                // 즉시 백그라운드에서 재연결 시도
                connectDB();
            }
        });

        return conn;
    }).catch(err => {
        console.error('DB Connection failed. Retrying in 2 seconds...', err.message);
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

async function savedataDB(ids, data) {
    let c;
    connectDB()
        .then((conn) => {
            c = conn;
            return conn.execute(`INSERT INTO youtube_chat2 (
                sid, channel, author, authorAlt, 
                authorId, authorThumb, message, msgdata,
                flag, timestamp) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, data);
        })
        .then((e) => {
            const index = pending.length <= 0 ? -1 : pending.findIndex(e => e.ids == ids);
            if (index !== -1) {
                console.log("pending insert -> ", data);
                pending.splice(index, 1);
            }
        })
        .catch((err) => {
            if (err) {
                if (err.code === 'PROTOCOL_CONNECTION_LOST' || err.code === 'ECONNRESET' || err.fatal) {
                    console.log('DB Connection lost during query. Triggering reconnect...');
                    dbConnectionPromise = null;
                    if (c && c.destroy) c.destroy();
                    connectDB();
                } else if (err.code !== "ER_DUP_ENTRY") {
                    console.error("Error: ", err);
                    const index = pending.length <= 0 ? -1 : pending.findIndex(e => e.ids == ids);
                    if (index === -1)
                        pending.push({ count: 120, ids, data });
                }
            }
        })
        .finally(() => {
        });
};

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

app.get("/data", (req, res) => {
    let c;
    let p;
    const channel = req.query.channel || "";
    const search = req.query.search || "";
    const start = parseInt(req.query.start) || 999999999999999;
    const superchat = search === "superchat" ? 16 : 0;

    connectDB()
        .then((conn) => {
            c = conn;
            return conn.execute(
                `SELECT id, channel, author, authorAlt, authorId,
                        authorThumb, message, flag, timestamp
                 FROM youtube_chat2
                 WHERE id < ?
                   AND ( ? = '' OR channel = ? )
                   AND ( ? = '' OR MATCH(msgdata) AGAINST(? IN BOOLEAN MODE)
                                 OR author = ? OR authorAlt = ? OR authorId = ? OR flag & ? != 0 )
                 ORDER BY id DESC
                 LIMIT 100`,
                [
                    start,
                    channel, channel,
                    search, searchTokMessage(search),
                    search, search, search, superchat
                ]
            );
        })
        .then(([rows]) => {
            p = rows;
        })
        .catch((err) => {
            if (err) {
                if (err.code === 'PROTOCOL_CONNECTION_LOST' || err.code === 'ECONNRESET' || err.fatal) {
                    console.log('DB Connection lost during API query. Triggering reconnect...');
                    dbConnectionPromise = null;
                    if (c && c.destroy) c.destroy();
                    connectDB();
                } else {
                    console.error(err);
                }
            }
        })
        .finally(() => {
            if (p) res.json(p);
        });
});

app.get("/udata", (req, res) => {
    let c;
    let p;
    const channel = req.query.channel || "";
    const search = req.query.search || "";
    const start = parseInt(req.query.start) || 0;
    const superchat = search === "superchat" ? 16 : 0;

    connectDB()
        .then((conn) => {
            c = conn;
            return conn.execute(
                `SELECT id, channel, author, authorAlt, authorId,
                        authorThumb, message, flag, timestamp
                 FROM youtube_chat2
                 WHERE id >= ?
                   AND ( ? = '' OR channel = ? )
                   AND ( ? = '' OR MATCH(msgdata) AGAINST(? IN BOOLEAN MODE)
                                 OR author = ? OR authorAlt = ? OR authorId = ? OR flag & ? != 0 )
                 ORDER BY id ASC
                 LIMIT 100`,
                [
                    start,
                    channel, channel,
                    search, searchTokMessage(search),
                    search, search, search, superchat
                ]
            );
        })
        .then(([rows]) => {
            p = rows;
        })
        .catch((err) => {
            if (err) {
                if (err.code === 'PROTOCOL_CONNECTION_LOST' || err.code === 'ECONNRESET' || err.fatal) {
                    console.log('DB Connection lost during API query. Triggering reconnect...');
                    dbConnectionPromise = null;
                    if (c && c.destroy) c.destroy();
                    connectDB();
                } else {
                    console.error(err);
                }
            }
        })
        .finally(() => {
            if (p) res.json(p);
        });
});

app.listen(PORT, () => {
    console.log(`Enter url: http://localhost:${PORT}`);
});

(async () => {
    const data = readData();
    for (let i = 0; i < data.length; ++i) {
        await createLive(data[i].id, true);
    }
    setInterval(() => {
        for (let i = pending.length - 1, e; i >= 0; i--) {
            if (!(e = pending[i])) {
                continue;
            } else if (--e.count > 0) {
                savedataDB(e.ids, e.data);
            } else {
                pending.splice(i, 1);
            }
        }
    }, 1000);
})();