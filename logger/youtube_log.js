// ═══════════════════════════════════════════════════════════════
// Main Thread: Express HTTPS 서버 (쿼리 전용)
// Worker Threads를 사용한 3-tier 아키텍처
// ═══════════════════════════════════════════════════════════════
const { Worker } = require('worker_threads');
const express = require("express");
const helmet = require("helmet");
const compression = require('compression');
const https = require('https');
const mysql = require("mysql2/promise");
const path = require("path");
const fs = require('fs');
const zlib = require("zlib");
const mecab = require('./mecab-ya.js');

const DATA_FILE = path.join(__dirname, 'data.json');
const CONFIG_FILE = path.join(__dirname, 'config.json');

// ─── 설정 파일 로드 ───
const config = JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8'));

// ─── SSL 인증서 설정 ───
const CERT_DIR = path.join(__dirname, 'certs');
const SSL_KEY_PATH = path.join(CERT_DIR, 'server.key');
const SSL_CERT_PATH = path.join(CERT_DIR, 'server.crt');

function ensureSslCerts() {
    if (fs.existsSync(SSL_KEY_PATH) && fs.existsSync(SSL_CERT_PATH)) {
        console.log('[SSL] Using existing certificates');
        return {
            key: fs.readFileSync(SSL_KEY_PATH),
            cert: fs.readFileSync(SSL_CERT_PATH)
        };
    }

    console.log('[SSL] Generating self-signed certificate...');
    const forge = require('node-forge');
    const pki = forge.pki;

    // RSA 2048 키 쌍 생성
    const keys = pki.rsa.generateKeyPair(2048);
    const cert = pki.createCertificate();
    cert.publicKey = keys.publicKey;
    cert.serialNumber = Date.now().toString(16);
    cert.validity.notBefore = new Date();
    cert.validity.notAfter = new Date();
    cert.validity.notAfter.setFullYear(cert.validity.notBefore.getFullYear() + 10);

    const attrs = [{ name: 'commonName', value: 'localhost' }];
    cert.setSubject(attrs);
    cert.setIssuer(attrs);
    cert.setExtensions([
        { name: 'basicConstraints', cA: true },
        {
            name: 'subjectAltName', altNames: [
                { type: 2, value: 'localhost' },
                { type: 7, ip: '127.0.0.1' }
            ]
        }
    ]);

    cert.sign(keys.privateKey, forge.md.sha256.create());

    const pemKey = pki.privateKeyToPem(keys.privateKey);
    const pemCert = pki.certificateToPem(cert);

    if (!fs.existsSync(CERT_DIR)) {
        fs.mkdirSync(CERT_DIR, { recursive: true });
    }
    fs.writeFileSync(SSL_KEY_PATH, pemKey);
    fs.writeFileSync(SSL_CERT_PATH, pemCert);
    console.log('[SSL] Self-signed certificate generated and saved');

    return { key: pemKey, cert: pemCert };
}

const DB_HOST = config.db.host;
const DB_USER = config.db.user;
const DB_PASS = config.db.password;
const DB_SCHEMA = config.db.schema;

const DB_CONNECT_TIMEOUT = 60000;
const DB_QUERY_TIMEOUT = 60000;

const app = express();

app.use(helmet({
    contentSecurityPolicy: false
}));

app.use(compression());

// 하드코딩된 아이디/비밀번호 (Basic Auth) - 아무나 사이트에 들어가지 못하게 차단
if (config.auth.enabled) {
    app.use((req, res, next) => {
        const HARDCODED_ID = config.auth.id;
        const HARDCODED_PW = config.auth.password;

        const authheader = req.headers.authorization;
        if (!authheader) {
            res.setHeader('WWW-Authenticate', 'Basic realm="Secure Area"');
            return res.status(401).send('인증이 필요합니다.');
        }

        const auth = Buffer.from(authheader.split(' ')[1], 'base64').toString().split(':');
        const user = auth[0];
        const pass = auth[1];

        if (user === HARDCODED_ID && pass === HARDCODED_PW) {
            next();
        } else {
            res.setHeader('WWW-Authenticate', 'Basic realm="Secure Area"');
            return res.status(401).send('인증에 실패했습니다. 아이디 또는 비밀번호를 확인해주세요.');
        }
    });
}

// ─── Worker Threads ───
let chatWorker = null;
let dbWorker = null;
let dbWorkerReady = false;
let dbConnected = false;
let bufferSize = 0;

// ─── 쿼리 전용 Read Pool (Main Thread) ───
let readPool = null;

function readData() {
    if (!fs.existsSync(DATA_FILE))
        fs.writeFileSync(DATA_FILE, JSON.stringify([]));
    return JSON.parse(fs.readFileSync(DATA_FILE));
}

function writeData(data) {
    fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
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

// ─── DB Worker 초기화 ───
function initDbWorker() {
    dbWorker = new Worker(path.join(__dirname, './db_worker.js'), {
        workerData: {
            DB_HOST,
            DB_USER,
            DB_PASS,
            DB_SCHEMA
        }
    });

    dbWorker.on('message', (msg) => {
        switch (msg.type) {
            case 'ready':
                dbWorkerReady = true;
                console.log('[Main] DB Worker ready');
                break;
            case 'dbStatus':
                dbConnected = msg.connected;
                console.log(`[Main] DB Status: ${dbConnected ? 'CONNECTED' : 'DISCONNECTED'}`);
                break;
            case 'bufferStatus':
                bufferSize = msg.size;
                if (!msg.connected && bufferSize > 0 && bufferSize % 1000 === 0) {
                    console.log(`[Main] DB offline, buffer: ${bufferSize} items`);
                }
                break;
            case 'flushProgress':
                break;
        }
    });

    dbWorker.on('error', (err) => {
        console.error('[Main] DB Worker error:', err.message);
    });

    dbWorker.on('exit', (code) => {
        console.error(`[Main] DB Worker exited: ${code}`);
        dbWorkerReady = false;
        // 자동 재시작
        setTimeout(() => {
            console.log('[Main] Restarting DB Worker...');
            initDbWorker();
        }, 5000);
    });
}

// ─── Chat Worker 초기화 ───
function initChatWorker() {
    chatWorker = new Worker(path.join(__dirname, './chat_worker.js'));

    chatWorker.on('message', (msg) => {
        switch (msg.type) {
            case 'ready':
                console.log('[Main] Chat Worker ready');
                // 기존 라이브 채팅 복원 (순차 연결)
                const data = readData();
                const ids = data.map(item => item.id);
                if (ids.length > 0) {
                    chatWorker.postMessage({ type: 'createLiveAll', ids, reset: true });
                }
                break;
            case 'chat':
                // Chat Worker → DB Worker로 포워딩
                if (dbWorkerReady) {
                    dbWorker.postMessage({ type: 'chat', data: msg.data });
                }
                break;
            case 'deleteLive':
                deleteLive(msg.id);
                break;
            case 'log':
                console.log(`[Chat] ${msg.message}`);
                break;
        }
    });

    chatWorker.on('error', (err) => {
        console.error('[Main] Chat Worker error:', err.message);
    });

    chatWorker.on('exit', (code) => {
        console.error(`[Main] Chat Worker exited: ${code}`);
        // 자동 재시작
        setTimeout(() => {
            console.log('[Main] Restarting Chat Worker...');
            initChatWorker();
        }, 5000);
    });
}

function deleteLive(id) {
    if (!id) return;

    const data = readData();
    const filtered = data.filter(item => item.id !== id);

    if (data.length === filtered.length)
        return { message: 'Item not found', data };

    if (chatWorker) {
        chatWorker.postMessage({ type: 'deleteLive', id });
    }

    writeData(filtered);
    return { message: 'Item deleted', data: filtered };
}

// ─── Read Pool 초기화 (쿼리 전용) ───
function initReadPool() {
    readPool = mysql.createPool({
        host: DB_HOST,
        user: DB_USER,
        password: DB_PASS,
        database: DB_SCHEMA,
        waitForConnections: true,
        connectionLimit: 6,
        queueLimit: 0,
        enableKeepAlive: true,
        keepAliveInitialDelay: 10000,
        connectTimeout: DB_CONNECT_TIMEOUT,
    });
    console.log('[Main] Read pool created');
}

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

    if (chatWorker) {
        chatWorker.postMessage({ type: 'createLive', id, reset: true });
    }

    writeData(data);
    res.json({ message: 'Item added', data });
});

app.get('/delete/:id', (req, res) => {
    const id = req.params.id;
    res.json(deleteLive(id));
});

app.get('/list', (req, res) => {
    res.json(readData());
});

app.get('/status', (req, res) => {
    res.json({
        dbConnected,
        bufferSize,
        chatWorkerReady: chatWorker !== null,
        dbWorkerReady
    });
});

// ─── 채널별 채팅 속도 (chat_worker 집계) ───
app.get('/rate', (req, res) => {
    if (!chatWorker) return res.json({});

    const unit = parseInt(req.query.unit) || 60;

    let responded = false;
    const timeout = setTimeout(() => {
        if (!responded) { responded = true; res.json({}); }
        chatWorker.off('message', onMsg);
    }, 2000);

    function onMsg(msg) {
        if (msg.type !== 'rateResult') return;
        clearTimeout(timeout);
        chatWorker.off('message', onMsg);
        if (!responded) { responded = true; res.json(msg.data); }
    }

    chatWorker.on('message', onMsg);
    chatWorker.postMessage({ type: 'getRate', unit });
});

// ─── 쿼리 최적화 함수들 ───
function buildFilterCondition(f) {
    let logic = f.logic || 'AND';
    if (f.type === 'text_or') { f.type = 'text'; logic = 'OR'; }
    if (f.type === 'text_not') { f.type = 'text'; logic = 'NOT'; }
    let sql = '';
    const params = [];
    switch (f.type) {
        case 'channel':
            sql = 'c.channel = ?'; params.push(f.value); break;
        case 'text': {
            const tok = searchTokMessage(f.value);
            if (tok.trim() !== '') {
                sql = 'MATCH(c.msgdata) AGAINST(? IN BOOLEAN MODE)';
                params.push(tok);
            }
            break;
        }
        case 'author':
            sql = 'c.nid IN (SELECT nid FROM youtube_user_names WHERE author = ? OR authorAlt = ?)';
            params.push(f.value, f.value);
            break;
        case 'userId':
            sql = 'c.nid IN (SELECT n2.nid FROM youtube_user_names n2 JOIN youtube_users u2 ON n2.uid = u2.uid WHERE u2.authorId = ?)';
            params.push(f.value);
            break;
        case 'superchat':
            sql = '(c.flag & 16) != 0';
            break;
    }
    return { sql, params, logic };
}

function tryUnionOptimization(filters, idCond, idParam, order) {
    let andFilters = [];
    let orBranches = [];
    let inGroup = false;
    let groupDepth = 0;

    for (const f of filters) {
        if (f.type === 'group_start') {
            groupDepth++;
            if (groupDepth > 1) return null;
            inGroup = true;
            continue;
        }
        if (f.type === 'group_end') {
            groupDepth--;
            if (groupDepth === 0) {
                inGroup = false;
                continue;
            }
        }
        if (!inGroup) {
            const cond = buildFilterCondition(f);
            if (!cond.sql) continue;
            if (cond.logic === 'OR' || cond.logic === 'NOT') return null;
            andFilters.push(cond);
        } else {
            const cond = buildFilterCondition(f);
            if (!cond.sql) continue;
            if (cond.logic === 'NOT') return null;
            orBranches.push(cond);
        }
    }

    if (orBranches.length < 2) return null;
    for (let i = 1; i < orBranches.length; i++) {
        if (orBranches[i].logic !== 'OR') return null;
    }

    let andSql = idCond;
    const andParams = [...idParam];
    for (const a of andFilters) {
        andSql += ` AND (${a.sql})`;
        andParams.push(...a.params);
    }

    const branches = orBranches.map(b => ({
        sql: `(SELECT c.id FROM youtube_chat3 c WHERE ${andSql} AND (${b.sql}) ORDER BY c.id ${order} LIMIT 100)`,
        params: [...andParams, ...b.params]
    }));

    const unionSql = branches.map(b => b.sql).join(' UNION ALL ');
    const unionParams = branches.flatMap(b => b.params);

    return {
        sql: `SELECT DISTINCT sub.id FROM (${unionSql}) sub ORDER BY sub.id ${order} LIMIT 100`,
        params: unionParams
    };
}

async function queryChat(req, res, direction) {
    const isDown = direction === "down";
    const start = parseInt(req.query.start) || (isDown ? 999999999999999 : 0);

    let filters = [];
    try {
        filters = JSON.parse(req.query.filters || "[]");
    } catch (e) {
        filters = [];
    }

    const idCond = isDown ? "c.id < ?" : "c.id >= ?";
    const idParam = [start];
    const order = isDown ? "DESC" : "ASC";

    let idQuerySql, idQueryParams;

    const unionResult = tryUnionOptimization(filters, idCond, idParam, order);

    if (unionResult) {
        idQuerySql = unionResult.sql;
        idQueryParams = unionResult.params;
    } else {
        const idConditions = [idCond];
        const idParams = [...idParam];
        let seqSqlParts = [];
        let seqParams = [];
        let expectOperator = false;
        let openCount = 0;

        for (let i = 0; i < filters.length; i++) {
            const f = filters[i];
            let logic = f.logic || 'AND';
            if (f.type === 'text_or') {
                f.type = 'text';
                logic = 'OR';
            }
            if (f.type === 'text_not') {
                f.type = 'text';
                logic = 'NOT';
            }

            if (f.type === 'group_start') {
                if (expectOperator) {
                    if (logic === 'OR') seqSqlParts.push("OR");
                    else if (logic === 'NOT') seqSqlParts.push("AND NOT");
                    else seqSqlParts.push("AND");
                } else {
                    if (logic === 'NOT') seqSqlParts.push("NOT");
                }
                seqSqlParts.push("(");
                openCount++;
                expectOperator = false;
                continue;
            }
            if (f.type === 'group_end') {
                if (openCount > 0) {
                    seqSqlParts.push(")");
                    openCount--;
                    expectOperator = true;
                }
                continue;
            }

            const cond = buildFilterCondition(f);
            if (!cond.sql) continue;

            if (expectOperator) {
                if (logic === 'OR') seqSqlParts.push("OR");
                else if (logic === 'NOT') seqSqlParts.push("AND NOT");
                else seqSqlParts.push("AND");
            } else {
                if (logic === 'NOT') seqSqlParts.push("NOT");
            }

            seqSqlParts.push(`(${cond.sql})`);
            seqParams.push(...cond.params);
            expectOperator = true;
        }

        while (openCount > 0) {
            seqSqlParts.push(")");
            openCount--;
        }

        if (seqSqlParts.length > 0) {
            let finalStr = seqSqlParts.join(" ");
            finalStr = finalStr.replace(/\(\s*\)/g, "(1=1)");
            idConditions.push(`(${finalStr})`);
            idParams.push(...seqParams);
        }

        const idWhere = idConditions.join(" AND ").replace(/\bc\./g, 'c2.');
        idQuerySql = `SELECT c2.id FROM youtube_chat3 c2 WHERE ${idWhere} ORDER BY c2.id ${order} LIMIT 100`;
        idQueryParams = idParams;
    }

    const sql = `SELECT c.id, c.channel, n.author, n.authorAlt, u.authorId,
                        n.thumb AS authorThumb, c.message, c.flag, c.timestamp
                 FROM youtube_chat3 c
                 JOIN (${idQuerySql}) sub ON c.id = sub.id
                 JOIN youtube_user_names n ON c.nid = n.nid
                 JOIN youtube_users u ON n.uid = u.uid
                 ORDER BY c.timestamp ${order}, c.id ${order}`;
    const allParams = [...idQueryParams];

    if (!readPool) {
        console.error('[Query] Read pool not available');
        return res.json([]);
    }

    let conn;
    try {
        conn = await getConnectionWithTimeout(readPool, DB_CONNECT_TIMEOUT);
        await conn.execute(`SET SESSION MAX_EXECUTION_TIME=${DB_QUERY_TIMEOUT}`);
        const [rows] = await conn.execute(sql, allParams);
        const result = rows.map(row => {
            return {
                id: row.id,
                channel: row.channel,
                author: row.author,
                authorThumb: row.authorThumb,
                authorAlt: row.authorAlt,
                authorId: row.authorId,
                message: row.message ? row.message.toString('base64') : null,
                timestamp: row.timestamp,
                flag: row.flag
            };
        });
        res.json(result);
    } catch (err) {
        console.error('[Query] Error:', err.message);
        res.status(500).json({ error: err.message });
    } finally {
        if (conn) conn.release();
    }
}

app.get('/api/user_history/:authorId', async (req, res) => {
    const authorId = req.params.authorId;
    if (!authorId) return res.status(400).json({ error: 'authorId is required' });

    if (!readPool) {
        console.error('[Query] Read pool not available');
        return res.json([]);
    }

    const sql = `
        SELECT n.author, n.thumb, n.first_seen
        FROM youtube_user_names n
        JOIN youtube_users u ON n.uid = u.uid
        WHERE u.authorId = ?
        ORDER BY n.first_seen ASC
    `;

    let conn;
    try {
        conn = await getConnectionWithTimeout(readPool, DB_CONNECT_TIMEOUT);
        const [rows] = await conn.execute(sql, [authorId]);
        res.json(rows);
    } catch (err) {
        console.error('[Query] Error:', err.message);
        res.status(500).json({ error: err.message });
    } finally {
        if (conn) conn.release();
    }
});

app.get("/data", (req, res) => queryChat(req, res, "down"));
app.get("/udata", (req, res) => queryChat(req, res, "up"));

// ─── 서버 시작 (HTTPS) ───
const sslOptions = ensureSslCerts();
const server = https.createServer(sslOptions, app);

server.listen(config.web.port, () => {
    console.log(`SERVER URL: https://localhost:${config.web.port}`);

    initReadPool();
    initDbWorker();
    initChatWorker();

    setInterval(async () => {
        if (readPool) {
            try {
                const conn = await getConnectionWithTimeout(readPool, DB_CONNECT_TIMEOUT);
                await conn.ping();
                conn.release();
            } catch (err) {
                console.error(`[Main] Read pool health ping failed: ${err.message}`);
                console.log('[Main] Restarting Read pool...');
                readPool.end().catch(() => { });
                initReadPool();
            }
        }
    }, 5000);
});

// ─── 종료 처리 ───
process.on('SIGINT', async () => {
    console.log('\n[Main] Shutting down...');

    if (chatWorker) {
        await chatWorker.terminate();
    }
    if (dbWorker) {
        await dbWorker.terminate();
    }
    if (readPool) {
        await readPool.end();
    }

    process.exit(0);
});
