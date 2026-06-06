// ═══════════════════════════════════════════════════════════════
// DB Worker: 버퍼 flush 전용 (독립 스레드)
// ═══════════════════════════════════════════════════════════════
const { parentPort, workerData } = require('worker_threads');
const mysql = require("mysql2/promise");
const fs = require('fs');
const path = require('path');

const BUFFER_FILE = path.join(__dirname, '../db_buffer.ndjson');

const DB_HOST = workerData.DB_HOST;
const DB_USER = workerData.DB_USER;
const DB_PASS = workerData.DB_PASS;
const DB_SCHEMA = workerData.DB_SCHEMA;

const DB_CONNECT_TIMEOUT = 60000;
const DB_BUFFER_LIMIT = 500000;
const BATCH_SIZE = 1000;
const FLUSH_INTERVAL = 50;
const COMPACT_THRESHOLD = 10000;

let dbBuffer = loadBufferFromFile();
let flushIndex = 0;
let dbConnected = false;
let isFlushing = false;
let poolVersion = 0;
let flushTimer = null;
let writePool = null;
let isSyncing = false;

const userCache = new Map();

// ─── 버퍼 파일 로드 ───
function loadBufferFromFile() {
    try {
        if (!fs.existsSync(BUFFER_FILE)) return [];
        const content = fs.readFileSync(BUFFER_FILE, 'utf8').trim();
        if (!content) return [];
        const items = content.split('\n').map(line => {
            try {
                const obj = JSON.parse(line);
                if (obj.message && typeof obj.message === 'string') {
                    obj.message = Buffer.from(obj.message, 'base64');
                }
                if (obj.timestamp && typeof obj.timestamp === 'string') {
                    obj.timestamp = new Date(obj.timestamp);
                }
                return obj;
            } catch { return null; }
        }).filter(Boolean);
        console.log(`[DBWorker] Loaded ${items.length} items from buffer file`);
        return items;
    } catch (err) {
        console.error('[DBWorker] Failed to load buffer file:', err.message);
        return [];
    }
}

// ─── 버퍼 파일 동기화 ───
function syncBufferFile() {
    if (isSyncing) return;
    try {
        const pending = dbBuffer.slice(flushIndex);
        if (pending.length === 0) {
            if (fs.existsSync(BUFFER_FILE)) {
                fs.unlink(BUFFER_FILE, () => { });
            }
            return;
        }
        isSyncing = true;
        const lines = pending.map(item => {
            const obj = { ...item };
            if (Buffer.isBuffer(obj.message)) {
                obj.message = obj.message.toString('base64');
            }
            return JSON.stringify(obj);
        });
        fs.writeFile(BUFFER_FILE, lines.join('\n') + '\n', (err) => {
            isSyncing = false;
            if (err) console.error('[DBWorker] Failed to sync buffer file:', err.message);
        });
    } catch (err) {
        isSyncing = false;
        console.error('[DBWorker] Error syncing buffer:', err.message);
    }
}



function compactBuffer() {
    if (flushIndex >= COMPACT_THRESHOLD) {
        dbBuffer = dbBuffer.slice(flushIndex);
        flushIndex = 0;
    }
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

function isConnectionError(err) {
    if (!err) return false;
    const code = err.code || '';
    const msg = (err.message || '').toLowerCase();
    const codes = ['PROTOCOL_CONNECTION_LOST', 'ECONNRESET', 'ECONNREFUSED',
        'ETIMEDOUT', 'ENETUNREACH', 'ENOTFOUND', 'EPIPE', 'ECONNABORTED'];
    if (codes.includes(code)) return true;
    if (msg.includes('connection lost') || msg.includes('cannot enqueue') ||
        msg.includes('connection closed') || msg.includes('socket hang up') ||
        msg.includes('timeout')) return true;
    if (err.fatal) return true;
    return false;
}

async function ensureIndexes(conn) {
    const indexDefs = [
        `ALTER TABLE youtube_chat3 ADD INDEX idx_channel_id (channel, id)`,
        `ALTER TABLE youtube_chat3 ADD INDEX idx_nid_id (nid, id)`,
        `ALTER TABLE youtube_chat3 ADD INDEX idx_channel_nid_id (channel, nid, id)`,
        `ALTER TABLE youtube_user_names ADD INDEX idx_uid_author_thumb (uid, author, thumb)`,
        `ALTER TABLE youtube_user_names ADD INDEX idx_author (author)`,
    ];
    for (const ddl of indexDefs) {
        try {
            await conn.execute(ddl);
        } catch (e) {
            if (e.code !== 'ER_DUP_KEYNAME') {
                console.error('[DBWorker] Index failed:', e.message);
            }
        }
    }
}

function makePool(connLimit) {
    return mysql.createPool({
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
}

async function destroyPool(p) {
    if (!p) return;
    try {
        const pool = p.pool || p;
        if (pool._freeConnections) {
            for (const conn of pool._freeConnections.toArray()) {
                try { conn.destroy(); } catch { }
            }
        }
        if (pool._allConnections) {
            for (const conn of pool._allConnections.toArray()) {
                try { conn.destroy(); } catch { }
            }
        }
        await p.end().catch(() => { });
    } catch { }
}

let isCreatingPool = false;

async function createPool() {
    if (isCreatingPool) return;
    isCreatingPool = true;
    dbConnected = false;

    const oldPool = writePool;
    writePool = null;
    await destroyPool(oldPool);

    poolVersion++;
    const myVersion = poolVersion;
    writePool = makePool(8);

    try {
        const conn = await getConnectionWithTimeout(writePool, DB_CONNECT_TIMEOUT);
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
        console.log(`[DBWorker] Connected (v${myVersion})`);
        parentPort.postMessage({ type: 'dbStatus', connected: true });
        
        if (dbBuffer.length - flushIndex > 0) {
            scheduleFlush();
        }
    } catch (err) {
        console.error(`[DBWorker] Connection failed (v${myVersion}): ${err.message}`);
        if (poolVersion === myVersion) {
            dbConnected = false;
            parentPort.postMessage({ type: 'dbStatus', connected: false });
        }
    } finally {
        isCreatingPool = false;
    }
}

async function insertChatBatch(thePool, items) {
    if (items.length === 0) return;

    const conn = await getConnectionWithTimeout(thePool, DB_CONNECT_TIMEOUT);
    try {
        await conn.beginTransaction();

        // 1단계: authorId 처리
        const uncachedUsers = [];
        for (let i = 0; i < items.length; i++) {
            if (!userCache.has(items[i].authorId)) {
                uncachedUsers.push({ idx: i, authorId: items[i].authorId });
            }
        }

        if (uncachedUsers.length > 0) {
            const uniqueAuthorIds = [...new Set(uncachedUsers.map(u => u.authorId))];
            const placeholders = uniqueAuthorIds.map(() => '(?)').join(',');
            await conn.execute(
                `INSERT IGNORE INTO youtube_users (authorId) VALUES ${placeholders}`,
                uniqueAuthorIds
            );

            const inPlaceholders = uniqueAuthorIds.map(() => '?').join(',');
            const [uRows] = await conn.execute(
                `SELECT uid, authorId FROM youtube_users WHERE authorId IN (${inPlaceholders})`,
                uniqueAuthorIds
            );

            for (const row of uRows) {
                if (!userCache.has(row.authorId)) {
                    userCache.set(row.authorId, { uid: row.uid, profiles: new Map() });
                }
            }
        }

        // 2단계: 프로필(nid) 처리
        const uncachedProfiles = [];
        for (const item of items) {
            const userEntry = userCache.get(item.authorId);
            if (!userEntry) continue;
            const profileKey = item.author + '\0' + item.thumb;
            if (!userEntry.profiles.has(profileKey)) {
                uncachedProfiles.push({ item, userEntry, profileKey });
            }
        }

        if (uncachedProfiles.length > 0) {
            const uniqueProfiles = new Map();
            for (const p of uncachedProfiles) {
                const key = p.userEntry.uid + '\0' + p.item.author + '\0' + p.item.thumb;
                if (!uniqueProfiles.has(key)) {
                    uniqueProfiles.set(key, p);
                }
            }

            const orConditions = [];
            const orParams = [];
            for (const [, p] of uniqueProfiles) {
                orConditions.push('(uid = ? AND author = ? AND thumb = ?)');
                orParams.push(p.userEntry.uid, p.item.author, p.item.thumb);
            }

            const [existingRows] = await conn.execute(
                `SELECT nid, uid, author, thumb FROM youtube_user_names WHERE ${orConditions.join(' OR ')}`,
                orParams
            );

            const foundSet = new Set();
            for (const row of existingRows) {
                const key = row.uid + '\0' + row.author + '\0' + row.thumb;
                foundSet.add(key);
                const matchedProfile = uniqueProfiles.get(key);
                if (matchedProfile) {
                    matchedProfile.userEntry.profiles.set(matchedProfile.profileKey, row.nid);
                }
            }

            const toInsert = [];
            for (const [key, p] of uniqueProfiles) {
                if (!foundSet.has(key)) {
                    toInsert.push(p);
                }
            }

            for (const p of toInsert) {
                const [ins] = await conn.execute(
                    'INSERT INTO youtube_user_names (uid, author, authorAlt, thumb, first_seen) VALUES (?, ?, ?, ?, ?)',
                    [p.userEntry.uid, p.item.author, p.item.authorAlt, p.item.thumb, p.item.timestamp]
                );
                p.userEntry.profiles.set(p.profileKey, ins.insertId);
            }
        }

        // 3단계: 채팅 Batch INSERT
        const chatValues = [];
        const chatParams = [];
        for (const item of items) {
            const userEntry = userCache.get(item.authorId);
            if (!userEntry) continue;
            const profileKey = item.author + '\0' + item.thumb;
            const nid = userEntry.profiles.get(profileKey);
            if (!nid) continue;

            chatValues.push('(?, ?, ?, ?, ?, ?, ?)');
            chatParams.push(item.sid, item.channel, nid, item.message, item.msgdata, item.flag, item.timestamp);
        }

        if (chatValues.length > 0) {
            await conn.execute(
                `INSERT IGNORE INTO youtube_chat3 (sid, channel, nid, message, msgdata, flag, timestamp) VALUES ${chatValues.join(',')}`,
                chatParams
            );
        }

        await conn.commit();
    } catch (err) {
        try { await conn.rollback(); } catch { }
        throw err;
    } finally {
        conn.release();
    }
}

let flushErrorCount = 0;
const FLUSH_MAX_RETRIES = 5;

async function flushDbBuffer() {
    const pendingCount = dbBuffer.length - flushIndex;
    if (isFlushing || pendingCount <= 0 || !dbConnected) return;
    isFlushing = true;
    const flushVersion = poolVersion;
    const startCount = pendingCount;

    while (flushIndex < dbBuffer.length && dbConnected && poolVersion === flushVersion) {
        const end = Math.min(flushIndex + BATCH_SIZE, dbBuffer.length);
        const batch = dbBuffer.slice(flushIndex, end);
        try {
            await insertChatBatch(writePool, batch);
            flushIndex += batch.length;
            flushErrorCount = 0;
            
            // Main thread에 진행 상황 알림
            parentPort.postMessage({
                type: 'flushProgress',
                processed: flushIndex,
                total: dbBuffer.length
            });
        } catch (err) {
            if (err && err.code === 'ER_DUP_ENTRY') {
                flushIndex += batch.length;
                flushErrorCount = 0;
                continue;
            }
            if (isConnectionError(err)) {
                console.error('[DBWorker] Connection lost during flush');
                if (poolVersion === flushVersion) {
                    dbConnected = false;
                    parentPort.postMessage({ type: 'dbStatus', connected: false });
                }
                break;
            }
            flushErrorCount++;
            if (flushErrorCount >= FLUSH_MAX_RETRIES) {
                console.error(`[DBWorker] Batch failed ${FLUSH_MAX_RETRIES} times, skipping:`, err.message);
                flushIndex += batch.length;
                flushErrorCount = 0;
            } else {
                console.error(`[DBWorker] Batch error (retry ${flushErrorCount}/${FLUSH_MAX_RETRIES}):`, err.message);
                break;
            }
        }
    }

    compactBuffer();
    isFlushing = false;
    syncBufferFile();

    const remaining = dbBuffer.length - flushIndex;
    if (remaining > 0) {
        console.log(`[DBWorker] ${remaining} items remaining`);
    }
}

function scheduleFlush() {
    if (flushTimer) return;
    flushTimer = setTimeout(() => {
        flushTimer = null;
        if (dbConnected && writePool && !isFlushing) {
            flushDbBuffer().catch(err =>
                console.error('[DBWorker] Flush error:', err.message));
        }
    }, FLUSH_INTERVAL);
}

function bufferPush(chatData) {
    dbBuffer.push(chatData);
    const pending = dbBuffer.length - flushIndex;
    
    if (pending > DB_BUFFER_LIMIT) {
        const dropped = pending - DB_BUFFER_LIMIT;
        const toBeSaved = dbBuffer.slice(flushIndex, flushIndex + dropped);
        try {
            const lines = toBeSaved.map(item => {
                const obj = { ...item };
                if (Buffer.isBuffer(obj.message)) {
                    obj.message = obj.message.toString('base64');
                }
                return JSON.stringify(obj);
            });
            const PERSISTENT_BACKUP = path.join(__dirname, '../db_persistent_backup.ndjson');
            fs.appendFileSync(PERSISTENT_BACKUP, lines.join('\n') + '\n');
        } catch (err) {
            console.error('[DBWorker] Emergency save failed:', err.message);
        }
        dbBuffer.splice(flushIndex, dropped);
        syncBufferFile();
        console.warn(`[DBWorker] Overflow, saved & dropped ${dropped} items`);
    }

    // Main thread에 버퍼 상태 전송
    if (pending % 100 === 0 || pending === 1) {
        parentPort.postMessage({
            type: 'bufferStatus',
            size: pending,
            connected: dbConnected
        });
    }

    if (dbConnected && writePool) {
        scheduleFlush();
    }
}

// ─── Main Thread로부터 메시지 수신 ───
parentPort.on('message', (msg) => {
    switch (msg.type) {
        case 'chat':
            bufferPush(msg.data);
            break;
        case 'reconnect':
            if (!dbConnected && !isCreatingPool) {
                createPool();
            }
            break;
        case 'ping':
            parentPort.postMessage({
                type: 'pong',
                bufferSize: dbBuffer.length - flushIndex,
                connected: dbConnected
            });
            break;
    }
});

// 시작 시 DB 연결
console.log('[DBWorker] Started');
createPool();
parentPort.postMessage({ type: 'ready' });

// ─── 헬스체크: DB 연결 끊김 시 자동 재연결 ───
setInterval(async () => {
    if (dbConnected && writePool) {
        try {
            const conn = await getConnectionWithTimeout(writePool, DB_CONNECT_TIMEOUT);
            await conn.ping();
            conn.release();
        } catch (err) {
            console.error(`[DBWorker] Health ping failed: ${err.message}`);
            dbConnected = false;
            parentPort.postMessage({ type: 'dbStatus', connected: false });
        }
    } else if (!dbConnected) {
        console.log(`[DBWorker] Health check: reconnecting... (${dbBuffer.length - flushIndex} buffered)`);
        await createPool();
    }
}, 5000);

// ─── 백그라운드 비상 파일 백업 (비동기) ───
setInterval(() => {
    if (!dbConnected && dbBuffer.length - flushIndex > 0) {
        syncBufferFile();
    }
}, 5000);
