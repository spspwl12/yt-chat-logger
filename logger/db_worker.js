// ═══════════════════════════════════════════════════════════════
// DB Worker: 버퍼 flush 전용 (독립 스레드)
// 단일 ndjson 파일 버퍼: insert 실패/느림 → 파일 저장, 정상화 → flush
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
const BATCH_SIZE = 5000;
const FLUSH_INTERVAL = 10;
const SLOW_INSERT_THRESHOLD_MS = 8000;
const MAX_CACHE_SIZE = 500000;
const OR_CHUNK_SIZE = 500;

let dbBuffer = [];        // 메모리 버퍼 (정상 상태에서 사용)
let flushIndex = 0;
let dbConnected = false;
let isFlushing = false;
let poolVersion = 0;
let flushTimer = null;
let writePool = null;
let isInsertSlow = false;  // insert 느림 상태

const userCache = new Map();

// ─── userCache 크기 제한 ───
function trimUserCache() {
    if (userCache.size <= MAX_CACHE_SIZE) return;
    const deleteCount = Math.floor(MAX_CACHE_SIZE / 2);
    let count = 0;
    for (const key of userCache.keys()) {
        if (count >= deleteCount) break;
        userCache.delete(key);
        count++;
    }
}

// ─── NDJSON 파일 유틸 ───
function serializeItem(item) {
    const obj = { ...item };
    // Buffer 또는 Uint8Array 모두 base64로 변환
    if (obj.message && (Buffer.isBuffer(obj.message) || obj.message instanceof Uint8Array)) {
        obj.message = Buffer.from(obj.message).toString('base64');
    }
    return JSON.stringify(obj);
}

function deserializeItem(line) {
    try {
        const obj = JSON.parse(line);
        if (obj.message) {
            if (typeof obj.message === 'string') {
                obj.message = Buffer.from(obj.message, 'base64');
            } else if (typeof obj.message === 'object') {
                // Uint8Array가 JSON으로 직렬화된 경우 {"0":72,"1":101,...} 복구
                obj.message = Buffer.from(Object.values(obj.message));
            }
        }
        if (obj.timestamp && typeof obj.timestamp === 'string') {
            obj.timestamp = new Date(obj.timestamp);
        }
        return obj;
    } catch { return null; }
}

// 파일에 append
function appendToFile(items) {
    try {
        const lines = items.map(serializeItem);
        fs.appendFileSync(BUFFER_FILE, lines.join('\n') + '\n');
    } catch (err) {
        console.error('[DBWorker] File append failed:', err.message);
    }
}

// 파일에서 읽어오기 (읽은 후 파일 삭제)
function loadFromFile() {
    try {
        if (!fs.existsSync(BUFFER_FILE)) return [];
        const content = fs.readFileSync(BUFFER_FILE, 'utf8').trim();
        if (!content) return [];
        const items = content.split('\n').map(deserializeItem).filter(Boolean);
        fs.unlinkSync(BUFFER_FILE);
        if (items.length > 0) {
            console.log(`[DBWorker] Loaded ${items.length} items from file`);
        }
        return items;
    } catch (err) {
        console.error('[DBWorker] File load failed:', err.message);
        return [];
    }
}

// ─── DB 연결 ───
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
    writePool = makePool(16);

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
        isInsertSlow = false;
        console.log(`[DBWorker] Connected (v${myVersion})`);
        parentPort.postMessage({ type: 'dbStatus', connected: true });

        // 연결 복구 시 파일에 저장된 데이터 복구
        const fileItems = loadFromFile();
        if (fileItems.length > 0) {
            dbBuffer.push(...fileItems);
        }

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

// ─── Insert 로직 (SELECT=conn.query, INSERT=conn.execute) ───
async function insertChatBatch(thePool, items) {
    if (items.length === 0) return;

    const conn = await getConnectionWithTimeout(thePool, DB_CONNECT_TIMEOUT);
    try {
        await conn.beginTransaction();

        // 1단계: authorId 처리
        const uncachedUsers = [];
        for (let i = 0; i < items.length; i++) {
            if (!userCache.has(items[i].authorId)) {
                uncachedUsers.push(items[i].authorId);
            }
        }

        if (uncachedUsers.length > 0) {
            const uniqueAuthorIds = [...new Set(uncachedUsers)];
            const placeholders = uniqueAuthorIds.map(() => '(?)').join(',');
            await conn.execute(
                `INSERT IGNORE INTO youtube_users (authorId) VALUES ${placeholders}`,
                uniqueAuthorIds
            );

            const inPlaceholders = uniqueAuthorIds.map(() => '?').join(',');
            const [uRows] = await conn.query(
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

            // OR 조건 chunking으로 기존 프로필 조회
            const profileEntries = [...uniqueProfiles.entries()];
            const foundSet = new Set();

            for (let ci = 0; ci < profileEntries.length; ci += OR_CHUNK_SIZE) {
                const chunk = profileEntries.slice(ci, ci + OR_CHUNK_SIZE);
                const orConditions = [];
                const orParams = [];
                for (const [, p] of chunk) {
                    orConditions.push('(uid = ? AND author = ? AND thumb = ?)');
                    orParams.push(p.userEntry.uid, p.item.author, p.item.thumb);
                }

                const [existingRows] = await conn.query(
                    `SELECT nid, uid, author, thumb FROM youtube_user_names WHERE ${orConditions.join(' OR ')}`,
                    orParams
                );

                for (const row of existingRows) {
                    const key = row.uid + '\0' + row.author + '\0' + row.thumb;
                    foundSet.add(key);
                    const matchedProfile = uniqueProfiles.get(key);
                    if (matchedProfile) {
                        matchedProfile.userEntry.profiles.set(matchedProfile.profileKey, row.nid);
                    }
                }
            }

            // 새 프로필 배치 INSERT
            const toInsert = [];
            for (const [key, p] of uniqueProfiles) {
                if (!foundSet.has(key)) {
                    toInsert.push(p);
                }
            }

            if (toInsert.length > 0) {
                // 멀티로우 INSERT IGNORE
                const profValues = [];
                const profParams = [];
                for (const p of toInsert) {
                    profValues.push('(?, ?, ?, ?, ?)');
                    profParams.push(p.userEntry.uid, p.item.author, p.item.authorAlt, p.item.thumb, p.item.timestamp);
                }
                await conn.execute(
                    `INSERT IGNORE INTO youtube_user_names (uid, author, authorAlt, thumb, first_seen) VALUES ${profValues.join(',')}`,
                    profParams
                );

                // 방금 INSERT한 프로필들의 nid 일괄 조회
                const lookupConditions = [];
                const lookupParams = [];
                for (const p of toInsert) {
                    lookupConditions.push('(uid = ? AND author = ? AND thumb = ?)');
                    lookupParams.push(p.userEntry.uid, p.item.author, p.item.thumb);
                }

                for (let li = 0; li < lookupConditions.length; li += OR_CHUNK_SIZE) {
                    const lChunk = lookupConditions.slice(li, li + OR_CHUNK_SIZE);
                    const lParams = lookupParams.slice(li * 3, (li + OR_CHUNK_SIZE) * 3);
                    const [nRows] = await conn.query(
                        `SELECT nid, uid, author, thumb FROM youtube_user_names WHERE ${lChunk.join(' OR ')}`,
                        lParams
                    );
                    for (const row of nRows) {
                        const matchKey = row.uid + '\0' + row.author + '\0' + row.thumb;
                        const matchedProfile = uniqueProfiles.get(matchKey);
                        if (matchedProfile) {
                            matchedProfile.userEntry.profiles.set(matchedProfile.profileKey, row.nid);
                        }
                    }
                }
            }
        }

        trimUserCache();

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

// ─── Flush 로직 ───
let flushErrorCount = 0;
const FLUSH_MAX_RETRIES = 5;

async function flushDbBuffer() {
    const pendingCount = dbBuffer.length - flushIndex;
    if (isFlushing || pendingCount <= 0 || !dbConnected) return;
    isFlushing = true;
    const flushVersion = poolVersion;

    while (flushIndex < dbBuffer.length && dbConnected && poolVersion === flushVersion) {
        const end = Math.min(flushIndex + BATCH_SIZE, dbBuffer.length);
        const batch = dbBuffer.slice(flushIndex, end);
        try {
            const startTime = Date.now();
            await insertChatBatch(writePool, batch);
            const elapsed = Date.now() - startTime;

            flushIndex += batch.length;
            flushErrorCount = 0;

            // insert 속도 감지
            if (elapsed > SLOW_INSERT_THRESHOLD_MS && !isInsertSlow) {
                isInsertSlow = true;
                console.warn(`[DBWorker] Insert slow (${elapsed}ms) → 파일 버퍼 전환`);
                // 아직 flush 안 된 나머지를 파일로 대피
                const remaining = dbBuffer.slice(flushIndex);
                if (remaining.length > 0) {
                    appendToFile(remaining);
                    console.log(`[DBWorker] ${remaining.length} items saved to file`);
                }
                dbBuffer = [];
                flushIndex = 0;
                break;
            } else if (elapsed <= SLOW_INSERT_THRESHOLD_MS && isInsertSlow) {
                isInsertSlow = false;
                console.log('[DBWorker] Insert speed normalized');
            }

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
                console.error('[DBWorker] Connection lost → 파일 버퍼 전환');
                // 남은 것 전부 파일로 대피
                const remaining = dbBuffer.slice(flushIndex);
                if (remaining.length > 0) {
                    appendToFile(remaining);
                    console.log(`[DBWorker] ${remaining.length} items saved to file`);
                }
                dbBuffer = [];
                flushIndex = 0;
                if (poolVersion === flushVersion) {
                    dbConnected = false;
                    parentPort.postMessage({ type: 'dbStatus', connected: false });
                }
                break;
            }
            flushErrorCount++;
            if (flushErrorCount >= FLUSH_MAX_RETRIES) {
                console.error(`[DBWorker] Batch failed ${FLUSH_MAX_RETRIES} times → 파일 저장 후 skip`);
                appendToFile(batch);
                flushIndex += batch.length;
                flushErrorCount = 0;
            } else {
                console.error(`[DBWorker] Batch error (retry ${flushErrorCount}/${FLUSH_MAX_RETRIES}):`, err.message);
                break;
            }
        }
    }

    // compact
    if (flushIndex > 0) {
        dbBuffer = dbBuffer.slice(flushIndex);
        flushIndex = 0;
    }
    isFlushing = false;

    const remaining = dbBuffer.length;
    if (remaining > 0) {
        console.log(`[DBWorker] ${remaining} items remaining in memory`);
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

// ─── 채팅 데이터 수신 ───
function bufferPush(chatData) {
    // Worker postMessage로 Buffer가 Uint8Array로 변환되므로 복원
    if (chatData.message && !Buffer.isBuffer(chatData.message)) {
        chatData.message = Buffer.from(chatData.message);
    }

    // DB 연결 안 됨 또는 insert 느림 → 파일에 직접 저장
    if (!dbConnected || isInsertSlow) {
        appendToFile([chatData]);
        return;
    }

    dbBuffer.push(chatData);

    const pending = dbBuffer.length - flushIndex;
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

// ─── 헬스체크: DB 연결 끊김 시 자동 재연결 및 느림 상태 복구 ───
setInterval(async () => {
    if (dbConnected && writePool) {
        try {
            const conn = await getConnectionWithTimeout(writePool, DB_CONNECT_TIMEOUT);
            await conn.ping();
            conn.release();

            // insert 느림 상태였다면, DB 핑이 정상이므로 복구 시도
            if (isInsertSlow) {
                console.log(`[DBWorker] Health check: trying to recover from slow state...`);
                isInsertSlow = false;
                const fileItems = loadFromFile();
                if (fileItems.length > 0) {
                    dbBuffer.push(...fileItems);
                }
                scheduleFlush();
            } else {
                // 평상시에도 파일에 남은 데이터가 있다면 복구 시도 (에러로 파일에 쓰였을 수 있음)
                if (dbBuffer.length === 0 && !isFlushing) {
                    const fileItems = loadFromFile();
                    if (fileItems.length > 0) {
                        dbBuffer.push(...fileItems);
                        scheduleFlush();
                    }
                }
            }
        } catch (err) {
            console.error(`[DBWorker] Health ping failed: ${err.message}`);
            dbConnected = false;
            parentPort.postMessage({ type: 'dbStatus', connected: false });
        }
    } else if (!dbConnected) {
        console.log(`[DBWorker] Health check: reconnecting...`);
        await createPool();
    }
}, 5000);
