// ═══════════════════════════════════════════════════════════════
// Chat Worker: 라이브 채팅 수신 전용 (독립 스레드)
// ═══════════════════════════════════════════════════════════════
const { parentPort, workerData } = require('worker_threads');
const { LiveChat } = require("./youtube-chat");
const crypto = require("crypto");
const zlib = require("zlib");
const fs = require('fs');
const path = require('path');
const mecab = require('./mecab-ya.js');

const EMERGENCY_LOG = path.join(__dirname, '../chat_emergency.ndjson');
const MAX_EMERGENCY_SIZE = 100 * 1024 * 1024; // 100MB

const yt = {};

function NVL(e) {
    return e ?? " ";
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

function tokMessage(text) {
    const tok = mecab.morphsSync(text, 'morphs');
    if (tok && tok.length > 0)
        return tok.join(' ');
    return text;
}

function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}



async function createLive(id, reset) {
    if (!id) return;

    const liveChat = new LiveChat({ liveId: id });
    if (!yt[id])
        yt[id] = { obj: null, error: 0, msgerr: 0 };

    if (reset) {
        yt[id].error = 0;
        yt[id].msgerr = 0;
    }

    yt[id].obj = liveChat;

    liveChat.on("start", (liveId) => {
        console.log("[ChatWorker] Connected:", id);
        parentPort.postMessage({ type: 'log', level: 'info', message: `Connected: ${id}` });
    });

    liveChat.on("chat", (chatItem) => {
        yt[id].error = 0;
        yt[id].msgerr = 0;
        const jstr = JSON.stringify(chatItem);
        const sid = crypto.createHash("sha256").update(jstr).digest("hex").substring(0, 32);
        const message = { m: chatItem.message, s: chatItem.superchat };
        if (!chatItem.superchat) delete message.s;

        const ts = chatItem.timestamp ? new Date(chatItem.timestamp) : new Date();
        const msgJson = JSON.stringify(message);

        // 비동기 압축으로 이벤트 루프 블로킹 방지
        zlib.deflateRaw(Buffer.from(msgJson, 'utf8'), (err, compressed) => {
            if (err) {
                console.error('[ChatWorker] Compress error:', err.message);
                return;
            }

            const chatData = {
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
            };

            // DB Worker로 전송
            parentPort.postMessage({ type: 'chat', data: chatData });
        });
    });

    liveChat.on("error", (err) => {
        const msg = err?.message || '';
        if (msg === "Live Stream was not found") {
            parentPort.postMessage({ type: 'deleteLive', id });
            console.log("[ChatWorker] Not found, requesting deletion:", id);
            return;
        }
        if (msg.includes("was not found") || msg.includes("liveChatContinuation")) {
            return;
        }
        if (++yt[id].msgerr >= 10) {
            yt[id].obj && yt[id].obj.stop();
        }
    });

    liveChat.on("end", (reason) => {
        console.log("[ChatWorker] Disconnected:", id, reason);
        if (++yt[id].error < 2) {
            const err = yt[id].error;
            setTimeout(createLive, 1000 * (err * err), id, false);
            console.log("[ChatWorker] Reconnecting:", id, `(attempt ${err})`);
        } else {
            parentPort.postMessage({ type: 'deleteLive', id });
            console.log("[ChatWorker] Max retries, requesting deletion:", id);
        }
    });

    liveChat.start();
    await delay(1000);
}

function deleteLive(id) {
    if (!id || !yt[id]) return;
    
    yt[id].error = 99999;
    yt[id].obj && yt[id].obj.stop();
    yt[id].obj = null;
    delete yt[id];
}

// ─── Main Thread로부터 명령 수신 ───
parentPort.on('message', (msg) => {
    switch (msg.type) {
        case 'createLive':
            createLive(msg.id, msg.reset);
            break;
        case 'deleteLive':
            deleteLive(msg.id);
            break;
        case 'ping':
            parentPort.postMessage({ type: 'pong' });
            break;
    }
});

console.log('[ChatWorker] Started');
parentPort.postMessage({ type: 'ready' });
