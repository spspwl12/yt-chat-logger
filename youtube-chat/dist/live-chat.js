"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.LiveChat = void 0;
const events_1 = require("events");
const requests_1 = require("./requests");
const NETWORK_ERROR_CODES = [
    'ENOTFOUND', 'ECONNREFUSED', 'ETIMEDOUT', 'ENETUNREACH',
    'ECONNRESET', 'ECONNABORTED', 'ERR_NETWORK', 'EAI_AGAIN'
];
/**
 */
class LiveChat extends events_1.EventEmitter {
    constructor(id, interval = 1000) {
        super();
        this._observer = null;
        this._options = null;
        this._interval = interval;
        this._id = null;
        this._isReconnecting = false;
        this._continuationErrorCount = 0;
        this._maxContinuationErrors = 3;
        this._stopped = false;  // stop() 호출 여부 추적
        this._executing = false; // execute 동시 실행 방지
        if (!id || (!("channelId" in id) && !("liveId" in id) && !("handle" in id))) {
            throw TypeError("Required channelId or liveId or handle.");
        }
        if ("liveId" in id) {
            this.liveId = id.liveId;
        }
        this._id = id;
    }
    start() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this._observer) {
                return false;
            }
            this._stopped = false;
            const maxStartRetries = 5;
            for (let attempt = 0; attempt < maxStartRetries; attempt++) {
                if (this._stopped) return false;
                try {
                    const options = yield (0, requests_1.fetchLivePage)(this._id);
                    this.liveId = options.liveId;
                    this._options = options;
                    this._observer = setInterval(() => this._execute(), this._interval);
                    this.emit("start", this.liveId);
                    return true;
                }
                catch (err) {
                    this.emit("error", err);
                    // 방송 종료/없음 에러는 재시도 안 함
                    const msg = (err && err.message) || "";
                    if (msg.includes("finished live") || msg.includes("Live Stream was not found")) {
                        return false;
                    }
                    if (attempt < maxStartRetries - 1) {
                        const backoff = Math.min(2000 * Math.pow(2, attempt), 16000);
                        this.emit("reconnect", { attempt: attempt + 1, maxAttempts: maxStartRetries, nextRetryMs: backoff });
                        yield new Promise(resolve => setTimeout(resolve, backoff));
                    }
                }
            }
            this.emit("error", new Error("start() failed after " + maxStartRetries + " retries"));
            return false;
        });
    }
    stop(reason) {
        this._stopped = true;
        this._isReconnecting = false;  // reconnect 루프도 중단
        if (this._observer) {
            clearInterval(this._observer);
            this._observer = null;
            this.emit("end", reason);
        }
    }
    _isNetworkError(err) {
        if (!err) return false;
        const code = err.code || '';
        const message = (err.message || '').toLowerCase();
        if (NETWORK_ERROR_CODES.includes(code)) return true;
        if (message.includes('network') || message.includes('timeout') ||
            message.includes('socket hang up') || message.includes('getaddrinfo')) return true;
        if (err.response === undefined && err.request) return true;
        return false;
    }
    _reconnect() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this._isReconnecting) return;
            this._isReconnecting = true;
            // 기존 polling 중지
            if (this._observer) {
                clearInterval(this._observer);
                this._observer = null;
            }
            const FATAL_MESSAGES = ["finished live", "Live Stream was not found"];
            let retryCount = 0;
            while (!this._stopped) {
                retryCount++;
                const backoff = Math.min(3000 * Math.pow(2, Math.min(retryCount - 1, 5)), 60000);
                this.emit("reconnect", { attempt: retryCount, nextRetryMs: backoff });
                yield new Promise(resolve => setTimeout(resolve, backoff));
                if (this._stopped) break;
                try {
                    const options = yield (0, requests_1.fetchLivePage)(this._id);
                    this.liveId = options.liveId;
                    this._options = options;
                    this._isReconnecting = false;
                    this._continuationErrorCount = 0;
                    this._observer = setInterval(() => this._execute(), this._interval);
                    this.emit("start", this.liveId);
                    return;
                }
                catch (err) {
                    const msg = (err && err.message) || "";
                    // 방송 종료/없음 → 완전 중단
                    if (FATAL_MESSAGES.some(f => msg.includes(f))) {
                        this._isReconnecting = false;
                        this.emit("end", msg);
                        return;
                    }
                    // 파싱 에러(Client Version, API Key, Continuation 등)는 조용히 재시도
                }
            }
            this._isReconnecting = false;
        });
    }
    _execute() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this._isReconnecting || this._stopped) return;
            if (this._executing) return;  // 이전 execute가 아직 실행 중이면 스킵
            this._executing = true;
            try {
                if (!this._options) {
                    const message = "Not found options";
                    this.emit("error", new Error(message));
                    this.stop(message);
                    return;
                }
                const [chatItems, continuation] = yield (0, requests_1.fetchChat)(this._options);
                this._continuationErrorCount = 0;
                chatItems.forEach((chatItem) => this.emit("chat", chatItem));
                if (continuation) {
                    this._options.continuation = continuation;
                }
            }
            catch (err) {
                if (this._isNetworkError(err)) {
                    this._reconnect();
                    return;
                }
                // liveChatContinuation 에러 연속 발생 시 재접속 시도
                if (err && err.code === "LIVE_CHAT_CONTINUATION_NOT_FOUND") {
                    this._continuationErrorCount++;
                    if (this._continuationErrorCount >= this._maxContinuationErrors) {
                        this._continuationErrorCount = 0;
                        this._reconnect();
                    }
                    return;
                }
                // HTTP 403/401 → YouTube가 차단, 재접속 시도
                if (err && err.response && (err.response.status === 403 || err.response.status === 401)) {
                    this._reconnect();
                    return;
                }
                this.emit("error", err);
            }
            finally {
                this._executing = false;
            }
        });
    }
}
exports.LiveChat = LiveChat;