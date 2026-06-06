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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.fetchLivePage = exports.fetchChat = void 0;
const axios_1 = __importDefault(require("axios"));
const parser_1 = require("./parser");

const REQUEST_TIMEOUT = 15000;  // 15초 타임아웃
const USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

function fetchChat(options) {
    return __awaiter(this, void 0, void 0, function* () {
        const url = `https://www.youtube.com/youtubei/v1/live_chat/get_live_chat?key=${encodeURIComponent(options.apiKey)}`;
        const res = yield axios_1.default.post(url, {
            context: {
                client: {
                    clientVersion: options.clientVersion,
                    clientName: "WEB",
                },
            },
            continuation: options.continuation,
        }, {
            timeout: REQUEST_TIMEOUT,
            headers: {
                "User-Agent": USER_AGENT,
                "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
            },
        });
        return (0, parser_1.parseChatData)(res.data);
    });
}
exports.fetchChat = fetchChat;
function fetchLivePage(id) {
    return __awaiter(this, void 0, void 0, function* () {
        const url = generateLiveUrl(id);
        if (!url) {
            throw TypeError("not found id");
        }
        const res = yield axios_1.default.get(url, {
            timeout: REQUEST_TIMEOUT,
            headers: {
                "User-Agent": USER_AGENT,
                "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
            },
        });
        return (0, parser_1.getOptionsFromLivePage)(res.data.toString());
    });
}
exports.fetchLivePage = fetchLivePage;
function generateLiveUrl(id) {
    if ("channelId" in id) {
        return `https://www.youtube.com/channel/${encodeURIComponent(id.channelId)}/live`;
    }
    else if ("liveId" in id) {
        return `https://www.youtube.com/watch?v=${encodeURIComponent(id.liveId)}`;
    }
    else if ("handle" in id) {
        let handle = id.handle;
        if (!handle.startsWith("@")) {
            handle = "@" + handle;
        }
        return `https://www.youtube.com/${encodeURIComponent(handle)}/live`;
    }
    return "";
}