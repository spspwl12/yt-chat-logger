"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.parseChatData = exports.getOptionsFromLivePage = void 0;
function getOptionsFromLivePage(data) {
    let liveId;
    const idResult = data.match(/<link rel="canonical" href="https:\/\/www.youtube.com\/watch\?v=(.+?)">/);
    if (idResult) {
        liveId = idResult[1];
    }
    else {
        throw new Error("Live Stream was not found");
    }
    const replayResult = data.match(/['"]isReplay['"]\s*:\s*(true)/);
    if (replayResult) {
        throw new Error(`${liveId} is finished live`);
    }
    let apiKey;
    const keyPatterns = [
        /['"]INNERTUBE_API_KEY['"]\s*:\s*['"](.+?)['"]/,
        /['"]innertubeApiKey['"]\s*:\s*['"](.+?)['"]/,
    ];
    for (const pattern of keyPatterns) {
        const keyResult = data.match(pattern);
        if (keyResult) {
            apiKey = keyResult[1];
            break;
        }
    }
    if (!apiKey) {
        throw new Error("API Key was not found");
    }
    let clientVersion;
    const verPatterns = [
        /['"]INNERTUBE_CLIENT_VERSION['"]\s*:\s*['"]([\w.]+?)['"]/,
        /['"]clientVersion['"]\s*:\s*['"]([\w.]+?)['"]/,
        /clientVersion\\?['"]\s*:\s*\\?['"]([\d.]+)/,
    ];
    for (const pattern of verPatterns) {
        const verResult = data.match(pattern);
        if (verResult) {
            clientVersion = verResult[1];
            break;
        }
    }
    if (!clientVersion) {
        throw new Error("Client Version was not found");
    }
    let continuation;
    const contPatterns = [
        /['"]continuation['"]\s*:\s*['"]([A-Za-z0-9_%-]+?)['"]/,
        /continuationCommand['"]\s*:\s*\{[^}]*?['"]token['"]\s*:\s*['"]([A-Za-z0-9_%-]+?)['"]/,
        /['"]reloadContinuationData['"]\s*:\s*\{[^}]*?['"]continuation['"]\s*:\s*['"]([A-Za-z0-9_%-]+?)['"]/,
    ];
    for (const pattern of contPatterns) {
        const contResult = data.match(pattern);
        if (contResult) {
            continuation = contResult[1];
            break;
        }
    }
    if (!continuation) {
        throw new Error("Continuation was not found");
    }
    return {
        liveId,
        apiKey,
        clientVersion,
        continuation,
    };
}
exports.getOptionsFromLivePage = getOptionsFromLivePage;
/** get_live_chat レスポンスを変換 */
function parseChatData(data) {
    var _a, _b;
    if (!((_a = data.continuationContents) === null || _a === void 0 ? void 0 : _a.liveChatContinuation)) {
        const err = new Error("liveChatContinuation is undefined");
        err.code = "LIVE_CHAT_CONTINUATION_NOT_FOUND";
        throw err;
    }
    const liveChatCont = data.continuationContents.liveChatContinuation;
    let chatItems = [];
    if (liveChatCont.actions) {
        chatItems = liveChatCont.actions
            .map((v) => parseActionToChatItem(v))
            .filter((v) => v !== null);
    }
    // continuations 배열 가드
    const continuations = liveChatCont.continuations;
    if (!continuations || continuations.length === 0) {
        return [chatItems, ""];
    }
    const continuationData = continuations[0];
    let continuation = "";
    if ((_b = continuationData.invalidationContinuationData) === null || _b === void 0 ? void 0 : _b.continuation) {
        continuation = continuationData.invalidationContinuationData.continuation;
    }
    else if (continuationData.timedContinuationData && continuationData.timedContinuationData.continuation) {
        continuation = continuationData.timedContinuationData.continuation;
    }
    else if (continuationData.liveChatReplayContinuationData && continuationData.liveChatReplayContinuationData.continuation) {
        continuation = continuationData.liveChatReplayContinuationData.continuation;
    }
    return [chatItems, continuation];
}
exports.parseChatData = parseChatData;
/** サムネイル → ImageItem (원본 배열 비파괴) */
function parseThumbnailToImageItem(data, alt) {
    if (!data || data.length === 0) {
        return { url: "", alt: "" };
    }
    // pop() 대신 마지막 요소 읽기 (원본 배열 보존)
    const thumbnail = data[data.length - 1];
    if (thumbnail && thumbnail.url) {
        return {
            url: thumbnail.url,
            alt: alt || "",
        };
    }
    return { url: "", alt: "" };
}
function convertColorToHex6(colorNum) {
    if (typeof colorNum !== 'number') return "#000000";
    return `#${colorNum.toString(16).slice(2).toLocaleUpperCase()}`;
}
/** メッセージ runs → MessageItem[] */
function parseMessages(runs) {
    if (!runs || !Array.isArray(runs)) return [];
    try {
        return runs.map((run) => {
            if ("text" in run) {
                return run;
            }
            else {
                // Emoji (null 가드 추가)
                const emoji = run.emoji;
                if (!emoji || !emoji.image || !emoji.image.thumbnails) {
                    return { text: "" };
                }
                const thumbnails = emoji.image.thumbnails;
                const thumbnail = thumbnails.length > 0 ? thumbnails[0] : null;
                const isCustomEmoji = Boolean(emoji.isCustomEmoji);
                const shortcut = emoji.shortcuts ? emoji.shortcuts[0] : "";
                return {
                    url: thumbnail ? thumbnail.url : "",
                    alt: shortcut,
                    isCustomEmoji: isCustomEmoji,
                    emojiText: isCustomEmoji ? shortcut : (emoji.emojiId || ""),
                };
            }
        });
    } catch {
        return [];
    }
}
/** action → Renderer */
function rendererFromAction(action) {
    if (!action.addChatItemAction) {
        return null;
    }
    const item = action.addChatItemAction.item;
    if (!item) return null;
    if (item.liveChatTextMessageRenderer) {
        return item.liveChatTextMessageRenderer;
    }
    else if (item.liveChatPaidMessageRenderer) {
        return item.liveChatPaidMessageRenderer;
    }
    else if (item.liveChatPaidStickerRenderer) {
        return item.liveChatPaidStickerRenderer;
    }
    else if (item.liveChatMembershipItemRenderer) {
        return item.liveChatMembershipItemRenderer;
    }
    return null;
}
/** action → ChatItem */
function parseActionToChatItem(data) {
    var _a, _b, _c, _d, _e, _f, _g;
    const messageRenderer = rendererFromAction(data);
    if (messageRenderer === null) {
        return null;
    }
    let message = [];
    if ("message" in messageRenderer && ((_a = messageRenderer.message) === null || _a === void 0 ? void 0 : _a.runs)) {
        message = messageRenderer.message.runs;
    }
    else if ("headerSubtext" in messageRenderer && ((_b = messageRenderer.headerSubtext) === null || _b === void 0 ? void 0 : _b.runs)) {
        message = messageRenderer.headerSubtext.runs;
    }
    const authorNameText = ((_d = (_c = messageRenderer.authorName) === null || _c === void 0 ? void 0 : _c.simpleText) !== null && _d !== void 0 ? _d : "");
    // authorPhoto 가드
    const authorThumbnails = ((_e = messageRenderer.authorPhoto) === null || _e === void 0 ? void 0 : _e.thumbnails) || [];
    const ret = {
        id: messageRenderer.id,
        author: {
            name: authorNameText,
            thumbnail: parseThumbnailToImageItem(authorThumbnails, authorNameText),
            channelId: messageRenderer.authorExternalChannelId || "",
        },
        message: parseMessages(message),
        isMembership: false,
        isOwner: false,
        isVerified: false,
        isModerator: false,
        timestamp: new Date(Number(messageRenderer.timestampUsec) / 1000),
    };
    if (messageRenderer.authorBadges) {
        for (const entry of messageRenderer.authorBadges) {
            const badge = entry.liveChatAuthorBadgeRenderer;
            if (!badge) continue;
            if (badge.customThumbnail) {
                ret.author.badge = {
                    thumbnail: parseThumbnailToImageItem(badge.customThumbnail.thumbnails || [], badge.tooltip || ""),
                    label: badge.tooltip || "",
                };
                ret.isMembership = true;
            }
            else {
                switch ((_f = badge.icon) === null || _f === void 0 ? void 0 : _f.iconType) {
                    case "OWNER":
                        ret.isOwner = true;
                        break;
                    case "VERIFIED":
                        ret.isVerified = true;
                        break;
                    case "MODERATOR":
                        ret.isModerator = true;
                        break;
                }
            }
        }
    }
    if ("sticker" in messageRenderer && messageRenderer.sticker) {
        const stickerLabel = ((_g = messageRenderer.sticker.accessibility) === null || _g === void 0 ? void 0 : _g.accessibilityData)
            ? messageRenderer.sticker.accessibility.accessibilityData.label
            : "";
        ret.superchat = {
            amount: messageRenderer.purchaseAmountText ? messageRenderer.purchaseAmountText.simpleText : "",
            color: convertColorToHex6(messageRenderer.backgroundColor),
            sticker: parseThumbnailToImageItem(messageRenderer.sticker.thumbnails || [], stickerLabel),
        };
    }
    else if ("purchaseAmountText" in messageRenderer && messageRenderer.purchaseAmountText) {
        ret.superchat = {
            amount: messageRenderer.purchaseAmountText.simpleText,
            color: convertColorToHex6(messageRenderer.bodyBackgroundColor),
        };
    }
    return ret;
}
