// ============================================================================
// GoIM WebSocket 协议 TypeScript 类型定义
// ============================================================================
// 基于 GoIM v1.0 WebSocket 协议
// 直接复制此文件到前端项目即可获得完整的 WS 消息类型安全
// ============================================================================

// ──────────────────────────────────────────────────────
// 消息类型枚举
// ──────────────────────────────────────────────────────

/** 消息内容类型 */
export enum MsgType {
  Text    = 1,  // 纯文本消息
  Image   = 2,  // 图片消息
  Video   = 3,  // 视频消息
  System  = 5,  // 系统通知
  Revoked = 6,  // 撤回占位符
}

/** 会话类型 */
export enum ConvType {
  Private = 1,  // 私聊 (convId 格式: p_{较小ID}_{较大ID})
  Group   = 2,  // 群聊 (convId 格式: g_{群组ID})
}

/** 群成员角色 */
export enum GroupRole {
  Member = 0,  // 普通成员
  Admin  = 1,  // 管理员
  Owner  = 2,  // 群主
}

/** 朋友圈可见性 */
export enum MomentVisibility {
  Friends = 2,  // 仅好友可见
  Private = 3,  // 仅自己可见
}

/** WS 消息 type 字段 — 客户端→服务端 */
export const ClientMsgType = {
  Msg:         "msg",
  DeliverAck:  "deliverAck",
  ReadAck:     "readAck",
  SyncReq:     "syncReq",
  RevokeMsg:   "revokeMsg",
} as const;

/** WS 消息 type 字段 — 服务端→客户端 */
export const ServerMsgType = {
  Msg:            "msg",
  ServerAck:      "serverAck",
  SyncBatch:      "syncBatch",
  ConvSync:       "convSync",
  MsgRevoked:     "msgRevoked",
  Kick:           "kick",
  Error:          "error",
} as const;

// ──────────────────────────────────────────────────────
// 数据载荷类型
// ──────────────────────────────────────────────────────

/** 收件箱/发件箱中的消息 */
export interface InboxMessage {
  msgId:      number;
  convId:     string;
  convType:   ConvType;
  fromId:     number;
  toId:       number;
  msgType:    MsgType;
  content:    string;
  readStatus: number;   // 0=未读, 1=已读 (仅私聊)
  groupSeq?:  number;   // 群消息序号 (仅群聊)
  timestamp:  number;   // Unix 毫秒时间戳
}

/** 客户端发送的聊天消息 */
export interface SendMessage {
  msgId:     string;    // 客户端生成的唯一ID，用于去重
  convType:  ConvType;
  toId:      number;    // 接收者ID(私聊) 或 群组ID(群聊)
  msgType:   MsgType;
  content:   string;
  timestamp: number;
}

/** 服务端回执 */
export interface ServerAck {
  clientMsgId: string;
  serverMsgId: number;
  groupSeq?:   number;   // 群聊时返回
  timestamp:   number;
}

/** 送达确认 */
export interface DeliverAck {
  serverMsgId: number;
}

/** 已读确认 */
export interface ReadAck {
  convId: string;
  /** 群聊已读水位线：已展示的最后一条群消息序号（私聊忽略，可选；缺省时服务端标记到最新） */
  lastReadGroupSeq?: number;
}

/** 离线同步请求 */
export interface SyncReq {
  lastSyncTime:   number;
  lastSyncMsgId?: number;
  batchSize:      number;
}

/** 离线同步批次 */
export interface SyncBatch {
  msgs:       InboxMessage[];
  hasMore:    boolean;
  syncTime:   number;
  syncMsgId?: number;
}

/** 会话摘要 */
export interface ConvSummary {
  convId:       string;
  convType:     ConvType;
  targetId:     number;
  targetName:   string;
  targetAvatar: string;
  lastMsg:      string;
  lastMsgTime:  number;
}

/** 会话同步 */
export interface ConvSync {
  conversations: ConvSummary[];
  unreadMap:     Record<string, number>;
}

/** 撤回消息请求 */
export interface RevokeMsgReq {
  convId:      string;
  serverMsgId: number;
}

/** 撤回通知 */
export interface RevokedNotification {
  convId:      string;
  serverMsgId: number;
  operatorId:  number;
}

/** 被踢出通知 */
export interface KickNotification {
  reason: "new_login";
}

export interface GroupRemovedNotification {
  groupId: number;
  reason: "removed";
}

export interface GroupAddedNotification {
  groupId: number;
  name: string;
}

/** 好友申请 (通过WS) */

/** 好友申请被接受通知 */

/** 错误通知 */
export interface WsError {
  code:    number;
  message: string;
}

// ──────────────────────────────────────────────────────
// 联合类型：客户端 → 服务端
// ──────────────────────────────────────────────────────

export type ClientWsMessage =
  | { type: "msg";          data: SendMessage }
  | { type: "deliverAck";   data: DeliverAck }
  | { type: "readAck";      data: ReadAck }
  | { type: "syncReq";      data: SyncReq }
  | { type: "revokeMsg";    data: RevokeMsgReq };

// ──────────────────────────────────────────────────────
// 联合类型：服务端 → 客户端
// ──────────────────────────────────────────────────────

export type ServerWsMessage =
  | { type: "msg";            data: InboxMessage }
  | { type: "serverAck";      data: ServerAck }
  | { type: "syncBatch";      data: SyncBatch }
  | { type: "convSync";       data: ConvSync }
  | { type: "msgRevoked";     data: RevokedNotification }
  // `kick` is emitted by the connection manager without a `data` envelope.
  | { type: "kick";           reason: KickNotification["reason"] }
    | { type: "groupRemoved";   data: GroupRemovedNotification }
    | { type: "groupAdded";     data: GroupAddedNotification }
  | { type: "error";          data: WsError };

// ──────────────────────────────────────────────────────
// 会话 ID 构建辅助函数
// ──────────────────────────────────────────────────────

/** 构建私聊会话ID: p_{较小ID}_{较大ID} */
export function buildPrivateConvId(userIdA: number, userIdB: number): string {
  const [smaller, larger] = userIdA < userIdB ? [userIdA, userIdB] : [userIdB, userIdA];
  return `p_${smaller}_${larger}`;
}

/** 构建群聊会话ID: g_{群组ID} */
export function buildGroupConvId(groupId: number): string {
  return `g_${groupId}`;
}

/** 从会话ID解析类型 */
export function parseConvId(convId: string): { type: ConvType; id: number } | null {
  if (convId.startsWith("p_")) {
    return { type: ConvType.Private, id: Number(convId.split("_")[1]) };
  }
  if (convId.startsWith("g_")) {
    return { type: ConvType.Group, id: Number(convId.slice(2)) };
  }
  return null;
}

// ──────────────────────────────────────────────────────
// REST API 错误码
// ──────────────────────────────────────────────────────

/** API 响应错误码对照表 */
export enum ApiErrorCode {
  Success = 0,

  // 通用
  InternalError  = 1000,
  MissingParam   = 1001,
  InvalidParam   = 1002,
  Unauthorized   = 1003,

  // 认证 1100+
  UsernameTooShort = 1101,
  PasswordTooShort = 1102,
  UsernameTaken    = 1103,
  UserNotFound     = 1104,
  WrongPassword    = 1105,
  InvalidToken     = 1106,

  // 好友 1200+
  SelfRequest      = 1201,
  AlreadyFriends   = 1202,
  FriendBlocked    = 1203,
  DuplicateRequest = 1204,
  RequestNotFound  = 1205,
  NotRequestTarget = 1206,
  AlreadyBlocked   = 1207,

  // 群组 1300+
  NotOwnerOrAdmin    = 1301,
  GroupNotFound      = 1302,
  AlreadyMember      = 1303,
  GroupFull          = 1304,
  CannotRemoveOwner  = 1305,
  CannotLeaveAsOwner = 1306,
  InvalidRole        = 1307,

  // 消息操作 1400+
  MsgNotRevocable   = 1401,
  MsgRevokeNotOwner = 1402,
  MsgDeleteFailed   = 1403,

  // 朋友圈 1500+
  MomentContentEmpty = 1501,
  MomentNotFound     = 1502,
  NotCommentOwner    = 1503,
  InvalidVisibility  = 1504,
  CommentNotFound    = 1505,

  // 设置 1700+
  SettingsNotFound = 1701,
  MuteConvExists   = 1702,
  MuteConvNotFound = 1703,

  // 私聊消息 (从 Lua 脚本返回)
  PMNotFriend  = 4001,
  PMBlocked    = 4002,
  PMDuplicate  = 4003,

  // 群聊消息 (从 Lua 脚本返回)
  GMNotMember  = 5001,
  GMMuted      = 5002,
  GMDuplicate  = 5003,
}

/** REST API 标准响应信封 */
export interface ApiResponse<T = unknown> {
  code:    number;
  message: string;
  data?:   T;
}
