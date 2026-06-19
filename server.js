const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");
const pool = require("./db");

const app = express();

app.use(cors());
app.use(express.json({ limit: "1mb" }));

// =========================
// CACHE HEADER
// =========================

function setNoStore(res) {
  res.set("Cache-Control", "no-store");
}

function setShortCache(res, seconds = 5) {
  res.set("Cache-Control", `public, max-age=${seconds}`);
}

// =========================
// TEST SERVER / DB
// =========================

app.get("/", (req, res) => {
  res.send("Dovui Server Running");
});

app.get("/test-db", async (req, res) => {
  try {
    const result = await pool.query("SELECT NOW()");
    res.json({
      success: true,
      time: result.rows[0],
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

pool
  .query("SELECT NOW()")
  .then(() => console.log("POSTGRES CONNECTED"))
  .catch((err) => console.error("POSTGRES ERROR", err));

// =========================
// SOCKET SERVER
// =========================

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
  },
});

// uid -> Set(socketId)
const onlineUsers = new Map();

function getOnlineUserIds() {
  return Array.from(onlineUsers.keys());
}

function addOnlineUser(uid, socketId) {
  if (!onlineUsers.has(uid)) {
    onlineUsers.set(uid, new Set());
  }

  onlineUsers.get(uid).add(socketId);
}

function removeOnlineUser(uid, socketId) {
  if (!onlineUsers.has(uid)) return false;

  const sockets = onlineUsers.get(uid);
  sockets.delete(socketId);

  if (sockets.size === 0) {
    onlineUsers.delete(uid);
    return true;
  }

  return false;
}

function getFriendChatId(uid1, uid2) {
  return [uid1, uid2].sort().join("_");
}

function generateRoomId() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let id = "";

  for (let i = 0; i < 6; i++) {
    id += chars[Math.floor(Math.random() * chars.length)];
  }

  return id;
}

// =========================
// USER HELPER
// =========================

async function getUserLite(uid) {
  const result = await pool.query(
    `
    SELECT uid, name, avatar, score
    FROM users
    WHERE uid = $1
    LIMIT 1
    `,
    [uid]
  );

  const user = result.rows[0];

  return {
    uid,
    name: user?.name || "Ẩn danh",
    avatar: user?.avatar || "",
    score: Number(user?.score || 0),
  };
}

// Bổ sung avatar cho player dựa vào userId.
// Hàm này giúp phòng cũ chưa có avatar vẫn có thể hiển thị avatar.
async function enrichPlayersWithAvatar(players = []) {
  if (!Array.isArray(players) || players.length === 0) return [];

  const userIds = [
    ...new Set(
      players
        .map((p) => p.userId)
        .filter((id) => typeof id === "string" && id.trim() !== "")
    ),
  ];

  if (userIds.length === 0) return players;

  const result = await pool.query(
    `
    SELECT uid, avatar, name
    FROM users
    WHERE uid = ANY($1::text[])
    `,
    [userIds]
  );

  const userMap = new Map();

  for (const row of result.rows) {
    userMap.set(row.uid, {
      avatar: row.avatar || "",
      name: row.name || "Ẩn danh",
    });
  }

  return players.map((p) => {
    const user = userMap.get(p.userId);

    return {
      ...p,
      displayName: p.displayName || user?.name || "Ẩn danh",
      avatar: p.avatar || user?.avatar || "",
    };
  });
}

// =========================
// ROOM HELPER
// =========================

async function createRoomWithRetry(buildRoomData, maxRetry = 5) {
  for (let i = 0; i < maxRetry; i++) {
    const roomId = generateRoomId();

    const exists = await pool.query(
      `
      SELECT room_id
      FROM game_rooms
      WHERE room_id = $1
      LIMIT 1
      `,
      [roomId]
    );

    if (exists.rows.length > 0) continue;

    const roomData = buildRoomData(roomId);

    await pool.query(
      `
      INSERT INTO game_rooms
      (
        room_id,
        host_id,
        host_name,
        category_id,
        category_name,
        type,
        password,
        question_count,
        time_per_question,
        status,
        players,
        invited_users,
        kicked_user_ids,
        started_at,
        current_level_id,
        created_at,
        updated_at
      )
      VALUES
      (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
        $11::jsonb,$12::jsonb,$13::jsonb,$14,$15,NOW(),NOW()
      )
      `,
      [
        roomData.roomId,
        roomData.hostId,
        roomData.hostName,
        roomData.categoryId,
        roomData.categoryName,
        roomData.type,
        roomData.password,
        roomData.questionCount,
        roomData.timePerQuestion,
        roomData.status,
        JSON.stringify(roomData.players),
        JSON.stringify(roomData.invitedUsers || []),
        JSON.stringify(roomData.kickedUserIds || []),
        roomData.startedAt || null,
        roomData.currentLevelId || null,
      ]
    );

    return { roomId, roomData };
  }

  throw new Error("CREATE_ROOM_FAILED");
}

async function roomRowToClient(row) {
  if (!row) return null;

  const players = await enrichPlayersWithAvatar(
    Array.isArray(row.players) ? row.players : []
  );

  return {
    roomId: row.room_id,
    hostId: row.host_id,
    hostName: row.host_name,
    categoryId: row.category_id,
    categoryName: row.category_name || "",
    type: row.type,
    password: row.password || "",
    questionCount: row.question_count,
    timePerQuestion: row.time_per_question,
    status: row.status,
    players,
    invitedUsers: Array.isArray(row.invited_users) ? row.invited_users : [],
    kickedUserIds: Array.isArray(row.kicked_user_ids)
      ? row.kicked_user_ids
      : [],
    startedAt: row.started_at,
    currentLevelId: row.current_level_id,
    createdAt: row.created_at ? new Date(row.created_at).getTime() : Date.now(),
    updatedAt: row.updated_at ? new Date(row.updated_at).getTime() : Date.now(),
  };
}

async function getGameRoom(roomId) {
  const result = await pool.query(
    `
    SELECT *
    FROM game_rooms
    WHERE room_id = $1
    LIMIT 1
    `,
    [roomId]
  );

  return roomRowToClient(result.rows[0]);
}

app.get("/users/:uid/room-create-count", async (req, res) => {
  try {
    const { uid } = req.params;

    const result = await pool.query(
      `
      SELECT room_creation_dates
      FROM users
      WHERE uid = $1
      LIMIT 1
      `,
      [uid]
    );

    const raw = result.rows[0]?.room_creation_dates || [];
    const cutoff = Date.now() - 7 * 24 * 60 * 60 * 1000;

    const recent = raw.filter((d) => {
      const time = new Date(d).getTime();
      return !Number.isNaN(time) && time > cutoff;
    });

    await pool.query(
      `
      UPDATE users
      SET room_creation_dates = $1::jsonb,
          updated_at = NOW()
      WHERE uid = $2
      `,
      [JSON.stringify(recent), uid]
    );

    res.json({
      count: recent.length,
      dates: recent,
    });
  } catch (e) {
    res.status(500).json({
      count: 0,
      error: e.message,
    });
  }
});

app.post("/users/:uid/record-room-create", async (req, res) => {
  try {
    const { uid } = req.params;
    const now = new Date().toISOString();

    await pool.query(
      `
      UPDATE users
      SET room_creation_dates =
        COALESCE(room_creation_dates, '[]'::jsonb) || $1::jsonb,
        updated_at = NOW()
      WHERE uid = $2
      `,
      [JSON.stringify([now]), uid]
    );

    res.json({ success: true });
  } catch (e) {
    res.status(500).json({
      success: false,
      error: e.message,
    });
  }
});

async function emitRoomUpdated(roomId) {
  const latestRoom = await getGameRoom(roomId);
  if (!latestRoom) return;

  io.to(roomId).emit("room_updated", latestRoom);
}

// =========================
// LEADERBOARD HELPER
// =========================

async function getTopLeaderboard() {
  const result = await pool.query(`
    SELECT
      uid,
      name,
      player_id,
      avatar,
      score,
      is_vip,
      is_admin,
      RANK() OVER (ORDER BY score DESC) AS rank
    FROM users
    ORDER BY score DESC
    LIMIT 10
  `);

  return result.rows;
}

function leaderboardKey(list) {
  return list
    .map(
      (u) =>
        `${u.uid}:${u.score}:${u.rank}:${u.name}:${u.avatar}:${u.is_vip}`
    )
    .join("|");
}

async function emitLeaderboardIfChanged(beforeTop) {
  const afterTop = await getTopLeaderboard();

  if (leaderboardKey(beforeTop) !== leaderboardKey(afterTop)) {
    io.emit("leaderboard_updated", afterTop);
  }

  return afterTop;
}

const lastUserPayloads = new Map();

function userKey(user) {
  if (!user) return "";

  return [
    user.uid,
    user.name,
    user.email,
    user.player_id,
    user.avatar,
    user.score,
    user.rank,
    user.is_vip,
    user.is_admin,
    user.is_online,
  ].join("|");
}

async function getFullUser(uid) {
  const result = await pool.query(
    `
    SELECT
      u.uid,
      u.name,
      u.email,
      u.player_id,
      u.avatar,
      u.score,
      u.is_vip,
      u.is_admin,
      u.is_online,
      u.last_seen,
      u.created_at,
      u.updated_at,
      (
        SELECT COUNT(*) + 1
        FROM users x
        WHERE x.score > u.score
      ) AS rank
    FROM users u
    WHERE u.uid = $1
    LIMIT 1
    `,
    [uid]
  );

  return result.rows[0] || null;
}

async function emitUserUpdated(uid, force = false) {
  if (!uid) return;

  const user = await getFullUser(uid);
  if (!user) return;

  const key = userKey(user);
  const oldKey = lastUserPayloads.get(uid);

  // Chỉ emit khi dữ liệu thật sự đổi
  if (!force && oldKey === key) return;

  lastUserPayloads.set(uid, key);

  const sockets = onlineUsers.get(uid);

  // Chỉ gửi cho đúng user đó, không broadcast toàn app
  if (sockets) {
    for (const socketId of sockets) {
      io.to(socketId).emit(`user_updated_${uid}`, user);
    }
  }
}

async function emitUserUpdated(uid, force = false) {
  const user = await getFullUser(uid);
  if (!user) return;

  const key = userKey(user);
  const oldKey = lastUserPayloads.get(uid);

  if (!force && oldKey === key) return;

  lastUserPayloads.set(uid, key);

  const sockets = onlineUsers.get(uid);

  if (sockets) {
    for (const socketId of sockets) {
      io.to(socketId).emit(`user_updated_${uid}`, user);
    }
  }
}

async function getUserByUid(uid) {
  const result = await pool.query(
    `
    SELECT uid, name, avatar
    FROM users
    WHERE uid = $1
    LIMIT 1
    `,
    [uid]
  );

  const user = result.rows[0];

  return {
    name: user?.name || "Người chơi",
    avatar: user?.avatar || "",
  };
}

// =========================
// SOCKET EVENTS
// =========================

io.on("connection", (socket) => {
  console.log("Connected:", socket.id);

  socket.emit("online_users", getOnlineUserIds());

  // User online
  socket.on("user_online", ({ uid }) => {
    if (!uid) return;

    socket.uid = uid;
    addOnlineUser(uid, socket.id);

    io.emit("user_status_changed", {
      uid,
      isOnline: true,
    });

    socket.emit("online_users", getOnlineUserIds());
  });

  // User logout
  socket.on("user_logout", ({ uid }) => {
    if (!uid) return;

    removeOnlineUser(uid, socket.id);

    io.emit("user_status_changed", {
      uid,
      isOnline: false,
    });
  });

  // =========================
  // GAME ROOM
  // =========================

  // Client join socket room để nhận room_updated realtime.
  socket.on("join_socket_room", ({ roomId }) => {
    if (!roomId) return;
    socket.join(roomId.trim().toUpperCase());
  });

  // Tạo phòng và lưu vào Neon
  socket.on("create_game_room", async (data, callback) => {
    try {
      const {
        uid,
        displayName,
        categoryId,
        categoryName = "",
        type,
        password = "",
        questionCount = 10,
        timePerQuestion = 15,
      } = data;

      if (!uid || !categoryId || !type) {
        return callback?.({
          success: false,
          message: "Thiếu dữ liệu tạo phòng",
        });
      }

      // Lấy tên + avatar trực tiếp từ Neon theo uid
      const user = await getUserLite(uid);

      const finalDisplayName =
        user.name && user.name !== "Ẩn danh"
          ? user.name
          : displayName && displayName !== "Ẩn danh"
            ? displayName
            : "Người chơi";

      const finalAvatar = user.avatar || "";
      const now = Date.now();

      const { roomId, roomData } = await createRoomWithRetry((newRoomId) => ({
        roomId: newRoomId,
        hostId: uid,
        hostName: finalDisplayName,
        categoryId,
        categoryName,
        type,
        password,
        questionCount,
        timePerQuestion,
        status: "waiting",
        kickedUserIds: [],
        invitedUsers: [],
        startedAt: null,
        currentLevelId: null,
        createdAt: now,
        updatedAt: now,
        players: [
          {
            userId: uid,
            displayName: finalDisplayName,
            avatar: finalAvatar,
            score: 0,
            isHost: true,
            isReady: true,
            isFinished: false,
            joinedAt: now,
          },
        ],
      }));

      socket.join(roomId);

      callback?.({
        success: true,
        roomId,
        room: roomData,
      });

      io.to(roomId).emit("room_updated", roomData);
    } catch (error) {
      console.error("create_game_room error:", error);

      callback?.({
        success: false,
        message: "Không thể tạo phòng",
      });
    }
  });

  // Vào phòng bằng mã hoặc lời mời
  socket.on("join_game_room", async (data, callback) => {
    try {
      const {
        roomId,
        uid,
        displayName,
        password = "",
        isDirectJoin = false,
      } = data;

      if (!roomId || !uid) {
        return callback?.({
          success: false,
          message: "Thiếu dữ liệu vào phòng",
        });
      }

      const cleanRoomId = roomId.trim().toUpperCase();

      const room = await getGameRoom(cleanRoomId);

      if (!room) {
        return callback?.({
          success: false,
          message: "Phòng không tồn tại",
        });
      }

      if (room.status !== "waiting") {
        return callback?.({
          success: false,
          message: "Phòng đã bắt đầu",
        });
      }

      const isInvited = room.invitedUsers.includes(uid);

      if (isDirectJoin && !isInvited) {
        return callback?.({
          success: false,
          message: "Bạn không có quyền tham gia phòng này",
        });
      }

      if (!isDirectJoin && room.password && room.password !== password.trim()) {
        return callback?.({
          success: false,
          message: "Sai mật khẩu",
        });
      }

      if (room.players.length >= 8) {
        return callback?.({
          success: false,
          message: "Phòng đã đầy",
        });
      }

      const user = await getUserLite(uid);

      const finalDisplayName =
        user.name && user.name !== "Ẩn danh"
          ? user.name
          : displayName && displayName !== "Ẩn danh"
            ? displayName
            : "Người chơi";

      const finalAvatar = user.avatar || "";

      const alreadyInRoom = room.players.some((p) => p.userId === uid);

      const newPlayers = alreadyInRoom
        ? room.players.map((p) =>
          p.userId === uid
            ? {
              ...p,
              displayName:
                p.displayName && p.displayName !== "Ẩn danh"
                  ? p.displayName
                  : finalDisplayName,
              avatar: p.avatar || finalAvatar,
            }
            : p
        )
        : [
          ...room.players,
          {
            userId: uid,
            displayName: finalDisplayName,
            avatar: finalAvatar,
            score: 0,
            isHost: false,
            isReady: false,
            isFinished: false,
            joinedAt: Date.now(),
          },
        ];

      const kickedUserIds = room.kickedUserIds.filter((id) => id !== uid);

      await pool.query(
        `
      UPDATE game_rooms
      SET players = $1::jsonb,
          kicked_user_ids = $2::jsonb,
          updated_at = NOW()
      WHERE room_id = $3
      `,
        [JSON.stringify(newPlayers), JSON.stringify(kickedUserIds), cleanRoomId]
      );

      socket.join(cleanRoomId);

      const latestRoom = await getGameRoom(cleanRoomId);

      callback?.({
        success: true,
        roomId: cleanRoomId,
        room: latestRoom,
      });

      io.to(cleanRoomId).emit("room_updated", latestRoom);
    } catch (error) {
      console.error("join_game_room error:", error);

      callback?.({
        success: false,
        message: "Không thể vào phòng",
      });
    }
  });

  // Rời phòng.
  // Nếu host rời hoặc phòng trống thì xóa phòng.
  socket.on("leave_game_room", async (data, callback) => {
    try {
      const { roomId, uid } = data;

      if (!roomId || !uid) {
        return callback?.({
          success: false,
          message: "Thiếu dữ liệu rời phòng",
        });
      }

      const room = await getGameRoom(roomId);

      socket.leave(roomId);

      if (!room) {
        return callback?.({
          success: true,
        });
      }

      const currentPlayer = room.players.find((p) => p.userId === uid);
      const newPlayers = room.players.filter((p) => p.userId !== uid);

      if (newPlayers.length === 0 || currentPlayer?.isHost) {
        io.to(roomId).emit("room_closed", {
          roomId,
          reason: "HOST_LEFT",
        });

        callback?.({
          success: true,
          closed: true,
        });

        setTimeout(async () => {
          try {
            await pool.query(
              `
              DELETE FROM game_rooms
              WHERE room_id = $1
              `,
              [roomId]
            );

            io.in(roomId).socketsLeave(roomId);
          } catch (error) {
            console.error("delete room error:", error);
          }
        }, 500);

        return;
      }

      await pool.query(
        `
        UPDATE game_rooms
        SET players = $1::jsonb,
            updated_at = NOW()
        WHERE room_id = $2
        `,
        [JSON.stringify(newPlayers), roomId]
      );

      await emitRoomUpdated(roomId);

      callback?.({
        success: true,
        closed: false,
      });
    } catch (error) {
      console.error("leave_game_room error:", error);

      callback?.({
        success: false,
        message: "Không thể rời phòng",
      });
    }
  });

  // Chủ phòng kick người chơi.
  socket.on("kick_player", async (data, callback) => {
    try {
      const { roomId, uid, targetUserId } = data;

      if (!roomId || !uid || !targetUserId) {
        return callback?.({
          success: false,
          message: "Thiếu dữ liệu kick",
        });
      }

      const room = await getGameRoom(roomId);

      if (!room) {
        return callback?.({
          success: false,
          message: "Phòng không tồn tại",
        });
      }

      if (room.hostId !== uid) {
        return callback?.({
          success: false,
          message: "Bạn không phải chủ phòng",
        });
      }

      const newPlayers = room.players.filter(
        (p) => p.userId !== targetUserId
      );

      const kickedUserIds = [...room.kickedUserIds];

      if (!kickedUserIds.includes(targetUserId)) {
        kickedUserIds.push(targetUserId);
      }

      await pool.query(
        `
        UPDATE game_rooms
        SET players = $1::jsonb,
            kicked_user_ids = $2::jsonb,
            updated_at = NOW()
        WHERE room_id = $3
        `,
        [JSON.stringify(newPlayers), JSON.stringify(kickedUserIds), roomId]
      );

      await emitRoomUpdated(roomId);

      callback?.({
        success: true,
      });
    } catch (error) {
      console.error("kick_player error:", error);

      callback?.({
        success: false,
        message: "Không thể kick người chơi",
      });
    }
  });

  // Chủ phòng đóng phòng.
  socket.on("close_game_room", async (data, callback) => {
    try {
      const { roomId, uid } = data;

      if (!roomId || !uid) {
        return callback?.({
          success: false,
          message: "Thiếu dữ liệu đóng phòng",
        });
      }

      const room = await getGameRoom(roomId);

      if (!room) {
        return callback?.({
          success: true,
        });
      }

      if (room.hostId !== uid) {
        return callback?.({
          success: false,
          message: "Bạn không phải chủ phòng",
        });
      }

      io.to(roomId).emit("room_closed", {
        roomId,
        reason: "HOST_CLOSED",
      });

      callback?.({
        success: true,
      });

      setTimeout(async () => {
        try {
          await pool.query(
            `
            DELETE FROM game_rooms
            WHERE room_id = $1
            `,
            [roomId]
          );

          io.in(roomId).socketsLeave(roomId);
        } catch (error) {
          console.error("close room delete error:", error);
        }
      }, 500);
    } catch (error) {
      console.error("close_game_room error:", error);

      callback?.({
        success: false,
        message: "Không thể đóng phòng",
      });
    }
  });

  // Mời bạn bè.
  // Đồng thời lưu uid được mời vào invited_users của Neon.
  socket.on("invite_friend", async ({ fromUid, toUid, roomId, fromName }) => {
    try {
      if (!fromUid || !toUid || !roomId) return;

      const cleanRoomId = roomId.trim().toUpperCase();

      const room = await getGameRoom(cleanRoomId);
      if (!room) return;

      const invitedUsers = [...room.invitedUsers];

      if (!invitedUsers.includes(toUid)) {
        invitedUsers.push(toUid);
      }

      await pool.query(
        `
        UPDATE game_rooms
        SET invited_users = $1::jsonb,
            updated_at = NOW()
        WHERE room_id = $2
        `,
        [JSON.stringify(invitedUsers), cleanRoomId]
      );

      await emitRoomUpdated(cleanRoomId);

      const sockets = onlineUsers.get(toUid);
      if (!sockets) return;

      for (const socketId of sockets) {
        io.to(socketId).emit("game_invite", {
          fromUid,
          fromName,
          roomId: cleanRoomId,
        });
      }
    } catch (error) {
      console.error("invite_friend error:", error);
    }
  });

  // =========================
  // ROOM CHAT - NEON
  // =========================

  // Khi mở chat phòng, load 100 tin nhắn gần nhất từ Neon.
  socket.on("join-room-chat", async ({ roomId }) => {
    try {
      if (!roomId) return;

      socket.join(`room_chat_${roomId}`);

      const result = await pool.query(
        `
        SELECT
          id,
          room_id,
          user_id,
          display_name,
          text,
          sent_at
        FROM room_messages
        WHERE room_id = $1
        ORDER BY sent_at ASC
        LIMIT 100
        `,
        [roomId]
      );

      const messages = result.rows.map((row) => ({
        id: row.id.toString(),
        roomId: row.room_id,
        userId: row.user_id,
        displayName: row.display_name || "Ẩn danh",
        text: row.text || "",
        sentAt: new Date(row.sent_at).toISOString(),
      }));

      socket.emit("room-messages", messages);
    } catch (error) {
      console.error("join-room-chat error:", error);
    }
  });

  socket.on("leave-room-chat", ({ roomId }) => {
    if (!roomId) return;
    socket.leave(`room_chat_${roomId}`);
  });

  // Gửi tin nhắn phòng.
  // Server lưu vào Neon rồi emit lại cho toàn bộ người trong phòng.
  socket.on("send-room-message", async (data) => {
    try {
      const { roomId, userId, displayName, text } = data;

      if (!roomId || !userId || !text?.trim()) return;

      const user = await getUserLite(userId);

      const result = await pool.query(
        `
        INSERT INTO room_messages
        (
          room_id,
          user_id,
          display_name,
          text,
          sent_at
        )
        VALUES ($1, $2, $3, $4, NOW())
        RETURNING
          id,
          room_id,
          user_id,
          display_name,
          text,
          sent_at
        `,
        [
          roomId,
          userId,
          displayName || user.name || "Ẩn danh",
          text.trim(),
        ]
      );

      const row = result.rows[0];

      const message = {
        id: row.id.toString(),
        roomId: row.room_id,
        userId: row.user_id,
        displayName: row.display_name || "Ẩn danh",
        text: row.text || "",
        sentAt: new Date(row.sent_at).toISOString(),
      };

      io.to(`room_chat_${roomId}`).emit("room-message", message);
    } catch (error) {
      console.error("send-room-message error:", error);
    }
  });

  // =========================
  // GAME STATUS
  // =========================

  // SAU — emit thẳng, bỏ round-trip thứ 2
  socket.on("set_player_ready", async ({ roomId, uid, isReady }) => {
    const room = await getGameRoom(roomId);
    if (!room) return;

    const players = await enrichPlayersWithAvatar(
      room.players.map((p) =>
        p.userId === uid ? { ...p, isReady, isFinished: isReady ? false : p.isFinished } : p
      )
    );

    await pool.query(`UPDATE game_rooms SET players=$1::jsonb, updated_at=NOW() WHERE room_id=$2`,
      [JSON.stringify(players), roomId]);

    // Emit thẳng từ data đã có, không query lại
    io.to(roomId).emit("room_updated", { ...room, players, updatedAt: Date.now() });
  });

  socket.on("start_game_with_reset", async ({ roomId }) => {
    try {
      const room = await getGameRoom(roomId);
      if (!room) return;

      // Chỉ kiểm tra người chơi không phải host
      const nonHostPlayers = room.players.filter((p) => !p.isHost);

      const allNonHostReady = nonHostPlayers.every(
        (p) => p.isReady === true
      );

      if (room.players.length < 2 || !allNonHostReady) {
        await emitRoomUpdated(roomId);
        return;
      }

      if (room.players.length < 2 || !allNonHostReady) {
        console.log("START BLOCKED:", {
          roomId,
          players: room.players.map((p) => ({
            name: p.displayName,
            isHost: p.isHost,
            isReady: p.isReady,
            isFinished: p.isFinished,
          })),
        });

        await emitRoomUpdated(roomId);
        return;
      }

      const startedAt = Date.now();

      const players = room.players.map((p) => ({
        ...p,
        score: 0,
        isFinished: false,
        // host luôn ready, người chơi giữ ready hiện tại
        isReady: p.isHost ? true : p.isReady,
      }));

      await pool.query(
        `
      UPDATE game_rooms
      SET status = 'playing',
          players = $1::jsonb,
          started_at = $2,
          updated_at = NOW()
      WHERE room_id = $3
      `,
        [JSON.stringify(players), startedAt, roomId]
      );

      await emitRoomUpdated(roomId);
    } catch (error) {
      console.error("start_game_with_reset error:", error);
    }
  });

  socket.on("update_room_score", async ({ roomId, uid, delta }) => {
    try {
      const room = await getGameRoom(roomId);
      if (!room) return;

      const players = room.players.map((p) =>
        p.userId === uid
          ? {
            ...p,
            score: Number(p.score || 0) + Number(delta || 0),
          }
          : p
      );

      await pool.query(
        `
        UPDATE game_rooms
        SET players = $1::jsonb,
            updated_at = NOW()
        WHERE room_id = $2
        `,
        [JSON.stringify(players), roomId]
      );

      await emitRoomUpdated(roomId);
    } catch (error) {
      console.error("update_room_score error:", error);
    }
  });

  // SAU — ĐÚNG
  socket.on("mark_player_finished", async ({ roomId, uid }) => {
    const room = await getGameRoom(roomId);
    if (!room) return;

    const players = room.players.map((p) =>
      p.userId === uid ? { ...p, isFinished: true } : p
    );

    await pool.query(`
    UPDATE game_rooms
    SET players = $1::jsonb, updated_at = NOW()
    WHERE room_id = $2
  `, [JSON.stringify(players), roomId]);

    await emitRoomUpdated(roomId);
  });

  socket.on("finish_game", async ({ roomId }) => {
    try {
      const room = await getGameRoom(roomId);
      if (!room) return;

      const beforeTop = await getTopLeaderboard();

      await pool.query("BEGIN");

      await pool.query(
        `
        UPDATE game_rooms
        SET status = 'waiting',
            updated_at = NOW()
        WHERE room_id = $1
        `,
        [roomId]
      );

      for (const p of room.players) {
        const score = Number(p.score || 0);

        if (score > 0) {
          await pool.query(
            `
            UPDATE users
            SET score = score + $1,
                updated_at = NOW()
            WHERE uid = $2
            `,
            [score, p.userId]
          );
        }
      }
      for (const p of room.players) {
        await emitUserUpdated(p.userId);
      }

      await pool.query("COMMIT");

      await emitRoomUpdated(roomId);
      await emitLeaderboardIfChanged(beforeTop);

      for (const p of room.players) {
        await emitUserUpdated(p.userId);
      }
    } catch (error) {
      await pool.query("ROLLBACK").catch(() => { });
      console.error("finish_game error:", error);
    }
  });

  socket.on("reset_room", async ({ roomId }) => {
    try {
      const room = await getGameRoom(roomId);
      if (!room) return;

      const players = room.players.map((p) => ({
        ...p,
        isFinished: false,
      }));

      await pool.query(
        `
        UPDATE game_rooms
        SET status = 'waiting',
            players = $1::jsonb,
            started_at = NULL,
            current_level_id = NULL,
            updated_at = NOW()
        WHERE room_id = $2
        `,
        [JSON.stringify(players), roomId]
      );

      await emitRoomUpdated(roomId);
    } catch (error) {
      console.error("reset_room error:", error);
    }
  });

  socket.on("reset_all_players_ready", async ({ roomId }) => {
    try {
      const room = await getGameRoom(roomId);
      if (!room) return;

      const players = room.players.map((p) =>
        p.isHost ? p : { ...p, isReady: false }
      );

      await pool.query(
        `
        UPDATE game_rooms
        SET players = $1::jsonb,
            updated_at = NOW()
        WHERE room_id = $2
        `,
        [JSON.stringify(players), roomId]
      );

      await emitRoomUpdated(roomId);
    } catch (error) {
      console.error("reset_all_players_ready error:", error);
    }
  });

  // =========================
  // FRIEND CHAT
  // =========================

  socket.on("join_friend_chat", async ({ currentUserId, friendId }) => {
    try {
      if (!currentUserId || !friendId) return;

      const chatId = getFriendChatId(currentUserId, friendId);

      socket.join(chatId);
      socket.activeChatId = chatId;

      const result = await pool.query(
        `
        SELECT
          id,
          chat_id,
          sender_id,
          receiver_id,
          text,
          type,
          media_url,
          is_read,
          created_at
        FROM messages
        WHERE chat_id = $1
        ORDER BY created_at DESC
        LIMIT 50
        `,
        [chatId]
      );

      const messages = result.rows.map((row) => ({
        id: row.id.toString(),
        chatId: row.chat_id,
        senderId: row.sender_id,
        receiverId: row.receiver_id,
        text: row.text || "",
        type: row.type || "text",
        mediaUrl: row.media_url || "",
        isRead: row.is_read === true,
        createdAt: new Date(row.created_at).getTime(),
      }));

      socket.emit("friend_messages", messages);
    } catch (error) {
      console.error("join_friend_chat error:", error);
    }
  });

  socket.on("send_friend_message", async ({ currentUserId, friendId, text }) => {
    try {
      if (!currentUserId || !friendId || !text?.trim()) return;

      const chatId = getFriendChatId(currentUserId, friendId);

      const receiverOnline = onlineUsers.has(friendId);

      const receiverInChat =
        receiverOnline &&
        (() => {
          const sockets = onlineUsers.get(friendId);

          for (const sid of sockets) {
            const s = io.sockets.sockets.get(sid);
            if (s?.activeChatId === chatId) return true;
          }

          return false;
        })();

      const result = await pool.query(
        `
        INSERT INTO messages
        (
          chat_id,
          sender_id,
          receiver_id,
          text,
          type,
          is_read
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING
          id,
          chat_id,
          sender_id,
          receiver_id,
          text,
          type,
          media_url,
          is_read,
          created_at
        `,
        [
          chatId,
          currentUserId,
          friendId,
          text.trim(),
          "text",
          receiverInChat,
        ]
      );

      await pool.query(
        `
        INSERT INTO friend_chats
        (
          chat_id,
          user1_id,
          user2_id,
          last_message,
          updated_at
        )
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (chat_id)
        DO UPDATE SET
          last_message = EXCLUDED.last_message,
          updated_at = NOW()
        `,
        [chatId, currentUserId, friendId, text.trim()]
      );

      const row = result.rows[0];

      const message = {
        id: row.id.toString(),
        chatId: row.chat_id,
        senderId: row.sender_id,
        receiverId: row.receiver_id,
        text: row.text || "",
        type: row.type || "text",
        mediaUrl: row.media_url || "",
        isRead: row.is_read === true,
        createdAt: new Date(row.created_at).getTime(),
      };

      io.to(chatId).emit("new_friend_message", message);
    } catch (error) {
      console.error("send_friend_message error:", error);
    }
  });

  socket.on("mark_friend_messages_read", async ({ currentUserId, friendId }) => {
    try {
      if (!currentUserId || !friendId) return;

      const chatId = getFriendChatId(currentUserId, friendId);

      const result = await pool.query(
        `
        UPDATE messages
        SET is_read = true
        WHERE chat_id = $1
        AND receiver_id = $2
        AND is_read = false
        RETURNING id
        `,
        [chatId, currentUserId]
      );

      if (result.rowCount > 0) {
        io.to(chatId).emit("friend_messages_read", {
          chatId,
          readerId: currentUserId,
        });
      }
    } catch (error) {
      console.error("mark_friend_messages_read error:", error);
    }
  });

  // =========================
  // DISCONNECT
  // =========================

  socket.on("disconnect", () => {
    const uid = socket.uid;

    if (uid) {
      const userReallyOffline = removeOnlineUser(uid, socket.id);

      if (userReallyOffline) {
        io.emit("user_status_changed", {
          uid,
          isOnline: false,
        });
      }
    }

    console.log("Disconnected:", socket.id);
  });
});

// =========================
// USER ROUTES
// =========================

app.post("/users/sync", async (req, res) => {
  try {
    const { uid, name, email, avatar = "" } = req.body;

    if (!uid || !email) {
      return res.status(400).json({
        success: false,
        message: "Thiếu uid hoặc email",
      });
    }

    const playerId = uid.substring(0, 8).toUpperCase();

    const result = await pool.query(
      `
      INSERT INTO users
      (
        uid,
        name,
        email,
        player_id,
        avatar,
        score,
        is_vip,
        is_admin,
        is_online,
        created_at,
        updated_at
      )
      VALUES
      ($1, $2, $3, $4, $5, 300, false, false, false, NOW(), NOW())
      ON CONFLICT (uid)
      DO UPDATE SET
        name = EXCLUDED.name,
        email = EXCLUDED.email,
        avatar = COALESCE(NULLIF(EXCLUDED.avatar, ''), users.avatar),
        updated_at = NOW()
      RETURNING
        uid,
        name,
        email,
        player_id,
        avatar,
        score,
        is_vip,
        is_admin,
        is_online,
        last_seen,
        created_at,
        updated_at
      `,
      [uid, name || "", email || "", playerId, avatar || ""]
    );

    const user = result.rows[0];

    await emitUserUpdated(user.uid, true);

    setNoStore(res);

    res.json({
      success: true,
      user,
    });
  } catch (e) {
    console.error("SYNC USER ERROR:", e);

    res.status(500).json({
      success: false,
      message: e.message,
    });
  }
});

app.get("/users/leaderboard", async (req, res) => {
  try {
    const users = await getTopLeaderboard();

    setShortCache(res, 5);

    res.json({
      users,
    });
  } catch (err) {
    console.error("LEADERBOARD ERROR:", err);

    res.status(500).json({
      users: [],
      error: err.message,
    });
  }
});

app.get("/users/count", async (req, res) => {
  try {
    const result = await pool.query("SELECT COUNT(*) FROM users");

    setShortCache(res, 10);

    res.json({
      count: Number(result.rows[0].count),
    });
  } catch (e) {
    res.status(500).json({
      count: 0,
      message: e.message,
    });
  }
});

app.get("/users/check-name", async (req, res) => {
  try {
    const name = req.query.name?.trim();

    if (!name) {
      return res.json({
        exists: false,
      });
    }

    const result = await pool.query(
      `
      SELECT uid
      FROM users
      WHERE LOWER(name) = LOWER($1)
      LIMIT 1
      `,
      [name]
    );

    res.json({
      exists: result.rows.length > 0,
    });
  } catch (e) {
    res.status(500).json({
      exists: false,
      message: e.message,
    });
  }
});

app.get("/users/check-player-id", async (req, res) => {
  try {
    const playerId = req.query.playerId?.trim();

    if (!playerId) {
      return res.status(400).json({
        error: "MISSING_PLAYER_ID",
      });
    }

    const result = await pool.query(
      `
      SELECT uid
      FROM users
      WHERE LOWER(player_id) = LOWER($1)
      LIMIT 1
      `,
      [playerId]
    );

    if (result.rows.length === 0) {
      return res.json({
        exists: false,
        uid: null,
      });
    }

    res.json({
      exists: true,
      uid: result.rows[0].uid,
    });
  } catch (err) {
    res.status(500).json({
      error: err.message,
    });
  }
});

app.get("/users/by-name/:name", async (req, res) => {
  try {
    const name = req.params.name?.trim();

    if (!name) {
      return res.status(400).json({
        user: null,
        message: "Thiếu tên",
      });
    }

    const result = await pool.query(
      `
      SELECT
        uid,
        name,
        email,
        player_id,
        avatar,
        score,
        is_vip,
        is_admin,
        is_online,
        last_seen,
        created_at,
        updated_at
      FROM users
      WHERE LOWER(TRIM(name)) = LOWER(TRIM($1))
      LIMIT 1
      `,
      [name]
    );

    res.json({
      user: result.rows[0] || null,
    });
  } catch (e) {
    res.status(500).json({
      user: null,
      message: e.message,
    });
  }
});

app.patch("/users/:uid/profile", async (req, res) => {
  try {
    const { uid } = req.params;
    const name = req.body.name?.trim();
    const playerId = (req.body.playerId || req.body.player_id)?.trim();
    const avatar = req.body.avatar ?? null;
    const beforeTop = await getTopLeaderboard();

    if (!name || !playerId) {
      return res.status(400).json({
        error: "MISSING_FIELDS",
      });
    }

    const existing = await pool.query(
      `
      SELECT uid
      FROM users
      WHERE LOWER(player_id) = LOWER($1)
      AND uid <> $2
      LIMIT 1
      `,
      [playerId, uid]
    );

    if (existing.rows.length > 0) {
      return res.status(409).json({
        error: "PLAYER_ID_EXISTS",
      });
    }

    const result = await pool.query(
      `
      UPDATE users
      SET
        name = $1,
        player_id = $2,
        avatar = COALESCE(NULLIF($3, ''), avatar),
        updated_at = NOW()
      WHERE uid = $4
      RETURNING
        uid,
        name,
        email,
        player_id,
        avatar,
        score,
        is_vip,
        is_admin,
        is_online,
        last_seen,
        created_at,
        updated_at
      `,
      [name, playerId, avatar, uid]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({
        error: "USER_NOT_FOUND",
      });
    }

    const user = result.rows[0];

    await emitUserUpdated(uid, true);
    await emitLeaderboardIfChanged(beforeTop);

    res.json({
      user,
    });
  } catch (err) {
    console.error("UPDATE PROFILE ERROR:", err);

    res.status(500).json({
      error: err.message,
    });
  }
});

app.patch("/users/:uid/score", async (req, res) => {
  try {
    const { score } = req.body;
    const { uid } = req.params;
    const safeScore = Number(score);

    if (!uid || Number.isNaN(safeScore)) {
      return res.status(400).json({
        success: false,
        message: "INVALID_DATA",
      });
    }

    const beforeTop = await getTopLeaderboard();

    const result = await pool.query(
      `
      UPDATE users
      SET
        score = $1,
        updated_at = NOW()
      WHERE uid = $2
      RETURNING
        uid,
        name,
        email,
        player_id,
        avatar,
        score,
        is_vip,
        is_admin,
        is_online,
        last_seen,
        created_at,
        updated_at
      `,
      [safeScore, uid]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({
        error: "USER_NOT_FOUND",
      });
    }

    await emitLeaderboardIfChanged(beforeTop);
    await emitUserUpdated(uid);

    res.json({
      success: true,
      user: result.rows[0],
    });
  } catch (e) {
    res.status(500).json({
      success: false,
      message: e.message,
    });
  }
});

app.patch("/users/:uid/vip", async (req, res) => {
  try {
    const result = await pool.query(
      `
      UPDATE users
      SET
        is_vip = true,
        updated_at = NOW()
      WHERE uid = $1
      RETURNING
        uid,
        name,
        email,
        player_id,
        avatar,
        score,
        is_vip,
        is_admin,
        is_online,
        last_seen,
        created_at,
        updated_at
      `,
      [req.params.uid]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({
        error: "USER_NOT_FOUND",
      });
    }

    const user = result.rows[0];

    await emitUserUpdated(req.params.uid, true);

    res.json({
      user,
    });
  } catch (err) {
    res.status(500).json({
      error: err.message,
    });
  }
});

app.post("/users/:uid/check-in", async (req, res) => {
  try {
    const { uid } = req.params;
    const beforeTop = await getTopLeaderboard();

    const result = await pool.query(
      `
      UPDATE users
      SET
        score = score + 10,
        last_check_in = CURRENT_DATE,
        updated_at = NOW()
      WHERE uid = $1
      AND (last_check_in IS NULL OR last_check_in < CURRENT_DATE)
      RETURNING
        uid,
        name,
        email,
        player_id,
        avatar,
        score,
        is_vip,
        is_admin,
        is_online,
        last_seen,
        created_at,
        updated_at
      `,
      [uid]
    );

    if (result.rows.length === 0) {
      return res.status(400).json({
        error: "ALREADY_CHECKED_IN",
      });
    }

    await emitLeaderboardIfChanged(beforeTop);
    await emitUserUpdated(uid);

    res.json({
      user: result.rows[0],
    });
  } catch (err) {
    res.status(500).json({
      error: err.message,
    });
  }
});

app.post("/users/:uid/watch-video-reward", async (req, res) => {
  try {
    const { uid } = req.params;
    const beforeTop = await getTopLeaderboard();

    const result = await pool.query(
      `
      UPDATE users
      SET
        score = score + 10,
        last_video_watch = CURRENT_DATE,
        updated_at = NOW()
      WHERE uid = $1
      AND (last_video_watch IS NULL OR last_video_watch < CURRENT_DATE)
      RETURNING
        uid,
        name,
        email,
        player_id,
        avatar,
        score,
        is_vip,
        is_admin,
        is_online,
        last_seen,
        created_at,
        updated_at
      `,
      [uid]
    );

    if (result.rows.length === 0) {
      return res.status(400).json({
        error: "ALREADY_WATCHED_VIDEO",
      });
    }

    await emitLeaderboardIfChanged(beforeTop);
    await emitUserUpdated(uid);

    res.json({
      user: result.rows[0],
    });
  } catch (err) {
    res.status(500).json({
      error: err.message,
    });
  }
});

app.get("/users/:uid/home", async (req, res) => {
  try {
    const result = await pool.query(
      `
      SELECT
        name,
        score,
        created_at
      FROM users
      WHERE uid = $1
      LIMIT 1
      `,
      [req.params.uid]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({
        message: "USER_NOT_FOUND",
      });
    }

    const user = result.rows[0];

    const days = Math.floor(
      (new Date() - new Date(user.created_at)) / (1000 * 60 * 60 * 24)
    );

    setNoStore(res);

    res.json({
      name: user.name,
      score: user.score,
      days,
    });
  } catch (err) {
    res.status(500).json({
      message: err.message,
    });
  }
});

app.post("/users/add-score", async (req, res) => {
  try {
    const { uid, score } = req.body;
    const safeScore = Number(score) || 0;

    if (!uid || safeScore <= 0) {
      return res.status(400).json({
        success: false,
        error: "INVALID_DATA",
      });
    }

    const beforeTop = await getTopLeaderboard();

    const result = await pool.query(
      `
      UPDATE users
      SET
        score = score + $1,
        updated_at = NOW()
      WHERE uid = $2
      RETURNING score
      `,
      [safeScore, uid]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({
        success: false,
        error: "USER_NOT_FOUND",
      });
    }

    await emitLeaderboardIfChanged(beforeTop);
    await emitUserUpdated(uid);

    res.json({
      success: true,
      score: result.rows[0].score,
    });
  } catch (e) {
    res.status(500).json({
      success: false,
      error: e.message,
    });
  }
});

app.post("/users/deduct-score", async (req, res) => {
  try {
    const { uid, amount } = req.body;
    const safeAmount = Number(amount) || 0;

    if (!uid || safeAmount <= 0) {
      return res.status(400).json({
        success: false,
        error: "INVALID_DATA",
      });
    }

    const beforeTop = await getTopLeaderboard();

    const result = await pool.query(
      `
      UPDATE users
      SET
        score = score - $1,
        updated_at = NOW()
      WHERE uid = $2
      AND score >= $1
      RETURNING score
      `,
      [safeAmount, uid]
    );

    if (result.rowCount === 0) {
      return res.json({
        success: false,
        message: "NOT_ENOUGH_SCORE",
      });
    }

    await emitLeaderboardIfChanged(beforeTop);
    await emitUserUpdated(uid);

    res.json({
      success: true,
      score: result.rows[0].score,
    });
  } catch (e) {
    res.status(500).json({
      success: false,
      error: e.message,
    });
  }
});

app.get("/users/:uid", async (req, res) => {
  try {
    const result = await pool.query(
      `
      SELECT
        u.uid,
        u.name,
        u.email,
        u.player_id,
        u.avatar,
        u.score,
        u.is_vip,
        u.is_admin,
        u.is_online,
        u.last_seen,
        u.created_at,
        u.updated_at,
        (
          SELECT COUNT(*) + 1
          FROM users x
          WHERE x.score > u.score
        ) AS rank
      FROM users u
      WHERE u.uid = $1
      LIMIT 1
      `,
      [req.params.uid]
    );

    setNoStore(res);

    res.json({
      user: result.rows[0] || null,
    });
  } catch (e) {
    res.status(500).json({
      user: null,
      message: e.message,
    });
  }
});

// =========================
// QUIZ PROGRESS ROUTES
// =========================

app.post("/quiz-progress/save", async (req, res) => {
  try {
    const { uid, categoryId, levelId = "", questionIndex } = req.body;

    if (!uid || !categoryId || questionIndex == null) {
      return res.status(400).json({
        success: false,
        error: "INVALID_DATA",
      });
    }

    await pool.query(
      `
      INSERT INTO quiz_progress
      (uid, category_id, level_id, question_index, updated_at)
      VALUES ($1, $2, $3, $4, NOW())
      ON CONFLICT (uid, category_id, level_id)
      DO UPDATE SET
        question_index = EXCLUDED.question_index,
        updated_at = NOW()
      `,
      [uid, categoryId, levelId, questionIndex]
    );

    res.json({
      success: true,
    });
  } catch (e) {
    res.status(500).json({
      success: false,
      error: e.message,
    });
  }
});

app.get("/quiz-progress/load", async (req, res) => {
  try {
    const { uid, categoryId, levelId = "" } = req.query;

    if (!uid || !categoryId) {
      return res.status(400).json({
        progress: null,
        error: "INVALID_DATA",
      });
    }

    const result = await pool.query(
      `
      SELECT question_index
      FROM quiz_progress
      WHERE uid = $1
      AND category_id = $2
      AND level_id = $3
      LIMIT 1
      `,
      [uid, categoryId, levelId]
    );

    if (result.rows.length === 0) {
      return res.json({
        progress: null,
      });
    }

    res.json({
      progress: {
        questionIndex: result.rows[0].question_index,
      },
    });
  } catch (e) {
    res.status(500).json({
      progress: null,
      error: e.message,
    });
  }
});

app.delete("/quiz-progress/clear", async (req, res) => {
  try {
    const { uid, categoryId, levelId = "" } = req.body;

    if (!uid || !categoryId) {
      return res.status(400).json({
        success: false,
        error: "INVALID_DATA",
      });
    }

    await pool.query(
      `
      DELETE FROM quiz_progress
      WHERE uid = $1
      AND category_id = $2
      AND level_id = $3
      `,
      [uid, categoryId, levelId]
    );

    res.json({
      success: true,
    });
  } catch (e) {
    res.status(500).json({
      success: false,
      error: e.message,
    });
  }
});

// =========================
// FRIEND ROUTES - NEON
// =========================
// Toàn bộ phần bạn bè/lời mời kết bạn dùng Neon.
// Không còn lưu friends/friend_requests trên Firebase nữa.

// Load nhanh toàn bộ dữ liệu màn bạn bè trong 1 request:
// - friends
// - requests nhận được
// - sentRequests đã gửi
// - suggestions gợi ý kết bạn
app.get("/friends/bootstrap/:uid", async (req, res) => {
  try {
    const { uid } = req.params;

    if (!uid) {
      return res.status(400).json({
        friends: [],
        requests: [],
        sentRequests: [],
        suggestions: [],
      });
    }

    const [friends, requests, sentRequests, suggestions] = await Promise.all([
      pool.query(
        `
        SELECT
          u.uid,
          u.name,
          u.player_id AS "playerId",
          u.avatar,
          u.score
        FROM friends f
        JOIN users u ON u.uid = f.friend_id
        WHERE f.user_id = $1
        ORDER BY LOWER(u.name) ASC
        `,
        [uid]
      ),

      pool.query(
        `
        SELECT
          fr.id AS "requestId",
          fr.from_uid AS "fromUid",
          fr.to_uid AS "toUid",
          fr.status,
          fr.created_at AS "createdAt",
          u.name AS "fromName",
          u.player_id AS "fromPlayerId",
          u.avatar AS "fromAvatar",
          u.score AS "fromScore"
        FROM friend_requests fr
        JOIN users u ON u.uid = fr.from_uid
        WHERE fr.to_uid = $1
        AND fr.status = 'pending'
        ORDER BY fr.created_at DESC
        `,
        [uid]
      ),

      pool.query(
        `
        SELECT
          fr.id AS "requestId",
          fr.from_uid AS "fromUid",
          fr.to_uid AS "toUid",
          fr.status,
          fr.created_at AS "createdAt",
          u.name AS "toName",
          u.player_id AS "toPlayerId",
          u.avatar AS "toAvatar",
          u.score AS "toScore"
        FROM friend_requests fr
        JOIN users u ON u.uid = fr.to_uid
        WHERE fr.from_uid = $1
        AND fr.status = 'pending'
        ORDER BY fr.created_at DESC
        `,
        [uid]
      ),

      pool.query(
        `
        SELECT
          u.uid,
          u.name,
          u.player_id AS "playerId",
          u.avatar,
          u.score
        FROM users u
        WHERE u.uid <> $1
        AND u.uid NOT IN (
          SELECT friend_id FROM friends WHERE user_id = $1
        )
        AND u.uid NOT IN (
          SELECT to_uid FROM friend_requests
          WHERE from_uid = $1 AND status = 'pending'
        )
        AND u.uid NOT IN (
          SELECT from_uid FROM friend_requests
          WHERE to_uid = $1 AND status = 'pending'
        )
        ORDER BY RANDOM()
        LIMIT 5
        `,
        [uid]
      ),
    ]);

    setNoStore(res);

    res.json({
      friends: friends.rows,
      requests: requests.rows,
      sentRequests: sentRequests.rows,
      suggestions: suggestions.rows,
    });
  } catch (e) {
    console.error("FRIENDS BOOTSTRAP ERROR:", e);
    res.status(500).json({
      friends: [],
      requests: [],
      sentRequests: [],
      suggestions: [],
      error: e.message,
    });
  }
});

// Tìm user theo playerId
app.get("/friends/search", async (req, res) => {
  try {
    const playerId = req.query.playerId?.trim();
    const currentUid = req.query.currentUid?.trim();

    if (!playerId || !currentUid) {
      return res.json({ user: null });
    }

    const result = await pool.query(
      `
      SELECT
        uid,
        name,
        player_id AS "playerId",
        avatar,
        score
      FROM users
      WHERE LOWER(player_id) = LOWER($1)
      AND uid <> $2
      LIMIT 1
      `,
      [playerId, currentUid]
    );

    setNoStore(res);

    res.json({
      user: result.rows[0] || null,
    });
  } catch (e) {
    console.error("FRIEND SEARCH ERROR:", e);
    res.status(500).json({
      user: null,
      error: e.message,
    });
  }
});

// Gửi lời mời kết bạn
app.post("/friends/request", async (req, res) => {
  try {
    const { fromUid, toUid } = req.body;

    if (!fromUid || !toUid || fromUid === toUid) {
      return res.status(400).json({
        success: false,
        message: "INVALID_DATA",
      });
    }

    const isFriend = await pool.query(
      `
      SELECT 1
      FROM friends
      WHERE user_id = $1
      AND friend_id = $2
      LIMIT 1
      `,
      [fromUid, toUid]
    );

    if (isFriend.rows.length > 0) {
      return res.json({
        success: true,
        message: "ALREADY_FRIEND",
      });
    }

    const requestId = `${fromUid}_${toUid}`;
    const reverseRequestId = `${toUid}_${fromUid}`;

    const reverse = await pool.query(
      `
      SELECT status
      FROM friend_requests
      WHERE id = $1
      LIMIT 1
      `,
      [reverseRequestId]
    );

    if (reverse.rows[0]?.status === "pending") {
      return res.json({
        success: true,
        message: "REVERSE_PENDING",
      });
    }

    await pool.query(
      `
      INSERT INTO friend_requests
      (
        id,
        from_uid,
        to_uid,
        status,
        created_at
      )
      VALUES ($1, $2, $3, 'pending', NOW())
      ON CONFLICT (id)
      DO UPDATE SET
        status = 'pending',
        created_at = NOW(),
        accepted_at = NULL,
        declined_at = NULL,
        removed_at = NULL
      `,
      [requestId, fromUid, toUid]
    );

    notifyFriendChanged(fromUid);
    notifyFriendChanged(toUid);

    res.json({ success: true });
  } catch (e) {
    console.error("SEND FRIEND REQUEST ERROR:", e);
    res.status(500).json({
      success: false,
      error: e.message,
    });
  }
});

// Danh sách bạn bè
app.get("/friends/list/:uid", async (req, res) => {
  try {
    const result = await pool.query(
      `
      SELECT
        u.uid,
        u.name,
        u.player_id AS "playerId",
        u.avatar,
        u.score
      FROM friends f
      JOIN users u ON u.uid = f.friend_id
      WHERE f.user_id = $1
      ORDER BY LOWER(u.name) ASC
      `,
      [req.params.uid]
    );

    setShortCache(res, 3);

    res.json({
      friends: result.rows,
    });
  } catch (e) {
    console.error("GET FRIENDS ERROR:", e);
    res.status(500).json({
      friends: [],
      error: e.message,
    });
  }
});

// Lời mời nhận được
app.get("/friends/requests/:uid", async (req, res) => {
  try {
    const result = await pool.query(
      `
      SELECT
        fr.id AS "requestId",
        fr.from_uid AS "fromUid",
        fr.to_uid AS "toUid",
        fr.status,
        fr.created_at AS "createdAt",
        u.name AS "fromName",
        u.player_id AS "fromPlayerId",
        u.avatar AS "fromAvatar",
        u.score AS "fromScore"
      FROM friend_requests fr
      JOIN users u ON u.uid = fr.from_uid
      WHERE fr.to_uid = $1
      AND fr.status = 'pending'
      ORDER BY fr.created_at DESC
      `,
      [req.params.uid]
    );

    setNoStore(res);

    res.json({
      requests: result.rows,
    });
  } catch (e) {
    console.error("GET REQUESTS ERROR:", e);
    res.status(500).json({
      requests: [],
      error: e.message,
    });
  }
});

// Lời mời đã gửi
app.get("/friends/sent/:uid", async (req, res) => {
  try {
    const result = await pool.query(
      `
      SELECT
        fr.id AS "requestId",
        fr.from_uid AS "fromUid",
        fr.to_uid AS "toUid",
        fr.status,
        fr.created_at AS "createdAt",
        u.name AS "toName",
        u.player_id AS "toPlayerId",
        u.avatar AS "toAvatar",
        u.score AS "toScore"
      FROM friend_requests fr
      JOIN users u ON u.uid = fr.to_uid
      WHERE fr.from_uid = $1
      AND fr.status = 'pending'
      ORDER BY fr.created_at DESC
      `,
      [req.params.uid]
    );

    setNoStore(res);

    res.json({
      sentRequests: result.rows,
    });
  } catch (e) {
    console.error("GET SENT REQUESTS ERROR:", e);
    res.status(500).json({
      sentRequests: [],
      error: e.message,
    });
  }
});

// Chấp nhận kết bạn
app.post("/friends/accept", async (req, res) => {
  const client = await pool.connect();

  try {
    const { requestId, currentUid } = req.body;

    if (!requestId || !currentUid) {
      return res.status(400).json({
        success: false,
        message: "INVALID_DATA",
      });
    }

    await client.query("BEGIN");

    const request = await client.query(
      `
      SELECT *
      FROM friend_requests
      WHERE id = $1
      AND status = 'pending'
      LIMIT 1
      `,
      [requestId]
    );

    if (request.rows.length === 0) {
      await client.query("ROLLBACK");
      return res.json({
        success: false,
        message: "REQUEST_NOT_FOUND",
      });
    }

    const data = request.rows[0];

    if (data.to_uid !== currentUid) {
      await client.query("ROLLBACK");
      return res.status(403).json({
        success: false,
        message: "NO_PERMISSION",
      });
    }

    await client.query(
      `
      UPDATE friend_requests
      SET status = 'accepted',
          accepted_at = NOW()
      WHERE id = $1
      `,
      [requestId]
    );

    await client.query(
      `
      INSERT INTO friends (user_id, friend_id, created_at)
      VALUES ($1, $2, NOW())
      ON CONFLICT DO NOTHING
      `,
      [data.to_uid, data.from_uid]
    );

    await client.query(
      `
      INSERT INTO friends (user_id, friend_id, created_at)
      VALUES ($1, $2, NOW())
      ON CONFLICT DO NOTHING
      `,
      [data.from_uid, data.to_uid]
    );

    await client.query("COMMIT");

    notifyFriendChanged(data.from_uid);
    notifyFriendChanged(data.to_uid);

    res.json({ success: true });
  } catch (e) {
    await client.query("ROLLBACK").catch(() => { });
    console.error("ACCEPT FRIEND ERROR:", e);
    res.status(500).json({
      success: false,
      error: e.message,
    });
  } finally {
    client.release();
  }
});

// Từ chối lời mời
app.post("/friends/decline", async (req, res) => {
  try {
    const { requestId } = req.body;

    if (!requestId) {
      return res.status(400).json({
        success: false,
        message: "INVALID_DATA",
      });
    }

    const result = await pool.query(
      `
      UPDATE friend_requests
      SET status = 'declined',
          declined_at = NOW()
      WHERE id = $1
      RETURNING from_uid, to_uid
      `,
      [requestId]
    );

    const row = result.rows[0];
    if (row) {
      notifyFriendChanged(row.from_uid);
      notifyFriendChanged(row.to_uid);
    }

    res.json({ success: true });
  } catch (e) {
    console.error("DECLINE FRIEND ERROR:", e);
    res.status(500).json({
      success: false,
      error: e.message,
    });
  }
});

// Xóa bạn bè
app.post("/friends/remove", async (req, res) => {
  const client = await pool.connect();

  try {
    const { uid, friendUid } = req.body;

    if (!uid || !friendUid) {
      return res.status(400).json({
        success: false,
        message: "INVALID_DATA",
      });
    }

    await client.query("BEGIN");

    await client.query(
      `
      DELETE FROM friends
      WHERE user_id = $1
      AND friend_id = $2
      `,
      [uid, friendUid]
    );

    await client.query(
      `
      DELETE FROM friends
      WHERE user_id = $1
      AND friend_id = $2
      `,
      [friendUid, uid]
    );

    await client.query(
      `
      INSERT INTO friend_requests
      (
        id,
        from_uid,
        to_uid,
        status,
        removed_at
      )
      VALUES ($1, $2, $3, 'removed', NOW())
      ON CONFLICT (id)
      DO UPDATE SET
        status = 'removed',
        removed_at = NOW()
      `,
      [`${uid}_${friendUid}`, uid, friendUid]
    );

    await client.query("COMMIT");

    notifyFriendChanged(uid);
    notifyFriendChanged(friendUid);

    res.json({ success: true });
  } catch (e) {
    await client.query("ROLLBACK").catch(() => { });
    console.error("REMOVE FRIEND ERROR:", e);
    res.status(500).json({
      success: false,
      error: e.message,
    });
  } finally {
    client.release();
  }
});

// Gợi ý bạn bè
app.get("/friends/suggestions/:uid", async (req, res) => {
  try {
    const uid = req.params.uid;

    const result = await pool.query(
      `
      SELECT
        u.uid,
        u.name,
        u.player_id AS "playerId",
        u.avatar,
        u.score
      FROM users u
      WHERE u.uid <> $1
      AND u.uid NOT IN (
        SELECT friend_id FROM friends WHERE user_id = $1
      )
      AND u.uid NOT IN (
        SELECT to_uid FROM friend_requests
        WHERE from_uid = $1 AND status = 'pending'
      )
      AND u.uid NOT IN (
        SELECT from_uid FROM friend_requests
        WHERE to_uid = $1 AND status = 'pending'
      )
      ORDER BY RANDOM()
      LIMIT 5
      `,
      [uid]
    );

    setShortCache(res, 5);

    res.json({
      suggestions: result.rows,
    });
  } catch (e) {
    console.error("GET SUGGESTIONS ERROR:", e);
    res.status(500).json({
      suggestions: [],
      error: e.message,
    });
  }
});

// Gửi socket để client tự reload bạn bè khi có thay đổi
function notifyFriendChanged(uid) {
  if (!uid) return;

  const sockets = onlineUsers.get(uid);
  if (!sockets) return;

  for (const socketId of sockets) {
    io.to(socketId).emit("friend_updated", {
      uid,
      updatedAt: Date.now(),
    });
  }
}

// =========================
// DELETE USER
// =========================

app.delete("/users/:uid", async (req, res) => {
  const client = await pool.connect();

  try {
    const { uid } = req.params;

    await client.query("BEGIN");

    // Bạn bè
    await client.query(
      `
      DELETE FROM friends
      WHERE user_id = $1
         OR friend_id = $1
      `,
      [uid]
    );

    // Lời mời kết bạn
    await client.query(
      `
      DELETE FROM friend_requests
      WHERE from_uid = $1
         OR to_uid = $1
      `,
      [uid]
    );

    // Chat riêng
    await client.query(
      `
      DELETE FROM messages
      WHERE sender_id = $1
         OR receiver_id = $1
      `,
      [uid]
    );

    await client.query(
      `
      DELETE FROM friend_chats
      WHERE user1_id = $1
         OR user2_id = $1
      `,
      [uid]
    );

    // Tiến trình quiz
    await client.query(
      `
      DELETE FROM quiz_progress
      WHERE uid = $1
      `,
      [uid]
    );

    // Tin nhắn phòng
    await client.query(
      `
      DELETE FROM room_messages
      WHERE user_id = $1
      `,
      [uid]
    );

    // Phòng game
    await client.query(
      `
      DELETE FROM game_rooms
      WHERE host_id = $1
      `,
      [uid]
    );

    // User
    await client.query(
      `
      DELETE FROM users
      WHERE uid = $1
      `,
      [uid]
    );

    await client.query("COMMIT");

    // Đá user khỏi socket online
    onlineUsers.delete(uid);

    res.json({
      success: true,
    });
  } catch (e) {
    await client.query("ROLLBACK").catch(() => { });

    console.error("DELETE USER ERROR:", e);

    res.status(500).json({
      success: false,
      message: e.message,
    });
  } finally {
    client.release();
  }
});

// =========================
// START SERVER
// =========================

const PORT = process.env.PORT || 3000;

server.listen(PORT, () => {
  console.log(`Server running on ${PORT}`);
});