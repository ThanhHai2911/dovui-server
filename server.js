const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");
const { admin, db } = require("./firebase");


const pool = require("./db");
const app = express();
app.use(cors());
app.use(express.json());

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

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
  },
});

const onlineUsers = new Map();
const rooms = new Map();

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
io.on("connection", (socket) => {
  console.log("Connected:", socket.id);

  socket.emit("online_users", getOnlineUserIds());

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

  //FREND CHAT EVENTS

  socket.on("send_message", ({ roomId, message }) => {
    if (!roomId || !message) return;

    const msg = {
      id: Date.now().toString(),
      ...message,
      createdAt: Date.now(),
    };

    const room = rooms.get(roomId);
    if (room) {
      room.messages.push(msg);

      if (room.messages.length > 100) {
        room.messages.shift();
      }
    }

    io.to(roomId).emit("new_message", msg);
  });

  socket.on("invite_friend", ({ fromUid, toUid, roomId, fromName }) => {
    if (!fromUid || !toUid || !roomId) return;

    const sockets = onlineUsers.get(toUid);
    if (!sockets) return;

    for (const socketId of sockets) {
      io.to(socketId).emit("game_invite", {
        fromUid,
        fromName,
        roomId,
      });
    }
  });

  socket.on("submit_answer", ({ roomId, uid, score }) => {
    if (!roomId || !uid) return;

    const room = rooms.get(roomId);
    if (!room) return;

    const player = room.players.find((p) => p.uid === uid);
    if (player) {
      player.score = score;
    }

    io.to(roomId).emit("room_updated", room);
  });

  socket.on("join_friend_chat", async ({ currentUserId, friendId }) => {
    try {
      const chatId = getFriendChatId(currentUserId, friendId);

      socket.join(chatId);

      const result = await pool.query(
        `
  SELECT *
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
        mediaUrl: row.media_url,
        isRead: row.is_read === true,
        createdAt: row.created_at
          ? new Date(row.created_at).getTime()
          : Date.now(),
      }));

      socket.emit("friend_messages", messages);
    } catch (error) {
      console.error("join_friend_chat error:", error);

      socket.emit("friend_chat_error", {
        message: "Không thể tải tin nhắn",
      });
    }
  });

  socket.on(
    "send_friend_message",
    async ({ currentUserId, friendId, text }) => {
      try {
        if (!currentUserId || !friendId || !text?.trim()) return;

        const chatId = getFriendChatId(currentUserId, friendId);

        const messageResult = await pool.query(
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
        VALUES ($1,$2,$3,$4,$5,$6)
        RETURNING *
        `,
          [
            chatId,
            currentUserId,
            friendId,
            text.trim(),
            "text",
            false,
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
        VALUES ($1,$2,$3,$4,NOW())
        ON CONFLICT (chat_id)
        DO UPDATE SET
          last_message = EXCLUDED.last_message,
          updated_at = NOW()
        `,
          [
            chatId,
            currentUserId,
            friendId,
            text.trim(),
          ]
        );

        const row = messageResult.rows[0];

        const messageToSend = {
          id: row.id.toString(),
          chatId: row.chat_id,
          senderId: row.sender_id,
          receiverId: row.receiver_id,
          text: row.text || "",
          type: row.type || "text",
          mediaUrl: row.media_url,
          isRead: row.is_read === true,
          createdAt: row.created_at
            ? new Date(row.created_at).getTime()
            : Date.now(),
        };

        io.to(chatId).emit(
          "new_friend_message",
          messageToSend
        );
      } catch (error) {
        console.error("send_friend_message error:", error);
      }
    }
  );

  socket.on(
    "mark_friend_messages_read",
    async ({ currentUserId, friendId }) => {
      try {
        const chatId = getFriendChatId(
          currentUserId,
          friendId
        );

        await pool.query(
          `
        UPDATE messages
        SET is_read = true
        WHERE chat_id = $1
        AND receiver_id = $2
        AND is_read = false
        `,
          [chatId, currentUserId]
        );

        io.to(chatId).emit(
          "friend_messages_read",
          {
            chatId,
            readerId: currentUserId,
          }
        );
      } catch (error) {
        console.error(
          "mark_friend_messages_read error:",
          error
        );
      }
    }
  );

  //ROOM EVENTS
  // =========================
  // CREATE GAME ROOM
  // =========================
  socket.on("create_game_room", async (data, callback) => {
    try {
      console.log("CREATE GAME ROOM:", data);

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

      if (!uid || !displayName || !categoryId || !type) {
        return callback?.({
          success: false,
          message: "Thiếu dữ liệu tạo phòng",
        });
      }

      let roomId = generateRoomId();

      while ((await db.collection("rooms").doc(roomId).get()).exists) {
        roomId = generateRoomId();
      }

      const roomRef = db.collection("rooms").doc(roomId);

      const roomData = {
        roomId,
        hostId: uid,
        hostName: displayName,
        categoryId,
        categoryName,
        type,
        password,
        questionCount,
        timePerQuestion,
        status: "waiting",
        kickedUserIds: [],
        invitedUsers: [],
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        players: [
          {
            userId: uid,
            displayName,
            score: 0,
            isHost: true,
            isReady: true,
            isFinished: false,
            joinedAt: Date.now(),
          },
        ],
      };

      await roomRef.set(roomData);

      socket.join(roomId);

      callback?.({
        success: true,
        roomId,
      });

      io.to(roomId).emit("room_updated", {
        ...roomData,
        createdAt: Date.now(),
        updatedAt: Date.now(),
      });
    } catch (error) {
      console.error("create_game_room error:", error);
      callback?.({
        success: false,
        message: "Không thể tạo phòng",
      });
    }
  });

  // =========================
  // JOIN GAME ROOM
  // =========================
  socket.on("join_game_room", async (data, callback) => {
    try {
      console.log("JOIN GAME ROOM:", data);

      const {
        roomId,
        uid,
        displayName,
        password = "",
        isDirectJoin = false,
      } = data;

      if (!roomId || !uid || !displayName) {
        return callback?.({
          success: false,
          message: "Thiếu dữ liệu vào phòng",
        });
      }

      const roomRef = db.collection("rooms").doc(roomId);

      await db.runTransaction(async (transaction) => {
        const snap = await transaction.get(roomRef);

        if (!snap.exists) {
          throw new Error("ROOM_NOT_FOUND");
        }

        const room = snap.data();

        if (room.status !== "waiting") {
          throw new Error("ROOM_NOT_WAITING");
        }

        const invitedUsers = Array.isArray(room.invitedUsers)
          ? room.invitedUsers
          : [];

        if (isDirectJoin && !invitedUsers.includes(uid)) {
          throw new Error("NOT_INVITED");
        }

        if (!isDirectJoin && room.password && room.password !== password) {
          throw new Error("WRONG_PASSWORD");
        }

        const players = Array.isArray(room.players) ? room.players : [];

        if (players.length >= 8) {
          throw new Error("ROOM_FULL");
        }

        const exists = players.some((p) => p.userId === uid);

        let newPlayers = players;

        if (!exists) {
          newPlayers = [
            ...players,
            {
              userId: uid,
              displayName,
              score: 0,
              isHost: false,
              isReady: false,
              isFinished: false,
              joinedAt: Date.now(),
            },
          ];
        }

        const kickedUserIds = Array.isArray(room.kickedUserIds)
          ? room.kickedUserIds.filter((id) => id !== uid)
          : [];

        transaction.update(roomRef, {
          players: newPlayers,
          kickedUserIds,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        });
      });

      const latestSnap = await roomRef.get();
      const latestRoom = latestSnap.data();

      socket.join(roomId);

      callback?.({
        success: true,
        roomId,
      });

      io.to(roomId).emit("room_updated", {
        ...latestRoom,
        updatedAt: Date.now(),
      });
    } catch (error) {
      console.error("join_game_room error:", error.message);

      let message = "Không thể vào phòng";

      if (error.message === "ROOM_NOT_FOUND") {
        message = "Phòng không tồn tại";
      }

      if (error.message === "ROOM_NOT_WAITING") {
        message = "Phòng đã bắt đầu";
      }

      if (error.message === "WRONG_PASSWORD") {
        message = "Sai mật khẩu";
      }

      if (error.message === "NOT_INVITED") {
        message = "Bạn không có quyền tham gia phòng này";
      }

      if (error.message === "ROOM_FULL") {
        message = "Phòng đã đầy";
      }

      callback?.({
        success: false,
        message,
      });
    }
  });

  // =========================
  // LEAVE GAME ROOM
  // =========================
  socket.on("leave_game_room", async (data, callback) => {
    try {
      const { roomId, uid } = data;

      if (!roomId || !uid) {
        return callback?.({
          success: false,
          message: "Thiếu dữ liệu rời phòng",
        });
      }

      const roomRef = db.collection("rooms").doc(roomId);
      const snap = await roomRef.get();

      if (!snap.exists) {
        socket.leave(roomId);
        return callback?.({ success: true });
      }

      const room = snap.data();
      const players = Array.isArray(room.players) ? room.players : [];

      const currentPlayer = players.find((p) => p.userId === uid);
      const newPlayers = players.filter((p) => p.userId !== uid);

      socket.leave(roomId);

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
          const messagesSnap = await roomRef.collection("messages").get();

          const batch = db.batch();

          messagesSnap.forEach((doc) => {
            batch.delete(doc.ref);
          });

          batch.delete(roomRef);

          await batch.commit();

          rooms.delete(roomId);
          io.in(roomId).socketsLeave(roomId);
        }, 500);

        return;
      }

      await roomRef.update({
        players: newPlayers,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });

      const latestRoom = {
        ...room,
        players: newPlayers,
        updatedAt: Date.now(),
      };

      io.to(roomId).emit("room_updated", latestRoom);

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

  // =========================
  // KICK PLAYER
  // =========================
  socket.on("kick_player", async (data, callback) => {
    try {
      const { roomId, uid, targetUserId } = data;

      if (!roomId || !uid || !targetUserId) {
        return callback?.({
          success: false,
          message: "Thiếu dữ liệu kick",
        });
      }

      const roomRef = db.collection("rooms").doc(roomId);
      const snap = await roomRef.get();

      if (!snap.exists) {
        return callback?.({
          success: false,
          message: "Phòng không tồn tại",
        });
      }

      const room = snap.data();

      if (room.hostId !== uid) {
        return callback?.({
          success: false,
          message: "Bạn không phải chủ phòng",
        });
      }

      const players = Array.isArray(room.players) ? room.players : [];
      const kickedUserIds = Array.isArray(room.kickedUserIds)
        ? room.kickedUserIds
        : [];

      const newPlayers = players.filter((p) => p.userId !== targetUserId);

      if (!kickedUserIds.includes(targetUserId)) {
        kickedUserIds.push(targetUserId);
      }

      await roomRef.update({
        players: newPlayers,
        kickedUserIds,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });

      io.to(roomId).emit("room_updated", {
        ...room,
        players: newPlayers,
        kickedUserIds,
        updatedAt: Date.now(),
      });

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

  // =========================
  // CLOSE GAME ROOM
  // =========================
  socket.on("close_game_room", async (data, callback) => {
    try {
      const { roomId, uid } = data;

      if (!roomId || !uid) {
        return callback?.({
          success: false,
          message: "Thiếu dữ liệu đóng phòng",
        });
      }

      const roomRef = db.collection("rooms").doc(roomId);
      const snap = await roomRef.get();

      if (!snap.exists) {
        return callback?.({
          success: true,
        });
      }

      const room = snap.data();

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
        await roomRef.delete();
        rooms.delete(roomId);
        io.in(roomId).socketsLeave(roomId);
      }, 500);
    } catch (error) {
      console.error("close_game_room error:", error);
      callback?.({
        success: false,
        message: "Không thể đóng phòng",
      });
    }
  });

  // =========================
  // OLD ROOM CHAT
  // =========================
  socket.on("join_room", ({ roomId, uid, name }) => {
    if (!roomId || !uid) return;

    socket.join(roomId);

    if (!rooms.has(roomId)) {
      rooms.set(roomId, {
        roomId,
        players: [],
        messages: [],
      });
    }

    const room = rooms.get(roomId);

    const exists = room.players.some((p) => p.uid === uid);
    if (!exists) {
      room.players.push({
        uid,
        name,
        score: 0,
        isFinished: false,
      });
    }

    io.to(roomId).emit("room_updated", room);
  });

  socket.on("leave_room", ({ roomId, uid }) => {
    if (!roomId || !uid) return;

    socket.leave(roomId);

    const room = rooms.get(roomId);
    if (!room) return;

    room.players = room.players.filter((p) => p.uid !== uid);

    if (room.players.length === 0) {
      rooms.delete(roomId);
      return;
    }

    io.to(roomId).emit("room_updated", room);
  });

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

  socket.on("join-room-chat", ({ roomId }) => {
    socket.join(`room_chat_${roomId}`);
  });

  socket.on("leave-room-chat", ({ roomId }) => {
    socket.leave(`room_chat_${roomId}`);
  });

  socket.on("send-room-message", async (data) => {
    const { roomId, userId, displayName, text } = data;

    if (!roomId || !userId || !text?.trim()) return;

    const now = new Date();

    const messageForSocket = {
      roomId,
      userId,
      displayName: displayName || "Ẩn danh",
      text: text.trim(),
      sentAt: now.toISOString(),
    };

    await db
      .collection("rooms")
      .doc(roomId)
      .collection("messages")
      .add({
        userId,
        displayName: displayName || "Ẩn danh",
        text: text.trim(),
        sentAt: admin.firestore.FieldValue.serverTimestamp(),
      });

    io.to(`room_chat_${roomId}`).emit("room-message", messageForSocket);
  });

});
app.get("/", (req, res) => {
  res.send("Dovui Server Running");
});

const PORT = process.env.PORT || 3000;

server.listen(PORT, () => {
  console.log(`Server running on ${PORT}`);
});