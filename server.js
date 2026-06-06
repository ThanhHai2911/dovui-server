const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");
const { admin, db } = require("./firebase");

const app = express();
app.use(cors());
app.use(express.json());

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
  if (!currentUserId || !friendId) return;

  const chatId = getFriendChatId(currentUserId, friendId);
  socket.join(chatId);

  try {
    const snap = await db
      .collection("friend_messages")
      .where("chatId", "==", chatId)
      .limit(50)
      .get();

    const messages = snap.docs
      .map((doc) => {
        const data = doc.data();

        return {
          id: doc.id,
          chatId: data.chatId,
          senderId: data.senderId,
          receiverId: data.receiverId,
          text: data.text,
          isRead: data.isRead ?? false,
          createdAt: data.createdAt?.toMillis?.() ?? Date.now(),
        };
      })
      .sort((a, b) => b.createdAt - a.createdAt);

    socket.emit("friend_messages", messages);
  } catch (error) {
    console.log("join_friend_chat error:", error);
    socket.emit("friend_messages", []);
  }
});

  socket.on("send_friend_message", async ({ currentUserId, friendId, text }) => {
    if (!currentUserId || !friendId || !text?.trim()) return;

    const cleanText = text.trim();
    const chatId = getFriendChatId(currentUserId, friendId);

    try {
      const chatRef = db.collection("friend_chats").doc(chatId);
      const msgRef = db.collection("friend_messages").doc();
      const now = admin.firestore.FieldValue.serverTimestamp();

      await db.runTransaction(async (tx) => {
        tx.set(
          chatRef,
          {
            chatId,
            users: [currentUserId, friendId],
            lastMessage: cleanText,
            lastMessageAt: now,
            updatedAt: now,
          },
          { merge: true }
        );

        tx.set(msgRef, {
          chatId,
          senderId: currentUserId,
          receiverId: friendId,
          text: cleanText,
          createdAt: now,
          isRead: false,
        });
      });

      io.to(chatId).emit("new_friend_message", {
        id: msgRef.id,
        chatId,
        senderId: currentUserId,
        receiverId: friendId,
        text: cleanText,
        createdAt: Date.now(),
        isRead: false,
      });
    } catch (error) {
      console.log("send_friend_message error:", error);
    }
  });

  socket.on("mark_friend_messages_read", async ({ currentUserId, friendId }) => {
    if (!currentUserId || !friendId) return;

    const chatId = getFriendChatId(currentUserId, friendId);

    try {
      const snap = await db
        .collection("friend_messages")
        .where("chatId", "==", chatId)
        .where("receiverId", "==", currentUserId)
        .where("isRead", "==", false)
        .get();

      const batch = db.batch();

      snap.docs.forEach((doc) => {
        batch.update(doc.ref, {
          isRead: true,
        });
      });

      await batch.commit();

      io.to(chatId).emit("friend_messages_read", {
        chatId,
        readerId: currentUserId,
      });
    } catch (error) {
      console.log("mark_friend_messages_read error:", error);
    }
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
});

const PORT = process.env.PORT || 3000;

server.listen(PORT, () => {
  console.log(`Server running on ${PORT}`);
});