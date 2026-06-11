require("dotenv").config();

const { admin, db } = require("./firebase");
const pool = require("./db");

function toDate(value) {
  if (!value) return new Date();

  if (typeof value.toDate === "function") {
    return value.toDate();
  }

  if (typeof value === "number") {
    return new Date(value);
  }

  if (typeof value === "string") {
    const date = new Date(value);
    return isNaN(date.getTime()) ? new Date() : date;
  }

  return new Date();
}

async function importCategories() {
  console.log("Import categories...");

  const snap = await db.collection("categories").get();

  for (const doc of snap.docs) {
    const data = doc.data();

    await pool.query(
      `
      INSERT INTO categories
      (id, name, image, type, sort_order, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6)
      ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        image = EXCLUDED.image,
        type = EXCLUDED.type,
        sort_order = EXCLUDED.sort_order,
        updated_at = EXCLUDED.updated_at
      `,
      [
        doc.id,
        data.name || null,
        data.image || null,
        data.type || null,
        data.order || null,
        toDate(data.updatedAt),
      ]
    );
  }

  console.log(`Imported categories: ${snap.size}`);
}

async function importUsers() {
  console.log("Import users...");

  const snap = await db.collection("users").get();

  for (const doc of snap.docs) {
    const data = doc.data();

    await pool.query(
      `
      INSERT INTO users
      (uid, display_name, email, player_id, avatar_url, created_at)
      VALUES ($1,$2,$3,$4,$5,$6)
      ON CONFLICT (uid) DO UPDATE SET
        display_name = EXCLUDED.display_name,
        email = EXCLUDED.email,
        player_id = EXCLUDED.player_id,
        avatar_url = EXCLUDED.avatar_url
      `,
      [
        doc.id,
        data.displayName || data.name || data.fullName || data.username || null,
        data.email || null,
        data.playerId || null,
        data.avatarUrl || data.photoUrl || data.image || null,
        toDate(data.createdAt),
      ]
    );
  }

  console.log(`Imported users: ${snap.size}`);
}

async function importFriends() {
  console.log("Import friends...");

  const snap = await db.collection("friends").get();

  for (const doc of snap.docs) {
    const data = doc.data();

    const userId = data.userId || data.uid || data.fromUid || null;
    const friendId = data.friendId || data.toUid || null;

    if (!userId || !friendId) {
      console.log("Skip friend:", doc.id, data);
      continue;
    }

    await pool.query(
      `
      INSERT INTO friends
      (user_id, friend_id, created_at)
      VALUES ($1,$2,$3)
      ON CONFLICT DO NOTHING
      `,
      [userId, friendId, toDate(data.createdAt)]
    );
  }

  console.log(`Imported friends: ${snap.size}`);
}

async function importFriendRequests() {
  console.log("Import friend_requests...");

  const snap = await db.collection("friend_requests").get();

  for (const doc of snap.docs) {
    const data = doc.data();

    const fromUid = data.fromUid || data.fromUserId || null;
    const toUid = data.toUid || data.toUserId || null;

    if (!fromUid || !toUid) {
      console.log("Skip friend_request:", doc.id, data);
      continue;
    }

    await pool.query(
      `
      INSERT INTO friend_requests
      (from_uid, to_uid, status, created_at)
      VALUES ($1,$2,$3,$4)
      ON CONFLICT DO NOTHING
      `,
      [
        fromUid,
        toUid,
        data.status || "pending",
        toDate(data.createdAt),
      ]
    );
  }

  console.log(`Imported friend_requests: ${snap.size}`);
}

async function importFriendChats() {
  console.log("Import friend_chats...");

  const chatsSnap = await db.collection("friend_chats").get();

  for (const chatDoc of chatsSnap.docs) {
    const chatId = chatDoc.id;
    const chat = chatDoc.data();
    const ids = chatId.split("_");

    await pool.query(
      `
      INSERT INTO friend_chats
      (chat_id, user1_id, user2_id, last_message, updated_at, created_at)
      VALUES ($1,$2,$3,$4,$5,$6)
      ON CONFLICT (chat_id) DO UPDATE SET
        last_message = EXCLUDED.last_message,
        updated_at = EXCLUDED.updated_at
      `,
      [
        chatId,
        chat.user1Id || chat.members?.[0] || ids[0] || null,
        chat.user2Id || chat.members?.[1] || ids[1] || null,
        chat.lastMessage || null,
        toDate(chat.updatedAt || chat.lastMessageAt),
        toDate(chat.createdAt),
      ]
    );

    const messagesSnap = await db
      .collection("friend_chats")
      .doc(chatId)
      .collection("messages")
      .get();

    for (const msgDoc of messagesSnap.docs) {
      const msg = msgDoc.data();

      await pool.query(
        `
        INSERT INTO messages
        (chat_id, sender_id, receiver_id, text, type, media_url, is_read, created_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        `,
        [
          chatId,
          msg.senderId || msg.currentUserId || msg.userId || null,
          msg.receiverId || msg.friendId || null,
          msg.text || null,
          msg.type || "text",
          msg.mediaUrl || null,
          msg.isRead === true,
          toDate(msg.createdAt || msg.sentAt),
        ]
      );
    }

    console.log(`Imported chat ${chatId}, messages: ${messagesSnap.size}`);
  }

  console.log(`Imported friend_chats: ${chatsSnap.size}`);
}

async function importLevelUser() {
  console.log("Import level_user...");

  const snap = await db.collection("level_user").get();

  for (const doc of snap.docs) {
    await pool.query(
      `
      INSERT INTO level_user
      (uid, data)
      VALUES ($1,$2)
      ON CONFLICT (uid)
      DO UPDATE SET
      data = EXCLUDED.data
      `,
      [
        doc.id,
        JSON.stringify(doc.data()),
      ]
    );
  }

  console.log(`Imported level_user: ${snap.size}`);
}

async function importUserVip() {
  console.log("Import user_vip...");

  const snap = await db.collection("user_vip").get();

  for (const doc of snap.docs) {
    await pool.query(
      `
      INSERT INTO user_vip
      (uid, data)
      VALUES ($1,$2)
      ON CONFLICT (uid)
      DO UPDATE SET
      data = EXCLUDED.data
      `,
      [
        doc.id,
        JSON.stringify(doc.data()),
      ]
    );
  }

  console.log(`Imported user_vip: ${snap.size}`);
}

async function main() {
  try {
    console.log("Migration started...");

    await importCategories();
    await importUsers();
    await importFriends();
    await importFriendRequests();
    await importFriendChats();

    await importLevelUser();
    await importUserVip();

    console.log("Migration completed.");
  } catch (error) {
    console.error("Migration failed:", error);
  } finally {
    await pool.end();
  }
}

main(); 