const { db } = require("./firebase");
const pool = require("./db");

async function migrateUsers() {
  try {
    console.log("Start migrate users...");

    const snap = await db.collection("users").get();

    let total = 0;

    for (const doc of snap.docs) {
      const uid = doc.id;
      const data = doc.data();

      await pool.query(
        `
        INSERT INTO users
        (
          uid,
          name,
          email,
          player_id,
          avatar,
          score,
          rank,
          stars,
          is_vip,
          is_admin,
          is_online,
          created_at,
          updated_at
        )
        VALUES
        ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW(),NOW())
        ON CONFLICT (uid)
        DO UPDATE SET
          name = EXCLUDED.name,
          email = EXCLUDED.email,
          player_id = EXCLUDED.player_id,
          avatar = EXCLUDED.avatar,
          score = EXCLUDED.score,
          rank = EXCLUDED.rank,
          stars = EXCLUDED.stars,
          is_vip = EXCLUDED.is_vip,
          is_admin = EXCLUDED.is_admin,
          is_online = EXCLUDED.is_online,
          updated_at = NOW()
        `,
        [
          uid,
          data.name || "",
          data.email || "",
          data.playerId || data.player_id || null,
          data.avatar || "",
          Number(data.score || 0),
          Number(data.rank || 0),
          Number(data.stars || 0),
          data.isVip === true,
          data.isAdmin === true,
          data.isOnline === true,
        ]
      );

      total++;
      console.log(`Migrated user ${total}: ${uid}`);
    }

    console.log("Done. Total users:", total);
  } catch (error) {
    console.error("Migration error:", error);
  } finally {
    await pool.end();
    process.exit(0);
  }
}

migrateUsers();