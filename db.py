# db.py
import aiosqlite
import time
import os
from typing import List, Dict, Optional

DB_PATH = os.path.join(os.path.dirname(__file__), "bot.db")


# ===================== INIT + MIGRATIONS =====================

async def init_db():
	async with aiosqlite.connect(DB_PATH) as db:

		# --- USERS (служебная) ---
		await db.execute("""
		CREATE TABLE IF NOT EXISTS users (
			user_id INTEGER PRIMARY KEY,
			state TEXT DEFAULT '',
			flow_status TEXT DEFAULT '',
			last_start_at TEXT DEFAULT '',
			updated_at TEXT DEFAULT ''
		)
		""")

		# --- FLOWS ---
		await db.execute("""
		CREATE TABLE IF NOT EXISTS flows (
			name TEXT PRIMARY KEY,
			sort_order INTEGER NOT NULL DEFAULT 0
		)
		""")

		# --- CONTENT BLOCKS ---
		# ⚠️ важно: запятые между колонками
		await db.execute("""
		CREATE TABLE IF NOT EXISTS content_blocks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			flow TEXT NOT NULL,
			position INTEGER NOT NULL,
			type TEXT NOT NULL,
			title TEXT DEFAULT '',
			text TEXT DEFAULT '',
			circle_path TEXT DEFAULT '',
			video_url TEXT DEFAULT '',
			buttons_json TEXT DEFAULT '',
			delay_seconds REAL DEFAULT 1.0,
			is_active INTEGER DEFAULT 1,

			-- attachments
			file_path TEXT DEFAULT '',
			file_kind TEXT DEFAULT '',
			file_name TEXT DEFAULT ''
		)
		""")

		# --- JOBS ---
		await db.execute("""
		CREATE TABLE IF NOT EXISTS jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id INTEGER NOT NULL,
			flow TEXT NOT NULL,
			run_at_ts INTEGER NOT NULL,
			is_done INTEGER DEFAULT 0
		)
		""")

		# чтобы не плодить дубли (одно задание на (user, flow))
		await db.execute("""
		CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_user_flow
		ON jobs(user_id, flow)
		""")

		# --- BOT USERS (АНАЛИТИКА) ---
		await db.execute("""
		CREATE TABLE IF NOT EXISTS bot_users (
			user_id INTEGER PRIMARY KEY,
			username TEXT DEFAULT '',
			first_seen_ts INTEGER NOT NULL,
			last_seen_ts INTEGER NOT NULL,
			starts_count INTEGER NOT NULL DEFAULT 0,
			messages_count INTEGER NOT NULL DEFAULT 0
		)
		""")

		# --- FLOW TRIGGERS ---
		await db.execute("""
		CREATE TABLE IF NOT EXISTS flow_triggers (
			flow TEXT PRIMARY KEY,
			trigger TEXT NOT NULL DEFAULT 'after_start',
			offset_seconds INTEGER NOT NULL DEFAULT 0,
			is_active INTEGER NOT NULL DEFAULT 1
		)
		""")

		await db.commit()

		# ---------------- MIGRATIONS ----------------

		# flows.sort_order
		cur = await db.execute("PRAGMA table_info(flows)")
		flows_cols = {r[1] for r in await cur.fetchall()}
		if "sort_order" not in flows_cols:
			await db.execute("ALTER TABLE flows ADD COLUMN sort_order INTEGER NOT NULL DEFAULT 0")

		# content_blocks additions
		cur = await db.execute("PRAGMA table_info(content_blocks)")
		cb_cols = {r[1] for r in await cur.fetchall()}

		if "delay_seconds" not in cb_cols:
			await db.execute("ALTER TABLE content_blocks ADD COLUMN delay_seconds REAL DEFAULT 1.0")
		if "title" not in cb_cols:
			await db.execute("ALTER TABLE content_blocks ADD COLUMN title TEXT DEFAULT ''")
		if "file_path" not in cb_cols:
			await db.execute("ALTER TABLE content_blocks ADD COLUMN file_path TEXT DEFAULT ''")
		if "file_kind" not in cb_cols:
			await db.execute("ALTER TABLE content_blocks ADD COLUMN file_kind TEXT DEFAULT ''")
		if "file_name" not in cb_cols:
			await db.execute("ALTER TABLE content_blocks ADD COLUMN file_name TEXT DEFAULT ''")

		# jobs migration: если старая таблица jobs с run_at TEXT
		cur = await db.execute("PRAGMA table_info(jobs)")
		j_cols = {r[1] for r in await cur.fetchall()}
		if "run_at_ts" not in j_cols:
			await db.execute("""
			CREATE TABLE IF NOT EXISTS jobs_new (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				user_id INTEGER NOT NULL,
				flow TEXT NOT NULL,
				run_at_ts INTEGER NOT NULL,
				is_done INTEGER DEFAULT 0
			)
			""")
			if "run_at" in j_cols:
				await db.execute("""
				INSERT INTO jobs_new(id, user_id, flow, run_at_ts, is_done)
				SELECT id, user_id, flow,
					   CASE
						 WHEN typeof(run_at)='integer' THEN run_at
						 ELSE CAST(run_at AS INTEGER)
					   END,
					   is_done
				FROM jobs
				""")
			await db.execute("DROP TABLE jobs")
			await db.execute("ALTER TABLE jobs_new RENAME TO jobs")
			await db.execute("""
			CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_user_flow
			ON jobs(user_id, flow)
			""")

		# flow_triggers migrations (если таблица добавлена позже)
		cur = await db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='flow_triggers'")
		if not await cur.fetchone():
			await db.execute("""
			CREATE TABLE IF NOT EXISTS flow_triggers (
				flow TEXT PRIMARY KEY,
				trigger TEXT NOT NULL DEFAULT 'after_start',
				offset_seconds INTEGER NOT NULL DEFAULT 0,
				is_active INTEGER NOT NULL DEFAULT 1
			)
			""")

		await db.commit()

		# восстановление flows, если пусто
		cur = await db.execute("SELECT COUNT(*) FROM flows")
		if (await cur.fetchone())[0] == 0:
			cur = await db.execute("SELECT DISTINCT flow FROM content_blocks ORDER BY flow")
			order = 1
			for (flow,) in await cur.fetchall():
				await db.execute(
					"INSERT OR IGNORE INTO flows(name, sort_order) VALUES (?, ?)",
					(flow, order)
				)
				order += 1
			await db.commit()


# ===================== BOT ANALYTICS =====================

async def inc_start(user_id: int, username: Optional[str]):
	now = int(time.time())
	username = (username or "").strip()

	async with aiosqlite.connect(DB_PATH) as db:
		await db.execute("""
		INSERT INTO bot_users(user_id, username, first_seen_ts, last_seen_ts, starts_count, messages_count)
		VALUES (?, ?, ?, ?, 1, 0)
		ON CONFLICT(user_id) DO UPDATE SET
			username=excluded.username,
			last_seen_ts=excluded.last_seen_ts,
			starts_count=starts_count + 1
		""", (user_id, username, now, now))
		await db.commit()


async def inc_message(user_id: int, username: Optional[str]):
	now = int(time.time())
	username = (username or "").strip()

	async with aiosqlite.connect(DB_PATH) as db:
		await db.execute("""
		INSERT INTO bot_users(user_id, username, first_seen_ts, last_seen_ts, starts_count, messages_count)
		VALUES (?, ?, ?, ?, 0, 1)
		ON CONFLICT(user_id) DO UPDATE SET
			username=excluded.username,
			last_seen_ts=excluded.last_seen_ts,
			messages_count=messages_count + 1
		""", (user_id, username, now, now))
		await db.commit()


async def get_stats():
	now = int(time.time())
	hour_ago = now - 3600

	async with aiosqlite.connect(DB_PATH) as db:
		total_users = (await (await db.execute(
			"SELECT COUNT(*) FROM bot_users"
		)).fetchone())[0]

		active_last_hour = (await (await db.execute(
			"SELECT COUNT(*) FROM bot_users WHERE last_seen_ts >= ?",
			(hour_ago,)
		)).fetchone())[0]

		starts, messages = await (await db.execute(
			"SELECT COALESCE(SUM(starts_count),0), COALESCE(SUM(messages_count),0) FROM bot_users"
		)).fetchone()

	return {
		"total_users": total_users,
		"active_last_hour": active_last_hour,
		"total_starts": starts,
		"total_messages": messages,
	}


async def get_users(limit: int = 500):
	async with aiosqlite.connect(DB_PATH) as db:
		cur = await db.execute("""
		SELECT user_id, username, first_seen_ts, last_seen_ts, starts_count, messages_count
		FROM bot_users
		ORDER BY last_seen_ts DESC
		LIMIT ?
		""", (limit,))
		rows = await cur.fetchall()

	return [
		{
			"user_id": r[0],
			"username": r[1],
			"first_seen_ts": r[2],
			"last_seen_ts": r[3],
			"starts_count": r[4],
			"messages_count": r[5],
		}
		for r in rows
	]


# ===================== FLOW TRIGGERS =====================

async def get_flow_triggers() -> List[Dict]:
	async with aiosqlite.connect(DB_PATH) as db:
		cur = await db.execute("""
		SELECT flow, trigger, offset_seconds, is_active
		FROM flow_triggers
		ORDER BY offset_seconds ASC, flow ASC
		""")
		rows = await cur.fetchall()

	return [
		{
			"flow": r[0],
			"trigger": r[1],
			"offset_seconds": int(r[2] or 0),
			"is_active": int(r[3] or 0),
		}
		for r in rows
	]


async def set_flow_trigger(flow: str, offset_seconds: int, is_active: int = 1, trigger: str = "after_start") -> None:
	flow = (flow or "").strip()
	if not flow:
		return

	offset_seconds = int(offset_seconds)
	is_active = 1 if int(is_active) else 0
	trigger = (trigger or "after_start").strip() or "after_start"

	async with aiosqlite.connect(DB_PATH) as db:
		await db.execute("""
		INSERT INTO flow_triggers(flow, trigger, offset_seconds, is_active)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(flow) DO UPDATE SET
			trigger=excluded.trigger,
			offset_seconds=excluded.offset_seconds,
			is_active=excluded.is_active
		""", (flow, trigger, offset_seconds, is_active))
		await db.commit()


async def delete_flow_trigger(flow: str) -> None:
	flow = (flow or "").strip()
	if not flow:
		return

	async with aiosqlite.connect(DB_PATH) as db:
		await db.execute("DELETE FROM flow_triggers WHERE flow=?", (flow,))
		await db.commit()


# ===================== JOBS =====================

async def upsert_job(user_id: int, flow: str, run_at_ts: int) -> None:
	flow = (flow or "").strip()
	if not flow:
		return
	run_at_ts = int(run_at_ts)

	async with aiosqlite.connect(DB_PATH) as db:
		await db.execute("""
		INSERT INTO jobs(user_id, flow, run_at_ts, is_done)
		VALUES (?, ?, ?, 0)
		ON CONFLICT(user_id, flow) DO UPDATE SET
			run_at_ts=excluded.run_at_ts,
			is_done=0
		""", (int(user_id), flow, run_at_ts))
		await db.commit()


async def fetch_due_jobs(limit: int = 50) -> List[Dict]:
	now = int(time.time())
	async with aiosqlite.connect(DB_PATH) as db:
		cur = await db.execute("""
		SELECT id, user_id, flow, run_at_ts
		FROM jobs
		WHERE is_done=0 AND run_at_ts <= ?
		ORDER BY run_at_ts ASC
		LIMIT ?
		""", (now, int(limit)))
		rows = await cur.fetchall()

	return [{"id": r[0], "user_id": r[1], "flow": r[2], "run_at_ts": r[3]} for r in rows]


async def mark_job_done(job_id: int) -> None:
	async with aiosqlite.connect(DB_PATH) as db:
		await db.execute("UPDATE jobs SET is_done=1 WHERE id=?", (int(job_id),))
		await db.commit()


# ===================== FLOWS =====================

async def get_flows() -> List[str]:
	async with aiosqlite.connect(DB_PATH) as db:
		cur = await db.execute("SELECT name FROM flows ORDER BY sort_order ASC")
		return [r[0] for r in await cur.fetchall()]


async def create_flow(name: str) -> None:
	name = (name or "").strip()
	if not name:
		return

	async with aiosqlite.connect(DB_PATH) as db:
		cur = await db.execute("SELECT COALESCE(MAX(sort_order), 0) FROM flows")
		mx = (await cur.fetchone())[0] or 0
		await db.execute(
			"INSERT OR IGNORE INTO flows(name, sort_order) VALUES (?, ?)",
			(name, int(mx) + 1)
		)
		await db.commit()


async def delete_flow(name: str) -> None:
	async with aiosqlite.connect(DB_PATH) as db:
		await db.execute("DELETE FROM content_blocks WHERE flow=?", (name,))
		await db.execute("DELETE FROM jobs WHERE flow=?", (name,))
		await db.execute("DELETE FROM flow_triggers WHERE flow=?", (name,))
		await db.execute("DELETE FROM flows WHERE name=?", (name,))
		await db.commit()


async def move_flow(name: str, direction: str) -> None:
	async with aiosqlite.connect(DB_PATH) as db:
		cur = await db.execute("SELECT name, sort_order FROM flows ORDER BY sort_order ASC")
		rows = await cur.fetchall()
		idx = next((i for i, r in enumerate(rows) if r[0] == name), None)
		if idx is None:
			return

		if direction == "up" and idx > 0:
			a_name, a_ord = rows[idx][0], rows[idx][1]
			b_name, b_ord = rows[idx - 1][0], rows[idx - 1][1]
		elif direction == "down" and idx < len(rows) - 1:
			a_name, a_ord = rows[idx][0], rows[idx][1]
			b_name, b_ord = rows[idx + 1][0], rows[idx + 1][1]
		else:
			return

		await db.execute("UPDATE flows SET sort_order=? WHERE name=?", (b_ord, a_name))
		await db.execute("UPDATE flows SET sort_order=? WHERE name=?", (a_ord, b_name))
		await db.commit()


# ===================== CONTENT BLOCKS =====================

async def next_position(flow: str) -> int:
	async with aiosqlite.connect(DB_PATH) as db:
		cur = await db.execute("SELECT COALESCE(MAX(position), 0) FROM content_blocks WHERE flow=?", (flow,))
		mx = (await cur.fetchone())[0] or 0
		return int(mx) + 1


async def get_blocks(flow: str) -> List[Dict]:
	async with aiosqlite.connect(DB_PATH) as db:
		cur = await db.execute("""
			SELECT
				id, flow, position, type,
				title, text,
				circle_path, video_url, buttons_json,
				is_active, delay_seconds,
				file_path, file_kind, file_name
			FROM content_blocks
			WHERE flow=?
			ORDER BY position ASC
		""", (flow,))
		rows = await cur.fetchall()

	return [{
		"id": r[0],
		"flow": r[1],
		"position": r[2],
		"type": r[3],
		"title": r[4] or "",
		"text": r[5] or "",
		"circle": r[6] or "",
		"video": r[7] or "",
		"buttons": r[8] or "",
		"is_active": int(r[9] or 0),
		"delay": float(r[10] or 1.0),
		"file_path": r[11] or "",
		"file_kind": r[12] or "",
		"file_name": r[13] or "",
	} for r in rows]


async def get_block(block_id: int) -> Optional[Dict]:
	async with aiosqlite.connect(DB_PATH) as db:
		cur = await db.execute("""
			SELECT
				id, flow, position, type,
				title, text,
				circle_path, video_url, buttons_json,
				is_active, delay_seconds,
				file_path, file_kind, file_name
			FROM content_blocks
			WHERE id=?
		""", (block_id,))
		r = await cur.fetchone()

	if not r:
		return None

	return {
		"id": r[0],
		"flow": r[1],
		"position": r[2],
		"type": r[3],
		"title": r[4] or "",
		"text": r[5] or "",
		"circle": r[6] or "",
		"video": r[7] or "",
		"buttons": r[8] or "",
		"is_active": int(r[9] or 0),
		"delay": float(r[10] or 1.0),
		"file_path": r[11] or "",
		"file_kind": r[12] or "",
		"file_name": r[13] or "",
	}


async def create_block(data: Dict) -> None:
	async with aiosqlite.connect(DB_PATH) as db:
		await db.execute("""
			INSERT INTO content_blocks
			(flow, position, type, title, text, circle_path, video_url, buttons_json,
			 is_active, delay_seconds, file_path, file_kind, file_name)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		""", (
			data["flow"], data["position"], data["type"],
			data.get("title", ""), data.get("text", ""),
			data.get("circle", ""), data.get("video", ""),
			data.get("buttons", ""), data.get("is_active", 1),
			data.get("delay", 1.0),
			data.get("file_path", ""),
			data.get("file_kind", ""),
			data.get("file_name", ""),
		))
		await db.commit()


async def update_block(block_id: int, data: Dict) -> None:
	async with aiosqlite.connect(DB_PATH) as db:
		await db.execute("""
			UPDATE content_blocks
			SET flow=?, position=?, type=?,
				title=?, text=?, circle_path=?, video_url=?, buttons_json=?,
				is_active=?, delay_seconds=?,
				file_path=?, file_kind=?, file_name=?
			WHERE id=?
		""", (
			data["flow"], data["position"], data["type"],
			data.get("title", ""), data.get("text", ""),
			data.get("circle", ""), data.get("video", ""),
			data.get("buttons", ""), data.get("is_active", 1),
			data.get("delay", 1.0),
			data.get("file_path", ""),
			data.get("file_kind", ""),
			data.get("file_name", ""),
			int(block_id)
		))
		await db.commit()


async def delete_block(block_id: int) -> None:
	async with aiosqlite.connect(DB_PATH) as db:
		await db.execute("DELETE FROM content_blocks WHERE id=?", (int(block_id),))
		await db.commit()


async def swap_positions(id_a: int, id_b: int) -> None:
	async with aiosqlite.connect(DB_PATH) as db:
		cur = await db.execute("SELECT position FROM content_blocks WHERE id=?", (id_a,))
		a_pos = (await cur.fetchone())[0]
		cur = await db.execute("SELECT position FROM content_blocks WHERE id=?", (id_b,))
		b_pos = (await cur.fetchone())[0]

		await db.execute("UPDATE content_blocks SET position=? WHERE id=?", (b_pos, id_a))
		await db.execute("UPDATE content_blocks SET position=? WHERE id=?", (a_pos, id_b))
		await db.commit()