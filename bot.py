# bot.py
import os
import time
import asyncio
import json
from typing import Optional, Dict, Any

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
	Message, BotCommand,
	ReplyKeyboardMarkup, KeyboardButton,
	InlineKeyboardMarkup, InlineKeyboardButton,
	CallbackQuery,
	FSInputFile,
	URLInputFile,
)

from db import (
	init_db, get_blocks, get_block,
	inc_start, inc_message,
	upsert_job, fetch_due_jobs, mark_job_done,
	get_flow_triggers,

	# flow modes
	get_flow_modes,

	# flow actions
	get_flow_actions,

	# gate pressed + cancel reminder job
	mark_gate_pressed,
	is_gate_pressed,
	mark_job_done_by_user_flow,

	# for broadcasts (all users)
	get_users,

	# ✅ нужно для user-state (разблокировка уроков)
	get_pool,
)

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
	raise RuntimeError("BOT_TOKEN is not set")

CRM_BASE_URL = (os.getenv("CRM_BASE_URL") or "").strip().rstrip("/")

SUPPORT_USERNAME = "@TataZakzheva"

WEB_URL = "https://www.happi10.com"
CLUB_URL = "https://www.happi10.com/programs"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ✅ какой flow считать "конец курса" (после него появятся Уроки)
_COURSE_COMPLETE_FLOW = (os.getenv("COURSE_COMPLETE_FLOW") or "day3").strip()

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

_jobs_task: asyncio.Task | None = None

# кеш режимов флоу
_FLOW_MODES: dict[str, str] = {}

# per-user lock, чтобы не было параллельного render_flow на одного юзера
_USER_LOCKS: dict[int, asyncio.Lock] = {}

# защита от дублей jobs пока задача в процессе
_RUNNING_JOBS: set[int] = set()

# ограничим общий параллелизм джобов, чтобы не убить бот/DB
_JOB_SEM = asyncio.Semaphore(int(os.getenv("JOBS_CONCURRENCY", "25")))

# как часто подтягивать режимы из БД (если в CRM переключили mode без рестарта бота)
_FLOW_MODES_REFRESH_SECONDS = int(os.getenv("FLOW_MODES_REFRESH_SECONDS", "20"))


def _lock(uid: int) -> asyncio.Lock:
	uid = int(uid)
	if uid not in _USER_LOCKS:
		_USER_LOCKS[uid] = asyncio.Lock()
	return _USER_LOCKS[uid]


def _mode(flow: str) -> str:
	"""off/manual/auto (default off)"""
	return (_FLOW_MODES.get((flow or "").strip()) or "off").strip().lower()


async def refresh_flow_modes() -> None:
	global _FLOW_MODES
	try:
		_FLOW_MODES = await get_flow_modes()
	except Exception:
		_FLOW_MODES = {}


# ─────────────────────────────────────────────────────────────
# ✅ delay parsing (фикс "дефолт 1.0" и странные задержки)

def _parse_delay_seconds(raw: Any) -> float:
	"""
	- если delay пустой/None/"" -> 0 (а не 1.0)
	- если delay < 0 -> 0
	- если delay не число -> 0
	"""
	if raw is None:
		return 0.0
	if isinstance(raw, str) and raw.strip() == "":
		return 0.0
	try:
		v = float(raw)
		return v if v > 0 else 0.0
	except Exception:
		return 0.0


# ─────────────────────────────────────────────────────────────
# ✅ user state (разблокировка "Уроки" после окончания курса)

async def _get_user_state(user_id: int) -> Dict[str, Any]:
	pool = await get_pool()
	async with pool.acquire() as conn:
		row = await conn.fetchrow("SELECT state FROM users WHERE user_id=$1;", int(user_id))
		if not row:
			await conn.execute(
				"INSERT INTO users(user_id, state, flow_status, last_start_at, updated_at) "
				"VALUES ($1,'{}','','','') ON CONFLICT (user_id) DO NOTHING;",
				int(user_id),
			)
			return {}
		raw = (row["state"] or "").strip()
		if not raw:
			return {}
		try:
			v = json.loads(raw)
			return v if isinstance(v, dict) else {}
		except Exception:
			return {}


async def _set_user_state(user_id: int, state: Dict[str, Any]) -> None:
	pool = await get_pool()
	js = json.dumps(state or {}, ensure_ascii=False)
	now = str(int(time.time()))
	async with pool.acquire() as conn:
		await conn.execute(
			"""
			INSERT INTO users(user_id, state, flow_status, last_start_at, updated_at)
			VALUES ($1,$2,'','','')
			ON CONFLICT (user_id) DO UPDATE SET
				state=EXCLUDED.state,
				updated_at=$3;
			""",
			int(user_id), js, now
		)


async def is_lessons_unlocked(user_id: int) -> bool:
	st = await _get_user_state(user_id)
	return bool(st.get("lessons_unlocked"))


async def unlock_lessons(user_id: int) -> None:
	st = await _get_user_state(user_id)
	if st.get("lessons_unlocked"):
		return
	st["lessons_unlocked"] = True
	await _set_user_state(user_id, st)


# ─────────────────────────────────────────────────────────────
# UI (✅ меню на русском + "Уроки" только после конца курса)

def reply_main_menu(lessons_unlocked: bool) -> ReplyKeyboardMarkup:
	rows = [
		[KeyboardButton(text="❓ FAQ")],
		[KeyboardButton(text="🌐 Сайт"), KeyboardButton(text="🏛️ Клуб Архитектора Счастья")],
		[KeyboardButton(text="🆘 Поддержка")],
	]
	if lessons_unlocked:
		rows.insert(0, [KeyboardButton(text="📚 Уроки")])

	return ReplyKeyboardMarkup(
		keyboard=rows,
		resize_keyboard=True,
		is_persistent=True,
	)


def inline_web_button() -> InlineKeyboardMarkup:
	return InlineKeyboardMarkup(
		inline_keyboard=[[InlineKeyboardButton(text="🌐 Перейти на сайт", url=WEB_URL)]]
	)


def inline_club_button() -> InlineKeyboardMarkup:
	return InlineKeyboardMarkup(
		inline_keyboard=[[InlineKeyboardButton(text="🏛️ Клуб Архитектора Счастья", url=CLUB_URL)]]
	)


def inline_lessons_menu() -> InlineKeyboardMarkup:
	return InlineKeyboardMarkup(
		inline_keyboard=[
			[InlineKeyboardButton(text="🔵 День 1", callback_data="lesson:day1")],
			[InlineKeyboardButton(text="🔵 День 2", callback_data="lesson:day2")],
			[InlineKeyboardButton(text="🔵 День 3", callback_data="lesson:day3")],
		]
	)


def build_buttons_kb(buttons_json: Optional[str]) -> Optional[InlineKeyboardMarkup]:
	s = (buttons_json or "").strip()
	if not s:
		return None

	try:
		btns = json.loads(s)
		if not isinstance(btns, list):
			return None

		rows = []
		for b in btns:
			if not isinstance(b, dict):
				continue
			text = (b.get("text") or "").strip()
			url = (b.get("url") or "").strip()
			if not text or not url:
				continue
			rows.append([InlineKeyboardButton(text=text, url=url)])

		return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None
	except Exception:
		return None


# ─────────────────────────────────────────────────────────────
# Files helpers

def _guess_kind_from_ext(path: str) -> str:
	ext = (os.path.splitext(path)[1] or "").lower()
	if ext in [".jpg", ".jpeg", ".png", ".webp"]:
		return "photo"
	if ext in [".mp4", ".mov", ".m4v", ".webm"]:
		return "video"
	if ext in [".mp3", ".wav", ".m4a", ".ogg"]:
		return "audio"
	return "document"


def _safe_filename(name: str) -> str:
	n = (name or "").strip()
	if not n:
		return ""
	n = os.path.basename(n)
	n = n.replace("\x00", "").replace("\n", " ").replace("\r", " ").strip()
	return n


def _resolve_local_path(file_path: str) -> str:
	p = (file_path or "").strip()
	if not p:
		return ""

	if os.path.isabs(p):
		return p

	cand = os.path.join(BASE_DIR, p)
	if os.path.exists(cand):
		return cand

	cand2 = os.path.join(BASE_DIR, "media", os.path.basename(p))
	if os.path.exists(cand2):
		return cand2

	return ""


def _to_public_url(p: str) -> str:
	p = (p or "").strip()
	if not p:
		return ""

	if p.startswith("http://") or p.startswith("https://"):
		return p

	if p.startswith("media/"):
		p = "/" + p

	if p.startswith("/media/"):
		if not CRM_BASE_URL:
			return ""
		return f"{CRM_BASE_URL}{p}"

	return ""


def _normalize_kind(kind: str, file_path: str) -> str:
	k = (kind or "").strip().lower()

	if k in ("image", "img", "photo", "picture"):
		return "photo"
	if k in ("file", "doc", "pdf"):
		return "document"
	if k in ("video", "audio", "document", "photo"):
		return k

	return _guess_kind_from_ext(file_path)


def _ensure_filename_with_ext(file_name: str, file_path: str) -> str:
	fn = _safe_filename(file_name)
	if not fn:
		fn = os.path.basename((file_path or "").strip()) or "file"

	if "." not in fn:
		ext = os.path.splitext(file_path)[1]
		if ext:
			fn = fn + ext

	return fn


async def send_attachment(
	chat_id: int,
	file_path: str,
	file_kind: str = "",
	file_name: str = "",
) -> None:
	if not file_path:
		return

	kind = _normalize_kind(file_kind, file_path)
	fn = _ensure_filename_with_ext(file_name, file_path)

	# 1) URL
	url = _to_public_url(file_path)
	if url:
		try:
			input_file = URLInputFile(url, filename=fn)
			if kind == "photo":
				await bot.send_photo(chat_id, photo=input_file)
			elif kind == "video":
				await bot.send_video(chat_id, video=input_file)
			elif kind == "audio":
				await bot.send_audio(chat_id, audio=input_file)
			else:
				await bot.send_document(chat_id, document=input_file)
			return
		except Exception:
			pass

	# 2) local
	abs_path = _resolve_local_path(file_path)
	if not abs_path:
		await bot.send_message(chat_id, f"⚠️ Файл не найден: <code>{file_path}</code>")
		return

	kind = kind or _guess_kind_from_ext(abs_path)
	f = FSInputFile(abs_path, filename=fn)
	try:
		if kind == "photo":
			await bot.send_photo(chat_id, photo=f)
		elif kind == "video":
			await bot.send_video(chat_id, video=f)
		elif kind == "audio":
			await bot.send_audio(chat_id, audio=f)
		else:
			await bot.send_document(chat_id, document=f)
	except Exception:
		await bot.send_message(chat_id, f"⚠️ Не удалось отправить файл: <code>{file_path}</code>")


async def send_circle(chat_id: int, circle_path: str) -> None:
	p = (circle_path or "").strip()
	if not p:
		return

	url = _to_public_url(p)
	if url:
		try:
			await bot.send_video_note(chat_id, video_note=URLInputFile(url, filename="circle.mp4"))
			return
		except Exception:
			pass

	abs_path = _resolve_local_path(p)
	if not abs_path:
		await bot.send_message(chat_id, f"⚠️ Файл не найден: <code>{p}</code>")
		return

	try:
		await bot.send_video_note(chat_id, video_note=FSInputFile(abs_path, filename="circle.mp4"))
	except Exception:
		await bot.send_message(chat_id, f"⚠️ Не удалось отправить кружок: <code>{p}</code>")


# ─────────────────────────────────────────────────────────────
# Job keys

def _job_flow(flow: str) -> str:
	return f"flow:{(flow or '').strip()}"


def _job_gate(block_id: int, next_flow: str) -> str:
	return f"gate:{int(block_id)}:{(next_flow or '').strip()}"


def _job_action(action_id: int) -> str:
	return f"action:{int(action_id)}"


# ─────────────────────────────────────────────────────────────
# GATE

def _gate_cb(user_id: int, block_id: int, next_flow: str) -> str:
	return f"gate:{user_id}:{block_id}:{next_flow}"


async def _schedule_gate_reminder(user_id: int, block_id: int, next_flow: str, seconds: int) -> None:
	seconds = int(seconds or 0)
	if seconds <= 0:
		return
	run_at = int(time.time()) + seconds
	await upsert_job(int(user_id), _job_gate(block_id, next_flow), run_at)


# ─────────────────────────────────────────────────────────────
# After-flow actions runner (ставим jobs, не запускаем render_flow напрямую)

async def _schedule_after_flow_actions(user_id: int, after_flow: str) -> None:
	try:
		actions = await get_flow_actions(after_flow)
	except Exception:
		return

	if not actions:
		return

	now = int(time.time())
	for a in actions:
		try:
			if int(a.get("is_active", 0) or 0) != 1:
				continue
			if (a.get("action_type") or "start_flow") != "start_flow":
				continue

			target = (a.get("target_flow") or "").strip()
			if not target:
				continue

			delay = int(a.get("delay_seconds", 0) or 0)
			if delay < 0:
				delay = 0

			action_id = int(a.get("id") or 0)
			key = _job_action(action_id) if action_id > 0 else _job_flow(target)
			await upsert_job(int(user_id), key, now + delay)
		except Exception:
			continue


# ─────────────────────────────────────────────────────────────
# Flow rendering (serialized per user)

async def render_flow(chat_id: int, flow: str):
	flow = (flow or "").strip()
	if not flow:
		return

	async with _lock(chat_id):
		blocks = await get_blocks(flow)

		# ✅ меню приклеиваем один раз (как было), но с учётом unlocked
		menu_attached = False

		for block in blocks:
			if not block.get("is_active"):
				continue

			t = (block.get("type") or "").strip()
			# ✅ FIX delay
			delay = _parse_delay_seconds(block.get("delay", None))
			kb = build_buttons_kb(block.get("buttons"))

			# reply keyboard прикрепляем только один раз на тексте welcome
			attach_reply_menu = False
			if (flow == "welcome") and (not menu_attached) and (t in ("text", "", None)) and bool((block.get("text") or "").strip()):
				attach_reply_menu = True

			# 1) content
			if t == "circle" and block.get("circle"):
				await send_circle(chat_id, block.get("circle", ""))

			elif t == "video" and block.get("video"):
				title = (block.get("title") or "").strip() or "<b>Видео урок:</b>"
				await bot.send_message(
					chat_id,
					title,
					reply_markup=InlineKeyboardMarkup(
						inline_keyboard=[[InlineKeyboardButton(text="▶️ Смотреть видео", url=block["video"])]]
					)
				)
				if kb:
					await bot.send_message(chat_id, " ", reply_markup=kb)

			elif t == "buttons":
				title = (block.get("title") or "").strip()
				text = (block.get("text") or "").strip()
				msg = title or text or " "
				if kb:
					await bot.send_message(chat_id, msg, reply_markup=kb)
				else:
					if block.get("buttons"):
						await bot.send_message(chat_id, "⚠️ buttons_json битый (невалидный JSON).")
					else:
						await bot.send_message(chat_id, msg)

			elif t == "text" and block.get("text"):
				if attach_reply_menu:
					unlocked = await is_lessons_unlocked(chat_id)
					await bot.send_message(chat_id, block["text"], reply_markup=reply_main_menu(unlocked))
					menu_attached = True
				else:
					await bot.send_message(chat_id, block["text"], reply_markup=kb)

			else:
				if block.get("text"):
					if attach_reply_menu:
						unlocked = await is_lessons_unlocked(chat_id)
						await bot.send_message(chat_id, block["text"], reply_markup=reply_main_menu(unlocked))
						menu_attached = True
					else:
						await bot.send_message(chat_id, block["text"], reply_markup=kb)

			# 2) attachment
			file_path = (block.get("file_path") or "").strip()
			file_kind = (block.get("file_kind") or "").strip()
			file_name = (block.get("file_name") or "").strip()
			if file_path:
				await send_attachment(chat_id, file_path, file_kind, file_name)

			# 3) GATE
			next_flow = (block.get("gate_next_flow") or "").strip()
			if next_flow:
				if delay > 0:
					await asyncio.sleep(delay)

				btn_text = (block.get("gate_button_text") or "").strip() or "Дальше"
				prompt_text = (block.get("gate_prompt_text") or "").strip() or " "
				rem_sec = int(block.get("gate_reminder_seconds") or 0)
				block_id = int(block.get("id") or 0)

				if rem_sec > 0 and block_id > 0:
					await _schedule_gate_reminder(chat_id, block_id, next_flow, rem_sec)

				await bot.send_message(
					chat_id,
					prompt_text,
					reply_markup=InlineKeyboardMarkup(
						inline_keyboard=[[
							InlineKeyboardButton(
								text=btn_text,
								callback_data=_gate_cb(chat_id, block_id, next_flow)
							)
						]]
					)
				)
				return

			# 4) delay for non-gate blocks
			if delay > 0:
				await asyncio.sleep(delay)

		# ✅ конец курса -> разблокируем уроки (только это добавили)
		if flow == _COURSE_COMPLETE_FLOW:
			await unlock_lessons(chat_id)

		await _schedule_after_flow_actions(chat_id, flow)


# ─────────────────────────────────────────────────────────────
# Scheduling from CRM (flow_triggers) only if mode == auto

async def schedule_from_flow_triggers(user_id: int) -> bool:
	try:
		triggers = await get_flow_triggers()
	except Exception:
		return False

	now = int(time.time())
	any_set = False

	for tr in triggers:
		try:
			flow = (tr.get("flow") or "").strip()
			is_active = int(tr.get("is_active") or 0)
			offset_seconds = int(tr.get("offset_seconds") or 0)

			if not flow or is_active != 1:
				continue
			if offset_seconds < 0:
				continue

			if _mode(flow) != "auto":
				continue

			await upsert_job(int(user_id), _job_flow(flow), now + offset_seconds)
			any_set = True
		except Exception:
			continue

	return any_set


# ─────────────────────────────────────────────────────────────
# Broadcast support via jobs key

async def _run_broadcast_job(current_uid: int, job_key: str) -> None:
	parts = job_key.split(":")
	flow = ""
	audience = ""
	repeat = 0

	if len(parts) >= 2:
		flow = (parts[1] or "").strip()

	if len(parts) >= 3:
		audience = (parts[2] or "").strip().lower()

	if len(parts) >= 4:
		try:
			repeat = int(parts[3] or 0)
		except Exception:
			repeat = 0

	if not flow:
		return

	if audience == "all":
		try:
			users = await get_users(50000)
		except Exception:
			users = []
		for u in users:
			try:
				uid = int(u.get("user_id") or 0)
			except Exception:
				uid = 0
			if uid > 0:
				asyncio.create_task(render_flow(uid, flow))
	elif audience.isdigit():
		uid = int(audience)
		if uid > 0:
			await render_flow(uid, flow)
	else:
		await render_flow(current_uid, flow)

	if repeat > 0:
		now = int(time.time())
		await upsert_job(int(current_uid), job_key, now + repeat)


# ─────────────────────────────────────────────────────────────
# Jobs worker (НЕ блокируем очередь ожиданием render_flow)

async def _execute_job_and_mark_done(jid: int, uid: int, job_key: str) -> None:
	async with _JOB_SEM:
		try:
			if job_key.startswith("flow:"):
				flow = job_key.split(":", 1)[1].strip()
				if flow and _mode(flow) == "auto":
					await render_flow(uid, flow)

			elif job_key.startswith("action:"):
				aid_s = job_key.split(":", 1)[1].strip()
				try:
					aid = int(aid_s)
				except Exception:
					aid = 0

				if aid > 0:
					try:
						actions = await get_flow_actions(None)
					except Exception:
						actions = []

					target = ""
					for a in actions or []:
						if int(a.get("id") or 0) == aid and int(a.get("is_active") or 0) == 1:
							target = (a.get("target_flow") or "").strip()
							break

					if target:
						await render_flow(uid, target)

			elif job_key.startswith("gate:"):
				parts = job_key.split(":", 2)
				if len(parts) == 3:
					block_id = int(parts[1])
					next_flow = parts[2].strip()

					if block_id > 0 and await is_gate_pressed(uid, block_id):
						pass
					else:
						btn_text = "Дальше"
						text = " "
						try:
							b = await get_block(block_id)
							if b:
								custom = (b.get("gate_reminder_text") or "").strip()
								if custom:
									text = custom
								bt = (b.get("gate_button_text") or "").strip()
								if bt:
									btn_text = bt
						except Exception:
							pass

						await bot.send_message(
							uid,
							text,
							reply_markup=InlineKeyboardMarkup(
								inline_keyboard=[[
									InlineKeyboardButton(
										text=btn_text,
										callback_data=_gate_cb(uid, block_id, next_flow)
									)
								]]
							)
						)

			elif job_key.startswith("broadcast:"):
				await _run_broadcast_job(uid, job_key)

		finally:
			try:
				await mark_job_done(jid)
			finally:
				_RUNNING_JOBS.discard(int(jid))


async def jobs_loop():
	last_modes_refresh = 0

	try:
		while True:
			try:
				now = int(time.time())

				if now - last_modes_refresh >= _FLOW_MODES_REFRESH_SECONDS:
					last_modes_refresh = now
					await refresh_flow_modes()

				due = await fetch_due_jobs(50)

				for job in due:
					jid = int(job["id"])
					if jid in _RUNNING_JOBS:
						continue
					_RUNNING_JOBS.add(jid)

					uid = int(job["user_id"])
					job_key = (job.get("flow") or "").strip()

					asyncio.create_task(_execute_job_and_mark_done(jid, uid, job_key))

			except Exception:
				pass

			await asyncio.sleep(1)

	except asyncio.CancelledError:
		return


# ─────────────────────────────────────────────────────────────
# Handlers

@dp.message(CommandStart())
async def cmd_start(message: Message):
	uid = message.from_user.id
	username = message.from_user.username or ""

	await inc_start(uid, username)

	await refresh_flow_modes()
	await schedule_from_flow_triggers(uid)
	return


@dp.message(Command("menu"))
async def cmd_menu(message: Message):
	await inc_message(message.from_user.id, message.from_user.username or "")
	unlocked = await is_lessons_unlocked(message.from_user.id)
	await message.answer(" ", reply_markup=reply_main_menu(unlocked))


# ✅ команда lessons “в списке” не показывается до конца курса (мы не регистрируем её в set_my_commands),
# но обработчик остаётся: если юзер введёт вручную — покажем замок.
@dp.message(Command("lessons"))
async def cmd_lessons(message: Message):
	await inc_message(message.from_user.id, message.from_user.username or "")
	if not await is_lessons_unlocked(message.from_user.id):
		await message.answer("🔒 Уроки откроются после полного прохождения курса.")
		return
	await message.answer("📚 <b>Уроки</b>\nВыбери день:", reply_markup=inline_lessons_menu())


@dp.message(Command("faq"))
async def cmd_faq(message: Message):
	await inc_message(message.from_user.id, message.from_user.username or "")
	await message.answer(
		"❓ <b>FAQ</b>\n\n"
		"• Курс состоит из 3 уроков\n"
		"• Видео внутри уроков\n"
		f"• Поддержка: {SUPPORT_USERNAME}"
	)


@dp.message(Command("web"))
async def cmd_web(message: Message):
	await inc_message(message.from_user.id, message.from_user.username or "")
	await message.answer("🌐 <b>Наш сайт</b>", reply_markup=inline_web_button())


@dp.message(Command("club"))
async def cmd_club(message: Message):
	await inc_message(message.from_user.id, message.from_user.username or "")
	await message.answer("🏛️ <b>Клуб Архитектора Счастья</b>", reply_markup=inline_club_button())


@dp.message(Command("support"))
async def cmd_support(message: Message):
	await inc_message(message.from_user.id, message.from_user.username or "")
	await message.answer(f"🆘 Поддержка: {SUPPORT_USERNAME}")


@dp.message(F.text == "📚 Уроки")
async def btn_lessons(message: Message):
	await inc_message(message.from_user.id, message.from_user.username or "")
	await cmd_lessons(message)


@dp.message(F.text == "❓ FAQ")
async def btn_faq(message: Message):
	await inc_message(message.from_user.id, message.from_user.username or "")
	await cmd_faq(message)


@dp.message(F.text == "🌐 Сайт")
async def btn_web(message: Message):
	await inc_message(message.from_user.id, message.from_user.username or "")
	await cmd_web(message)


@dp.message(F.text == "🏛️ Клуб Архитектора Счастья")
async def btn_club(message: Message):
	await inc_message(message.from_user.id, message.from_user.username or "")
	await cmd_club(message)


@dp.message(F.text == "🆘 Поддержка")
async def btn_support(message: Message):
	await inc_message(message.from_user.id, message.from_user.username or "")
	await cmd_support(message)


@dp.callback_query(F.data.startswith("lesson:"))
async def cb_lesson(call: CallbackQuery):
	await call.answer()
	await inc_message(call.from_user.id, call.from_user.username or "")

	# ✅ уроки доступны только после конца курса
	if not await is_lessons_unlocked(call.from_user.id):
		await call.message.answer("🔒 Уроки откроются после полного прохождения курса.")
		return

	flow = call.data.split(":", 1)[1].strip()
	await render_flow(call.from_user.id, flow)


@dp.callback_query(F.data.startswith("gate:"))
async def cb_gate_next(call: CallbackQuery):
	try:
		_, uid_s, block_id_s, next_flow = call.data.split(":", 3)
		target_uid = int(uid_s)
		block_id = int(block_id_s)
		next_flow = (next_flow or "").strip()
	except Exception:
		await call.answer("Ошибка кнопки", show_alert=True)
		return

	if call.from_user.id != target_uid:
		await call.answer("Это не для тебя", show_alert=True)
		return

	if block_id > 0:
		try:
			await mark_gate_pressed(target_uid, block_id)
		except Exception:
			pass

	try:
		await mark_job_done_by_user_flow(target_uid, _job_gate(block_id, next_flow))
	except Exception:
		pass

	await call.answer()
	await render_flow(target_uid, next_flow)


@dp.message()
async def any_message(message: Message):
	if message.text and message.text.startswith("/"):
		return
	await inc_message(message.from_user.id, message.from_user.username or "")


# ─────────────────────────────────────────────────────────────

async def on_startup():
	global _jobs_task

	await init_db()
	await refresh_flow_modes()

	# ✅ /lessons НЕ добавляем в список команд (чтобы “не появлялась” до конца курса)
	await bot.set_my_commands([
		BotCommand(command="start", description="Начать курс"),
		BotCommand(command="menu", description="Меню"),
		BotCommand(command="faq", description="FAQ"),
		BotCommand(command="web", description="Сайт"),
		BotCommand(command="club", description="Клуб Архитектора Счастья"),
		BotCommand(command="support", description="Поддержка"),
	])

	if _jobs_task is None or _jobs_task.done():
		_jobs_task = asyncio.create_task(jobs_loop())


async def main():
	await on_startup()
	await dp.start_polling(bot)


if __name__ == "__main__":
	asyncio.run(main())