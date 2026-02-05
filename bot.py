# bot.py
import os
import time
import asyncio
import json
from typing import Optional

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
	Message, BotCommand,
	ReplyKeyboardMarkup, KeyboardButton,
	InlineKeyboardMarkup, InlineKeyboardButton,
	CallbackQuery, FSInputFile
)

from db import (
	init_db, get_blocks,
	inc_start, inc_message,
	upsert_job, fetch_due_jobs, mark_job_done,
	get_flow_triggers,
)

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
	raise RuntimeError("BOT_TOKEN is not set")
SUPPORT_USERNAME = "@client_support"
WEB_URL = "https://www.happi10.com"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

_jobs_task: asyncio.Task | None = None


# ─────────────────────────────────────────────────────────────
# UI

def reply_main_menu() -> ReplyKeyboardMarkup:
	return ReplyKeyboardMarkup(
		keyboard=[
			[KeyboardButton(text="📚 Lessons"), KeyboardButton(text="❓ FAQ")],
			[KeyboardButton(text="🌐 Web"), KeyboardButton(text="🆘 Support")],
		],
		resize_keyboard=True,
	)


def inline_web_button() -> InlineKeyboardMarkup:
	return InlineKeyboardMarkup(
		inline_keyboard=[[InlineKeyboardButton(text="🌐 Перейти на сайт", url=WEB_URL)]]
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
# Files

def _guess_kind_from_ext(path: str) -> str:
	ext = (os.path.splitext(path)[1] or "").lower()
	if ext in [".jpg", ".jpeg", ".png", ".webp"]:
		return "photo"
	if ext in [".mp4", ".mov", ".m4v", ".webm"]:
		return "video"
	if ext in [".mp3", ".wav", ".m4a", ".ogg"]:
		return "audio"
	return "document"


def _resolve_path(file_path: str) -> str:
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

	return cand


def _safe_filename(name: str) -> str:
	"""
	Telegram берёт имя файла из multipart filename.
	Дадим нормальное имя (без путей), иначе будут uuid-цифры.
	"""
	n = (name or "").strip()
	if not n:
		return ""
	n = os.path.basename(n)
	# очень грубо: убираем null/переводы строк
	n = n.replace("\x00", "").replace("\n", " ").replace("\r", " ").strip()
	return n


async def send_attachment(
	chat_id: int,
	file_path: str,
	file_kind: str = "",
	file_name: str = "",
) -> None:
	if not file_path:
		return

	abs_path = _resolve_path(file_path)
	if not abs_path or not os.path.exists(abs_path):
		await bot.send_message(chat_id, f"⚠️ Файл не найден: <code>{file_path}</code>")
		return

	# kind from CRM
	kind = (file_kind or "").strip().lower()
	if kind in ("image", "img", "photo", "picture"):
		kind = "photo"
	elif kind in ("file", "doc", "pdf"):
		kind = "document"
	kind = kind or _guess_kind_from_ext(abs_path)

	# ✅ задаём нормальное имя файла (иначе будет uuid...)
	fn = _safe_filename(file_name)
	if not fn:
		fn = os.path.basename(abs_path)

	f = FSInputFile(abs_path, filename=fn)

	try:
		if kind == "photo":
			await bot.send_photo(chat_id, photo=f)
		elif kind == "video":
			await bot.send_video(chat_id, video=f)
		elif kind == "audio":
			await bot.send_audio(chat_id, audio=f)
		else:
			# PDF preview у Telegram клиент/сервер-стороны: появится только если Telegram смог сделать thumb.
			# Мы гарантируем правильное расширение и имя файла.
			await bot.send_document(chat_id, document=f)
	except Exception:
		try:
			await bot.send_document(chat_id, document=f)
		except Exception:
			await bot.send_message(chat_id, f"⚠️ Не удалось отправить файл: <code>{file_path}</code>")


# ─────────────────────────────────────────────────────────────
# Flow rendering

async def render_flow(chat_id: int, flow: str):
	blocks = await get_blocks(flow)

	for block in blocks:
		if not block.get("is_active"):
			continue

		t = (block.get("type") or "").strip()
		delay = float(block.get("delay", 1.0) or 0)
		kb = build_buttons_kb(block.get("buttons"))

		# 1) content
		if t == "circle" and block.get("circle"):
			try:
				circle_path = _resolve_path(block["circle"])
				await bot.send_video_note(chat_id, video_note=FSInputFile(circle_path))
			except Exception:
				await bot.send_message(chat_id, f"⚠️ Не удалось отправить кружок: <code>{block.get('circle','')}</code>")

		elif t == "video" and block.get("video"):
			title = (block.get("title") or "").strip() or "🎬 <b>Видео урок:</b>"
			await bot.send_message(
				chat_id,
				title,
				reply_markup=InlineKeyboardMarkup(
					inline_keyboard=[[InlineKeyboardButton(text="▶️ Смотреть видео", url=block["video"])]]
				)
			)
			if kb:
				await bot.send_message(chat_id, "⬇️", reply_markup=kb)

		elif t == "buttons":
			title = (block.get("title") or "").strip()
			text = (block.get("text") or "").strip()
			msg = title or text or "Выбери:"
			if kb:
				await bot.send_message(chat_id, msg, reply_markup=kb)
			else:
				if block.get("buttons"):
					await bot.send_message(chat_id, "⚠️ buttons_json битый (невалидный JSON).")
				else:
					await bot.send_message(chat_id, msg)

		elif t == "text" and block.get("text"):
			await bot.send_message(chat_id, block["text"], reply_markup=kb)

		else:
			if block.get("text"):
				await bot.send_message(chat_id, block["text"], reply_markup=kb)

		# 2) attachment (+ file_name)
		file_path = (block.get("file_path") or "").strip()
		file_kind = (block.get("file_kind") or "").strip()
		file_name = (block.get("file_name") or "").strip()  # ✅ новое поле
		if file_path:
			await send_attachment(chat_id, file_path, file_kind, file_name)

		# 3) delay
		if delay > 0:
			await asyncio.sleep(delay)


# ─────────────────────────────────────────────────────────────
# Scheduling from CRM (flow_triggers)

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

			await upsert_job(user_id, flow, now + offset_seconds)
			any_set = True
		except Exception:
			continue

	return any_set


async def schedule_fallback_day2_day3(user_id: int) -> None:
	now = int(time.time())
	await upsert_job(user_id, "day2", now + 24 * 3600)
	await upsert_job(user_id, "day3", now + 48 * 3600)


# ─────────────────────────────────────────────────────────────
# Jobs worker

async def jobs_loop():
	try:
		while True:
			try:
				due = await fetch_due_jobs(50)
				for job in due:
					jid = job["id"]
					uid = job["user_id"]
					flow = job["flow"]

					try:
						await render_flow(uid, flow)
					finally:
						await mark_job_done(jid)

			except Exception:
				pass

			await asyncio.sleep(20)

	except asyncio.CancelledError:
		return


# ─────────────────────────────────────────────────────────────
# Handlers

@dp.message(CommandStart())
async def cmd_start(message: Message):
	uid = message.from_user.id
	username = message.from_user.username or ""

	await inc_start(uid, username)

	ok = await schedule_from_flow_triggers(uid)
	if not ok:
		await schedule_fallback_day2_day3(uid)

	await render_flow(uid, "welcome")
	await message.answer("👇", reply_markup=reply_main_menu())
	await render_flow(uid, "day1")


@dp.message(Command("menu"))
async def cmd_menu(message: Message):
	await inc_message(message.from_user.id, message.from_user.username or "")
	await message.answer("Меню 👇", reply_markup=reply_main_menu())


@dp.message(Command("lessons"))
async def cmd_lessons(message: Message):
	await inc_message(message.from_user.id, message.from_user.username or "")
	await message.answer("📚 <b>Уроки</b>\nВыбери день:", reply_markup=inline_lessons_menu())


@dp.message(Command("faq"))
async def cmd_faq(message: Message):
	await inc_message(message.from_user.id, message.from_user.username or "")
	await message.answer(
		"❓ <b>FAQ</b>\n\n"
		"• Курс длится 3 дня\n"
		"• Видео внутри уроков\n"
		f"• Поддержка: {SUPPORT_USERNAME}"
	)


@dp.message(Command("web"))
async def cmd_web(message: Message):
	await inc_message(message.from_user.id, message.from_user.username or "")
	await message.answer("🌐 <b>Наш сайт</b>", reply_markup=inline_web_button())


@dp.message(Command("support"))
async def cmd_support(message: Message):
	await inc_message(message.from_user.id, message.from_user.username or "")
	await message.answer(f"🆘 Поддержка: {SUPPORT_USERNAME}")


@dp.message(F.text == "📚 Lessons")
async def btn_lessons(message: Message):
	await inc_message(message.from_user.id, message.from_user.username or "")
	await cmd_lessons(message)


@dp.message(F.text == "❓ FAQ")
async def btn_faq(message: Message):
	await inc_message(message.from_user.id, message.from_user.username or "")
	await cmd_faq(message)


@dp.message(F.text == "🌐 Web")
async def btn_web(message: Message):
	await inc_message(message.from_user.id, message.from_user.username or "")
	await cmd_web(message)


@dp.message(F.text == "🆘 Support")
async def btn_support(message: Message):
	await inc_message(message.from_user.id, message.from_user.username or "")
	await cmd_support(message)


@dp.callback_query(F.data.startswith("lesson:"))
async def cb_lesson(call: CallbackQuery):
	await call.answer()
	await inc_message(call.from_user.id, call.from_user.username or "")
	flow = call.data.split(":", 1)[1]
	await render_flow(call.from_user.id, flow)


@dp.message()
async def any_message(message: Message):
	if message.text and message.text.startswith("/"):
		return
	await inc_message(message.from_user.id, message.from_user.username or "")


# ─────────────────────────────────────────────────────────────

async def on_startup():
	global _jobs_task

	await init_db()

	await bot.set_my_commands([
		BotCommand(command="start", description="Начать курс"),
		BotCommand(command="menu", description="Меню"),
		BotCommand(command="lessons", description="Уроки"),
		BotCommand(command="faq", description="FAQ"),
		BotCommand(command="web", description="Сайт"),
		BotCommand(command="support", description="Поддержка"),
	])

	if _jobs_task is None or _jobs_task.done():
		_jobs_task = asyncio.create_task(jobs_loop())


async def main():
	await on_startup()
	await dp.start_polling(bot)


if __name__ == "__main__":
	asyncio.run(main())