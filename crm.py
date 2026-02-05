# crm.py
import os
import json
import uuid
from typing import Optional
from io import BytesIO
from datetime import datetime

from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import RedirectResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from openpyxl import Workbook
from openpyxl.utils import get_column_letter

from db import (
	init_db,
	get_flows, create_flow, delete_flow, move_flow,
	get_blocks, get_block, create_block, update_block, delete_block,
	next_position, swap_positions,
	get_stats, get_users,
	get_flow_triggers, set_flow_trigger, delete_flow_trigger,

	# ✅ NEW: flow modes (off/manual/auto)
	get_flow_modes, set_flow_mode,
)

from seed import seed as run_seed  # ✅ автосид

app = FastAPI()
templates = Jinja2Templates(directory="templates")

os.makedirs("media", exist_ok=True)
app.mount("/media", StaticFiles(directory="media"), name="media")


@app.on_event("startup")
async def startup():
	await init_db()

	# ✅ автосид только если flows пустые (первый запуск на новой БД)
	try:
		flows = await get_flows()
		if not flows:
			await run_seed()
	except Exception:
		pass


# ─────────────────────────────────────────────────────────────
# Helpers

def _unit_to_seconds(unit: str) -> int:
	u = (unit or "").strip().lower()
	if u == "minutes":
		return 60
	if u == "hours":
		return 3600
	return 86400  # days default


def _seconds_to_value_unit(total_seconds: int, preferred_unit: str = "days") -> tuple[int, str]:
	s = int(total_seconds or 0)
	p = (preferred_unit or "days").strip().lower()
	if p not in ("minutes", "hours", "days"):
		p = "days"

	if s <= 0:
		return 0, p

	if s % 86400 == 0:
		return s // 86400, "days"
	if s % 3600 == 0:
		return s // 3600, "hours"
	if s % 60 == 0:
		return s // 60, "minutes"

	return max(1, s // 60), "minutes"


def _safe_filename(name: str) -> str:
	n = (name or "").strip()
	n = os.path.basename(n)
	n = n.replace("\x00", "").replace("\n", " ").replace("\r", " ").strip()
	return n


def _norm_mode(mode: str) -> str:
	m = (mode or "").strip().lower()
	if m not in ("off", "manual", "auto"):
		return "off"
	return m


# ─────────────────────────────────────────────────────────────
# INDEX (FLOWS + STATS + TRIGGERS + MODES)

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
	flows = await get_flows()
	stats = await get_stats()
	users = await get_users(200)

	# ✅ flow modes
	try:
		modes = await get_flow_modes()  # dict: flow -> mode
	except Exception:
		modes = {}

	# triggers (offset + enabled)
	raw = await get_flow_triggers()
	triggers_map = {}

	for f in flows:
		# default trigger view
		triggers_map[f] = {
			"flow": f,
			"enabled": False,
			"offset_value": 0,
			"offset_unit": "days",
			"offset_seconds": 0,
			"mode": _norm_mode(modes.get(f, "off")),
		}

	for t in raw:
		flow = (t.get("flow") or "").strip()
		if not flow:
			continue

		enabled = int(t.get("is_active", 0) or 0)
		offset_seconds = int(t.get("offset_seconds", 0) or 0)

		val, unit = _seconds_to_value_unit(offset_seconds, preferred_unit="days")

		if flow not in triggers_map:
			triggers_map[flow] = {
				"flow": flow,
				"enabled": bool(enabled),
				"offset_value": int(val),
				"offset_unit": unit,
				"offset_seconds": offset_seconds,
				"mode": _norm_mode(modes.get(flow, "off")),
			}
		else:
			triggers_map[flow].update({
				"enabled": bool(enabled),
				"offset_value": int(val),
				"offset_unit": unit,
				"offset_seconds": offset_seconds,
				"mode": _norm_mode(modes.get(flow, triggers_map[flow].get("mode", "off"))),
			})

	return templates.TemplateResponse(
		"index.html",
		{
			"request": request,
			"flows": flows,
			"stats": stats,
			"users": users,
			"triggers": triggers_map,  # теперь тут есть triggers[flow]["mode"]
		},
	)


# ─────────────────────────────────────────────────────────────
# FLOW MODE + TRIGGERS routes
#
# Новая логика:
# mode=off    -> ничего авто не отправляем
# mode=manual -> ничего авто не отправляем (запуск только кнопкой)
# mode=auto   -> авто отправка по offset
#
# Поэтому auto = is_active=1, off/manual = is_active=0

@app.post("/flow/{flow}/trigger")
async def flow_trigger_save(
	flow: str,
	mode: str = Form("off"),
	offset_value: int = Form(0),
	offset_unit: str = Form("days"),
):
	flow = (flow or "").strip()
	if not flow:
		return RedirectResponse("/", status_code=302)

	mode = _norm_mode(mode)
	await set_flow_mode(flow, mode)

	offset_value = int(offset_value or 0)
	if offset_value < 0:
		offset_value = 0

	unit = (offset_unit or "days").strip().lower()
	if unit not in ("minutes", "hours", "days"):
		unit = "days"

	seconds = offset_value * _unit_to_seconds(unit)

	is_active = 1 if mode == "auto" else 0

	await set_flow_trigger(
		flow=flow,
		offset_seconds=int(seconds),
		is_active=int(is_active),
		trigger="after_start",
	)
	return RedirectResponse("/", status_code=302)


@app.post("/flow/{flow}/trigger/delete")
async def flow_trigger_delete(flow: str):
	flow = (flow or "").strip()
	if flow:
		# удаляем trigger и ставим off, чтобы точно ничего не авто-слалось
		await delete_flow_trigger(flow)
		try:
			await set_flow_mode(flow, "off")
		except Exception:
			pass
	return RedirectResponse("/", status_code=302)


# ─────────────────────────────────────────────────────────────
# EXPORT (XLSX)

@app.get("/export/users.xlsx")
async def export_users_xlsx():
	users = await get_users(50000)

	wb = Workbook()
	ws = wb.active
	ws.title = "bot_users"

	headers = [
		"user_id",
		"username",
		"first_seen_ts",
		"last_seen_ts",
		"first_seen_utc",
		"last_seen_utc",
		"starts_count",
		"messages_count",
	]
	ws.append(headers)

	def ts_to_utc_str(ts: int) -> str:
		if not ts:
			return ""
		return datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")

	for u in users:
		ws.append([
			u.get("user_id"),
			u.get("username", ""),
			u.get("first_seen_ts"),
			u.get("last_seen_ts"),
			ts_to_utc_str(u.get("first_seen_ts")),
			ts_to_utc_str(u.get("last_seen_ts")),
			u.get("starts_count", 0),
			u.get("messages_count", 0),
		])

	for col_idx, h in enumerate(headers, start=1):
		ws.column_dimensions[get_column_letter(col_idx)].width = max(14, len(h) + 2)

	buf = BytesIO()
	wb.save(buf)
	buf.seek(0)

	filename = f"bot_users_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
	return StreamingResponse(
		buf,
		media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		headers={"Content-Disposition": f'attachment; filename="{filename}"'},
	)


# ─────────────────────────────────────────────────────────────
# FLOWS

@app.get("/flow/new")
async def flow_new_get():
	return RedirectResponse("/", status_code=302)


@app.post("/flow/new")
async def flow_new_post(name: str = Form("")):
	name = (name or "").strip()
	if not name:
		return RedirectResponse("/", status_code=302)

	await create_flow(name)
	# default mode = off (ничего не слать само)
	try:
		await set_flow_mode(name, "off")
	except Exception:
		pass
	return RedirectResponse("/", status_code=302)


@app.post("/flow/{flow}/delete")
async def flow_delete(flow: str):
	await delete_flow(flow)
	return RedirectResponse("/", status_code=302)


@app.post("/flow/{flow}/up")
async def flow_up(flow: str):
	await move_flow(flow, "up")
	return RedirectResponse("/", status_code=302)


@app.post("/flow/{flow}/down")
async def flow_down(flow: str):
	await move_flow(flow, "down")
	return RedirectResponse("/", status_code=302)


@app.get("/flow/{flow}", response_class=HTMLResponse)
async def flow_page(request: Request, flow: str):
	blocks = await get_blocks(flow)
	return templates.TemplateResponse(
		"flow.html",
		{"request": request, "flow": flow, "blocks": blocks},
	)


# ─────────────────────────────────────────────────────────────
# BLOCKS

@app.get("/block/new", response_class=HTMLResponse)
async def new_block_page(request: Request, flow: str):
	flow = (flow or "").strip()
	if not flow:
		return RedirectResponse("/", status_code=302)

	await create_flow(flow)
	pos = await next_position(flow)

	flows = await get_flows()  # ✅ для dropdown "Next flow"

	empty = {
		"id": 0,
		"flow": flow,
		"position": pos,
		"type": "text",
		"title": "",
		"text": "",
		"circle": "",
		"video": "",
		"buttons": "",
		"is_active": 1,
		"delay": 1.0,

		"file_path": "",
		"file_kind": "",
		"file_name": "",

		# UI buttons
		"btn1_text": "",
		"btn1_url": "",
		"btn2_text": "",
		"btn2_url": "",
		"btn3_text": "",
		"btn3_url": "",
		"buttons_json": "",

		# ✅ GATE defaults
		"gate_next_flow": "",
		"gate_button_text": "✅ Готов к следующему уроку",
		"gate_reminder_value": 0,
		"gate_reminder_unit": "hours",
		"gate_reminder_text": "",
	}

	return templates.TemplateResponse(
		"edit.html",
		{"request": request, "block": empty, "is_new": True, "flows": flows},
	)


@app.get("/block/{block_id}/edit", response_class=HTMLResponse)
async def edit_block_page(request: Request, block_id: int):
	block = await get_block(block_id)
	if not block:
		return RedirectResponse("/", status_code=302)

	flows = await get_flows()  # ✅ для dropdown "Next flow"

	# распарсим кнопки
	btns = []
	try:
		if block.get("buttons"):
			btns = json.loads(block["buttons"])
	except Exception:
		pass

	for i in range(3):
		block[f"btn{i+1}_text"] = ""
		block[f"btn{i+1}_url"] = ""

	for i, b in enumerate(btns[:3]):
		if isinstance(b, dict):
			block[f"btn{i+1}_text"] = b.get("text", "")
			block[f"btn{i+1}_url"] = b.get("url", "")

	block["buttons_json"] = block.get("buttons", "")

	# ✅ GATE: секунд -> value+unit для UI
	rem_sec = int(block.get("gate_reminder_seconds") or 0)
	val, unit = _seconds_to_value_unit(rem_sec, preferred_unit="hours")
	block["gate_reminder_value"] = int(val)
	block["gate_reminder_unit"] = unit

	# дефолты, если пусто
	if not (block.get("gate_button_text") or "").strip():
		block["gate_button_text"] = "✅ Готов к следующему уроку"

	return templates.TemplateResponse(
		"edit.html",
		{"request": request, "block": block, "is_new": False, "flows": flows},
	)


@app.post("/block/save")
async def save_block(
	request: Request,
	block_id: int = Form(0),
	flow: str = Form(...),
	position: int = Form(...),
	type: str = Form(...),

	title: str = Form(""),
	text: str = Form(""),
	circle_path: str = Form(""),
	video_url: str = Form(""),
	is_active: int = Form(1),
	delay_seconds: float = Form(1.0),

	file_path: str = Form(""),
	file_kind: str = Form(""),
	file_name: str = Form(""),

	# UI buttons
	btn1_text: str = Form(""),
	btn1_url: str = Form(""),
	btn2_text: str = Form(""),
	btn2_url: str = Form(""),
	btn3_text: str = Form(""),
	btn3_url: str = Form(""),
	buttons_json: str = Form(""),

	# uploads
	circle_file: UploadFile | None = File(None),
	attach_file: UploadFile | None = File(None),

	# ✅ GATE fields (из формы)
	gate_next_flow: str = Form(""),
	gate_button_text: str = Form(""),
	gate_reminder_value: int = Form(0),
	gate_reminder_unit: str = Form("hours"),
	gate_reminder_text: str = Form(""),
):
	flow = (flow or "").strip()
	if not flow:
		return RedirectResponse("/", status_code=302)

	await create_flow(flow)

	# ✅ upload circle
	if circle_file and circle_file.filename:
		ext = os.path.splitext(circle_file.filename)[1].lower() or ".mp4"
		fname = f"{uuid.uuid4().hex}{ext}"
		with open(os.path.join("media", fname), "wb") as f:
			f.write(await circle_file.read())
		circle_path = f"/media/{fname}"

	# ✅ upload attachment
	if attach_file and attach_file.filename:
		orig_name = _safe_filename(attach_file.filename)
		ext = os.path.splitext(orig_name)[1].lower()
		fname = f"{uuid.uuid4().hex}{ext}" if ext else f"{uuid.uuid4().hex}"

		with open(os.path.join("media", fname), "wb") as f:
			f.write(await attach_file.read())

		file_path = f"/media/{fname}"
		file_name = orig_name

		ct = (attach_file.content_type or "").lower()
		if ct.startswith("image/"):
			file_kind = "photo"
		elif ct.startswith("video/"):
			file_kind = "video"
		elif ct.startswith("audio/"):
			file_kind = "audio"
		else:
			file_kind = "document"

	# buttons
	buttons = []
	for t, u in [(btn1_text, btn1_url), (btn2_text, btn2_url), (btn3_text, btn3_url)]:
		t = (t or "").strip()
		u = (u or "").strip()
		if t and u:
			buttons.append({"text": t, "url": u})

	buttons_final = ""
	if (buttons_json or "").strip():
		buttons_final = buttons_json.strip()
	elif buttons:
		buttons_final = json.dumps(buttons, ensure_ascii=False)

	# ✅ gate normalize
	gate_next_flow = (gate_next_flow or "").strip()
	gate_button_text = (gate_button_text or "").strip()
	gate_reminder_value = int(gate_reminder_value or 0)
	if gate_reminder_value < 0:
		gate_reminder_value = 0

	gate_reminder_unit = (gate_reminder_unit or "hours").strip().lower()
	if gate_reminder_unit not in ("minutes", "hours", "days"):
		gate_reminder_unit = "hours"

	gate_reminder_seconds = gate_reminder_value * _unit_to_seconds(gate_reminder_unit)
	gate_reminder_text = (gate_reminder_text or "").strip()

	data = {
		"flow": flow,
		"position": int(position),
		"type": type,
		"title": title,
		"text": text,
		"circle": circle_path,
		"video": video_url,
		"buttons": buttons_final,
		"is_active": int(is_active),
		"delay": float(delay_seconds),

		"file_path": (file_path or "").strip(),
		"file_kind": (file_kind or "").strip(),
		"file_name": (file_name or "").strip(),

		# ✅ GATE persist
		"gate_next_flow": gate_next_flow,
		"gate_button_text": gate_button_text,
		"gate_reminder_seconds": int(gate_reminder_seconds),
		"gate_reminder_text": gate_reminder_text,
	}

	if int(block_id) == 0:
		await create_block(data)
	else:
		await update_block(int(block_id), data)

	return RedirectResponse(f"/flow/{flow}", status_code=302)


@app.post("/block/{block_id}/delete")
async def delete_block_action(block_id: int, flow: str = Form(...)):
	await delete_block(block_id)
	return RedirectResponse(f"/flow/{flow}", status_code=302)


@app.post("/block/{block_id}/up")
async def move_up(block_id: int, flow: str = Form(...)):
	blocks = await get_blocks(flow)
	idx = next((i for i, b in enumerate(blocks) if b["id"] == block_id), None)
	if idx is not None and idx > 0:
		await swap_positions(blocks[idx]["id"], blocks[idx - 1]["id"])
	return RedirectResponse(f"/flow/{flow}", status_code=302)


@app.post("/block/{block_id}/down")
async def move_down(block_id: int, flow: str = Form(...)):
	blocks = await get_blocks(flow)
	idx = next((i for i, b in enumerate(blocks) if b["id"] == block_id), None)
	if idx is not None and idx < len(blocks) - 1:
		await swap_positions(blocks[idx]["id"], blocks[idx + 1]["id"])
	return RedirectResponse(f"/flow/{flow}", status_code=302)