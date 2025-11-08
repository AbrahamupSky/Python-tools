import os
import json
import time
import threading
from pathlib import Path
from typing import List, Tuple, Optional

import flet as ft

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".m4v", ".wmv", ".flv", ".webm"}
SETTINGS_PATH = Path.home() / ".jellyfin_renamer_settings.json"

def load_settings() -> dict:
  try:
    if SETTINGS_PATH.exists():
      return json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
  except Exception:
    pass
  return {
    "folder": "",
    "season": 1,
    "start": 1,
    "recurse": False,
    "use_ctime": False,
    "keep_titles": True,
    "theme_mode": "dark",
    "window_w": 1000,
    "window_h": 720,
    "last_log": "",
  }


def save_settings(s: dict):
  try:
    SETTINGS_PATH.write_text(json.dumps(s, indent=2), encoding="utf-8")
  except Exception:
    pass


def list_media_files(folder: Path, recurse: bool) -> List[Path]:
  if recurse:
    files = [p for p in folder.rglob("*") if p.is_file() and p.suffix.lower() in VIDEO_EXTS]
  else:
    files = [p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in VIDEO_EXTS]
  return files


def sort_files_by_time(files: List[Path], use_ctime: bool) -> List[Path]:
  key_fn = (lambda p: (p.stat().st_ctime, p.name.lower())) if use_ctime \
    else (lambda p: (p.stat().st_mtime, p.name.lower()))
  return sorted(files, key=key_fn)


def already_named_like(target_stem: str, fname: str) -> bool:
  return fname.upper().startswith(target_stem.upper())


def build_new_name(season: int, episode: int, ext: str, keep_titles: bool, original_name: str) -> str:
  tag = f"S{season:02d}E{episode:02d}"
  if keep_titles:
    base = Path(original_name).stem
    base_clean = base.lstrip(" .-_")
    if already_named_like(tag, base_clean):
      title_part = base_clean[len(tag):].lstrip(" .-_")
    else:
      title_part = base_clean
    return f"{tag}{(' - ' + title_part) if title_part else ''}{ext}"
  else:
    return f"{tag}{ext}"


def plan_changes(files: List[Path], season: int, start_at: int, keep_titles: bool) -> List[Tuple[Path, Path]]:
  ops = []
  ep = start_at
  for f in files:
    ext = f.suffix
    new_name = build_new_name(season, ep, ext, keep_titles, f.name)
    target = f.with_name(new_name)
    if f.name == new_name:
      ops.append((f, f))  # no-op
    else:
      ops.append((f, target))
    ep += 1
  return ops


def ensure_no_overwrites(ops: List[Tuple[Path, Path]]) -> Tuple[bool, List[str]]:
  errors = []
  targets = {}
  for src, dst in ops:
    if src == dst:
      continue
    if dst.exists() and dst.resolve() != src.resolve():
      errors.append(f"Would overwrite: {dst}")
    key = dst.resolve()
    if key in targets and targets[key] != src.resolve():
      errors.append(f"Multiple sources mapped to same target: {dst}")
    targets[key] = src.resolve()
  return (len(errors) == 0, errors)


def main(page: ft.Page):
  # ---------- Settings ----------
  settings = load_settings()

  page.title = "Episode Renamer"
  page.window_width = settings.get("window_w", 1000)
  page.window_height = settings.get("window_h", 720)
  page.window_min_width = 900
  page.window_min_height = 600
  page.padding = 16
  page.scroll = "auto"
  page.theme_mode = ft.ThemeMode.DARK if settings.get("theme_mode", "dark") == "dark" else ft.ThemeMode.LIGHT

  # Persist window size on close
  def on_close(e):
    settings["window_w"] = int(page.window_width or settings["window_w"])
    settings["window_h"] = int(page.window_height or settings["window_h"])
    save_settings(settings)
    page.window_destroy()

  page.on_window_event = lambda e: on_close(e) if e.data == "close" else None

  # ---------- State ----------
  planned_ops: List[Tuple[Path, Path]] = []
  files_sorted: List[Path] = []
  # For inline edit tracking: index -> edited new name
  edited_names: dict[int, str] = {}

  applying_lock = threading.Lock()
  applying_flag = False
  cancel_event = threading.Event()

  # ---------- Controls ----------
  # Top bar
  title_text = ft.Text("Episode Renamer", size=22, weight=ft.FontWeight.BOLD)
  theme_switch = ft.Switch(label="Dark mode", value=(page.theme_mode == ft.ThemeMode.DARK))

  def on_theme_toggle(e):
    page.theme_mode = ft.ThemeMode.DARK if theme_switch.value else ft.ThemeMode.LIGHT
    settings["theme_mode"] = "dark" if theme_switch.value else "light"
    save_settings(settings)
    page.update()

  theme_switch.on_change = on_theme_toggle

  # Inputs
  folder_tf = ft.TextField(label="Folder", read_only=True, expand=True, value=settings.get("folder", ""))
  pick_btn = ft.ElevatedButton("Pick Folder", icon=ft.Icons.FOLDER_OPEN)

  season_tf = ft.TextField(label="Season", value=str(settings.get("season", 1)), width=120)
  start_tf = ft.TextField(label="Start Episode", value=str(settings.get("start", 1)), width=150)
  recurse_chk = ft.Checkbox(label="Recurse subfolders", value=settings.get("recurse", False))
  ctime_chk = ft.Checkbox(label="Sort by creation time (Windows)", value=settings.get("use_ctime", False))
  keep_titles_chk = ft.Checkbox(label="Keep titles after SxxExx", value=settings.get("keep_titles", True))

  preview_btn = ft.FilledButton("Preview", icon=ft.Icons.PREVIEW)
  apply_btn = ft.ElevatedButton("Apply Renames", icon=ft.Icons.DRIVE_FILE_RENAME_OUTLINE, disabled=True)
  undo_btn = ft.OutlinedButton("Undo Last Run (Ctrl+Z)", icon=ft.Icons.UNDO, disabled=(settings.get("last_log", "") == ""))
  status_text = ft.Text("", selectable=True)

  progress_bar = ft.ProgressBar(value=0, width=400, visible=False)
  cancel_btn = ft.TextButton("Cancel", icon=ft.Icons.CANCEL, visible=False)

  # Table
  table = ft.DataTable(
    columns=[
      ft.DataColumn(ft.Text("#")),
      ft.DataColumn(ft.Text("Current Name")),
      ft.DataColumn(ft.Text("New Name (editable)")),
      ft.DataColumn(ft.Text("Status")),
    ],
    rows=[],
    width=page.width - 64,
    data_row_max_height=56,
    column_spacing=20,
    heading_row_height=40,
  )

  # File picker overlay
  picker = ft.FilePicker(on_result=lambda e: None)
  page.overlay.append(picker)

  def open_folder_picker(e):
    def on_pick(res: ft.FilePickerResultEvent):
      if res.path:
        folder_tf.value = res.path
        folder_tf.update()
        settings["folder"] = res.path
        save_settings(settings)
    picker.on_result = on_pick
    picker.get_directory_path(dialog_title="Select season folder")

  pick_btn.on_click = open_folder_picker

  # ---------- Helpers ----------
  def clear_table():
    table.rows.clear()
    edited_names.clear()
    table.update()

  def parse_int(tf: ft.TextField, default: int) -> int:
    try:
      v = int(tf.value.strip())
      return v if v >= 0 else default
    except Exception:
      return default

  def add_row(idx: int, src: Path, dst: Path, status: str, conflict: bool = False, skip: bool = False):
    # Create editable TextField for "New Name"
    new_name_tf = ft.TextField(
      value=dst.name,
      dense=True,
      on_change=lambda e, i=idx: edited_names.__setitem__(i, e.control.value),
      width=450,
    )
    color = ft.Colors.RED if conflict else (ft.Colors.GREY if skip else None)
    row = ft.DataRow(
      cells=[
        ft.DataCell(ft.Text(str(idx))),
        ft.DataCell(ft.Text(src.name)),
        ft.DataCell(new_name_tf),
        ft.DataCell(ft.Text(status, color=color)),
      ]
    )
    table.rows.append(row)

  def rebuild_ops_with_edits() -> List[Tuple[Path, Path]]:
    """Apply inline edits to destination names."""
    new_ops: List[Tuple[Path, Path]] = []
    for i, (src, dst) in enumerate(planned_ops, start=1):
      new_name = edited_names.get(i, dst.name)
      new_ops.append((src, src.with_name(new_name)))
    return new_ops

  def save_current_settings():
    settings["season"] = parse_int(season_tf, 1)
    settings["start"] = parse_int(start_tf, 1)
    settings["recurse"] = recurse_chk.value
    settings["use_ctime"] = ctime_chk.value
    settings["keep_titles"] = keep_titles_chk.value
    save_settings(settings)

  # ---------- Actions ----------
  def do_preview(e):
    clear_table()
    apply_btn.disabled = True
    status_text.value = ""

    folder = Path(folder_tf.value.strip()) if folder_tf.value.strip() else None
    if not folder or not folder.exists() or not folder.is_dir():
      status_text.value = "Pick a valid folder."
      page.update()
      return

    season = parse_int(season_tf, 1)
    start_ep = parse_int(start_tf, 1)

    try:
      found = list_media_files(folder, recurse_chk.value)
    except Exception as ex:
      status_text.value = f"Failed to list files: {ex}"
      page.update()
      return

    if not found:
      status_text.value = "No video files found."
      page.update()
      return

    # Sort and plan
    nonlocal files_sorted, planned_ops
    files_sorted = sort_files_by_time(found, ctime_chk.value)
    planned_ops = plan_changes(files_sorted, season, start_ep, keep_titles_chk.value)

    # Build initial table
    for i, (src, dst) in enumerate(planned_ops, start=1):
      if src == dst:
        add_row(i, src, dst, "SKIP (already named)", skip=True)
      else:
        add_row(i, src, dst, "OK")

    # Save settings persistently
    save_current_settings()

    # Validate conflicts based on current plan
    ok, errs = ensure_no_overwrites(planned_ops)
    if not ok:
      status_text.value = "Conflicts detected:\n" + "\n".join(f"- {e}" for e in errs)
      apply_btn.disabled = True
    else:
      status_text.value = f"Preview ready: {len(planned_ops)} file(s). You can edit 'New Name' cells. When ready, click Apply."
      apply_btn.disabled = False

    table.update()
    apply_btn.update()
    status_text.update()

  preview_btn.on_click = do_preview

  def do_apply(e):
    nonlocal applying_flag

    if not planned_ops:
      status_text.value = "Nothing to apply. Click Preview first."
      status_text.update()
      return

    # Build ops using any inline edits
    ops = rebuild_ops_with_edits()

    # Re-validate conflicts before applying
    ok, errs = ensure_no_overwrites(ops)
    if not ok:
      status_text.value = "Aborted due to conflicts:\n" + "\n".join(f"- {x}" for x in errs)
      status_text.update()
      return

    # Disable UI during apply; show progress/cancel
    apply_btn.disabled = True
    preview_btn.disabled = True
    pick_btn.disabled = True
    cancel_btn.visible = True
    progress_bar.visible = True
    progress_bar.value = 0
    page.update()

    # Log for undo
    folder = Path(folder_tf.value.strip())
    log_path = folder / f"_rename_log_{int(time.time())}.json"

    cancel_event.clear()

    def run_apply():
      nonlocal applying_flag
      with applying_lock:
        applying_flag = True
      errors = []
      changes: List[dict] = []

      total = sum(1 for s, d in ops if s != d)
      done = 0

      try:
        for src, dst in ops:
          if cancel_event.is_set():
            errors.append("Operation canceled by user.")
            break
          if src == dst:
            # No-op, ignore in progress
            continue
          try:
            os.rename(src, dst)
            changes.append({"from": str(src), "to": str(dst)})
            done += 1
          except Exception as ex:
            errors.append(f"Failed to rename '{src.name}': {ex}")
          # Update progress UI
          page.call_from_thread(update_progress, done, total)

        # Save log if anything changed
        if changes:
          try:
            log_path.write_text(json.dumps(changes, indent=2), encoding="utf-8")
            settings["last_log"] = str(log_path)
            save_settings(settings)
            page.call_from_thread(update_undo_btn)
          except Exception as ex:
            errors.append(f"Failed to write undo log: {ex}")

      finally:
        with applying_lock:
          applying_flag = False
        page.call_from_thread(apply_finished, done, total, errors)

    def update_progress(done, total):
      progress_bar.value = (done / total) if total else 0
      progress_bar.update()

    def update_undo_btn():
      undo_btn.disabled = False
      undo_btn.update()

    def apply_finished(done, total, errors):
      progress_bar.visible = False
      cancel_btn.visible = False
      preview_btn.disabled = False
      pick_btn.disabled = False
      apply_btn.disabled = False  # in case user wants to apply again

      msg = f"Done. Renamed {done}/{total} file(s)."
      if cancel_event.is_set():
        msg = f"Canceled. Renamed {done}/{total} file(s) before cancel."
      if errors:
        msg += "\nErrors:\n" + "\n".join(f"- {x}" for x in errors)
      status_text.value = msg
      status_text.update()

      # Refresh preview to show final state
      do_preview(None)

    # Background thread so UI stays responsive
    t = threading.Thread(target=run_apply, daemon=True)
    t.start()

  apply_btn.on_click = do_apply

  def do_cancel(e):
    # Signal cancel; the apply loop checks cancel_event
    cancel_event.set()
    status_text.value = "Cancel requested… finishing current step."
    status_text.update()

  cancel_btn.on_click = do_cancel

  def do_undo(e):
    last_log = settings.get("last_log", "")
    if not last_log:
      status_text.value = "No undo log found."
      status_text.update()
      return
    log_file = Path(last_log)
    if not log_file.exists():
      status_text.value = "Undo log not found on disk."
      status_text.update()
      return

    try:
      changes = json.loads(log_file.read_text(encoding="utf-8"))
    except Exception as ex:
      status_text.value = f"Failed to read undo log: {ex}"
      status_text.update()
      return

    # Perform reverse renames
    reverted = 0
    errs = []
    for item in reversed(changes):
      src = Path(item["to"])
      dst = Path(item["from"])
      try:
        if src.exists() and not dst.exists():
          os.rename(src, dst)
          reverted += 1
        # if dst exists, skip to avoid overwrites
      except Exception as ex:
        errs.append(f"Failed to undo '{src.name}': {ex}")

    msg = f"Undo complete. Reverted {reverted}/{len(changes)}."
    if errs:
      msg += "\nErrors:\n" + "\n".join(f"- {x}" for x in errs)
    status_text.value = msg
    status_text.update()

    # Refresh preview
    do_preview(None)

  undo_btn.on_click = do_undo

  # ---------- Keyboard Shortcuts ----------
  def on_key(e: ft.KeyboardEvent):
    if e.ctrl and e.key.lower() == "p":
      do_preview(None)
    elif e.ctrl and (e.key == "Enter" or e.key == "NumpadEnter"):
      if not apply_btn.disabled:
        do_apply(None)
    elif e.ctrl and e.key.lower() == "z":
      if not undo_btn.disabled:
        do_undo(None)

  page.on_keyboard_event = on_key

  # ---------- Layout ----------
  page.add(
    ft.Column(
      [
        ft.Row([title_text, ft.Container(expand=True), theme_switch], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
        ft.Row([folder_tf, pick_btn], vertical_alignment=ft.CrossAxisAlignment.CENTER),
        ft.Row(
          [season_tf, start_tf, recurse_chk, ctime_chk, keep_titles_chk],
          wrap=True,
          vertical_alignment=ft.CrossAxisAlignment.CENTER,
        ),
        ft.Row([preview_btn, apply_btn, undo_btn, progress_bar, cancel_btn], spacing=12),
        ft.Divider(),
        ft.Text("Preview (double-check or edit 'New Name' before Apply)", size=16, weight=ft.FontWeight.BOLD),
        ft.Container(
          content=ft.Column([table], scroll="auto"),
          height=380,
          padding=0,
        ),
        ft.Divider(),
        status_text,
        ft.Text("Shortcuts:  Ctrl+P Preview   •   Ctrl+Enter Apply   •   Ctrl+Z Undo", size=12, color=ft.Colors.GREY),
      ],
      spacing=12,
    )
  )

  # Auto-preview if a folder is already stored
  if settings.get("folder"):
    do_preview(None)


if __name__ == "__main__":
  ft.app(target=main)
