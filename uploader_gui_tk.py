import os
import sys
import re
import threading
import queue

from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np
import subprocess

from core.config import (
    get_data_dir,
    load_config as core_load_config,
    save_config as core_save_config,
    compute_edge_url,
    validate_config,
)
from core.transform import build_records_plc
from core import files as core_files
import core.upload as core_upload
import core.work_log as core_work_log
import core.cycle_processing as core_cycle


# Tkinter UI
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from PIL import Image

KST = timezone(timedelta(hours=9))

# Data directory (AppData) for persistent state
# Data directory (AppData) for persistent state
DATA_DIR = get_data_dir()
LOG_PATH = os.path.join(DATA_DIR, 'processed_files.log')
RESUME_PATH = os.path.join(DATA_DIR, 'upload_resume.json')

def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.dirname(os.path.abspath(__file__))

    return os.path.join(base_path, relative_path)

# Icon path for window/taskbar (local asset)
APP_ICON = resource_path(os.path.join('assets', 'app.ico'))

# Set explicit AppUserModelID on Windows so taskbar uses our icon
if os.name == 'nt':
    try:
        import ctypes

        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("ExtrusionUploader")
    except Exception:
        pass


def _migrate_legacy_state_gui():
    """GUI-side migration of legacy state files into AppData.
    Safe union/merge like CLI.
    """
    import core.state as core_state

    core_state.migrate_legacy_state(os.path.dirname(os.path.abspath(__file__)))

def kst_now() -> datetime:
    return datetime.now(KST)


def load_processed() -> set:
    import core.state as core_state

    return core_state.load_processed(LOG_PATH)


def log_processed(folder: str, filename: str):
    import core.state as core_state

    core_state.log_processed(folder, filename, LOG_PATH)


# --- Resume state (파일별 마지막 배치 오프셋) ---
def load_resume() -> dict:
    import core.state as core_state

    return core_state.load_resume(RESUME_PATH)


def save_resume(data: dict):
    import core.state as core_state

    core_state.save_resume(data, RESUME_PATH)


def set_resume_offset(key: str, offset: int):
    import core.state as core_state

    core_state.set_resume_offset(key, offset, RESUME_PATH)


def get_resume_offset(key: str) -> int:
    import core.state as core_state

    return core_state.get_resume_offset(key, RESUME_PATH)


def is_locked(path: str) -> bool:
    return core_files.is_locked(path)


def file_mtime_kst(path: str) -> datetime:
    return core_files.file_mtime_kst(path)


def parse_plc_date_from_filename(name: str) -> datetime | None:
    return core_files.parse_plc_date_from_filename(name)


def parse_temp_end_date_from_filename(name: str) -> datetime | None:
    return core_files.parse_temp_end_date_from_filename(name)


def within_cutoff(file_date: datetime, cutoff_date: datetime) -> bool:
    return core_files.within_cutoff(file_date, cutoff_date)


def stable_enough(path: str, lag_minutes: int) -> bool:
    return core_files.stable_enough(path, lag_minutes)


def load_config(path: str | None = None) -> dict:
    cfg, _ = core_load_config(path)
    return cfg


def save_config(values: dict, path: str | None = None):
    core_save_config(values, path)


def compute_cutoff(mode: str, custom_date: str) -> datetime:
    # Delegate to shared core.files implementation for consistency
    return core_files.compute_cutoff(mode, custom_date)


def process_file(kind: str, path: str, filename: str) -> pd.DataFrame:
    try:
        # Single mode: always use build_records_plc (now integrated)
        return build_records_plc(path, filename)
    except Exception:
        pass
    return pd.DataFrame()


def preview_diagnostics(plc_dir: str, temp_dir: str, cutoff: datetime, lag_min: int, include_today: bool, check_lock: bool):
    included = []  # (folder, filename, path, kind)
    excluded = []  # (folder, filename, reason)
    processed = load_processed()

    # Helper to validate content
    def has_data(kind: str, path: str, filename: str) -> bool:
        df = process_file(kind, path, filename)
        return not df.empty

    # PLC
    if os.path.isdir(plc_dir):
        for fn in sorted(os.listdir(plc_dir)):
            full = os.path.join(plc_dir, fn)
            if not fn.lower().endswith('.csv'):
                excluded.append((plc_dir, fn, 'CSV 아님'))
                continue
            fdate = parse_plc_date_from_filename(fn)
            if not fdate or not within_cutoff(fdate, cutoff):
                excluded.append((plc_dir, fn, '컷오프 범위 밖'))
                continue
            if f"{plc_dir}/{fn}" in processed or fn in processed:
                excluded.append((plc_dir, fn, '이미 처리됨'))
                continue
            if fdate.date() == kst_now().date() and include_today:
                if not stable_enough(full, lag_min):
                    excluded.append((plc_dir, fn, f'오늘 파일 미안정({lag_min}분 이내 변경)'))
                    continue
                if check_lock and is_locked(full):
                    excluded.append((plc_dir, fn, '파일 잠금'))
                    continue
            # content check
            if has_data('plc', full, fn):
                included.append((plc_dir, fn, full, 'plc'))
            else:
                excluded.append((plc_dir, fn, '데이터 없음'))

    # Temperature
    if temp_dir and os.path.isdir(temp_dir):
        for fn in sorted(os.listdir(temp_dir)):
            full = os.path.join(temp_dir, fn)
            if not fn.lower().endswith('.csv'):
                excluded.append((temp_dir, fn, 'CSV 아님'))
                continue
            fdate = parse_temp_end_date_from_filename(fn)
            if not fdate:
                try:
                    fdate = file_mtime_kst(full)
                except Exception:
                    fdate = None
            if not fdate or not within_cutoff(fdate, cutoff):
                excluded.append((temp_dir, fn, '컷오프 범위 밖'))
                continue
            if f"{temp_dir}/{fn}" in processed or fn in processed:
                excluded.append((temp_dir, fn, '이미 처리됨'))
                continue
            if fdate.date() == kst_now().date() and include_today:
                if not stable_enough(full, lag_min):
                    excluded.append((temp_dir, fn, f'오늘 파일 미안정({lag_min}분 이내 변경)'))
                    continue
                if check_lock and is_locked(full):
                    excluded.append((temp_dir, fn, '파일 잠금'))
                    continue
            if has_data('temp', full, fn):
                included.append((temp_dir, fn, full, 'temp'))
            else:
                excluded.append((temp_dir, fn, '데이터 없음'))

    return included, excluded


import customtkinter as ctk

# Set theme
ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")

class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title('Extrusion Uploader')
        self.geometry('1100x700')
        self.resizable(True, True)
        try:
            self.iconbitmap(APP_ICON)
        except Exception:
            pass
        
        self.cfg = load_config()
        
        # Shared state
        self.active_progress = {}
        self.progress_lock = threading.Lock()
        self.total_files = 0
        self.processed_count = 0
        self.is_uploading = False
        self.pause_event = threading.Event()
        self.pause_event.set() # Start as running (not paused)
        
        # Thread-safe logging
        self.log_queue = queue.Queue()
        self.check_log_queue()
        
        # Grid layout (1x2)
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)
        
        self.create_sidebar()
        self.create_main_area()
        
        # Handle window close
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Initial View
        self.show_dashboard()
        
        # Close Splash Screen
        self.after(200, self.close_splash)


    def on_closing(self):
        self.destroy()
        os._exit(0)

    def close_splash(self):
        try:
            import pyi_splash
            pyi_splash.close()
            print("Splash screen closed.")
        except ImportError:
            pass

    def create_sidebar(self):
        self.sidebar = ctk.CTkFrame(self, width=200, corner_radius=0)
        self.sidebar.grid(row=0, column=0, sticky="nsew")
        self.sidebar.grid_rowconfigure(5, weight=1)
        
        # Load Logo
        logo_path = resource_path(os.path.join('assets', 'logo.png'))
        try:
            logo_img = ctk.CTkImage(light_image=Image.open(logo_path), dark_image=Image.open(logo_path), size=(80, 80))
            self.logo_label = ctk.CTkLabel(self.sidebar, text="Extrusion\nUploader", image=logo_img, compound="top", font=ctk.CTkFont(size=20, weight="bold"))
        except Exception as e:
            print(f"Logo load failed: {e}")
            self.logo_label = ctk.CTkLabel(self.sidebar, text="Extrusion\nUploader", font=ctk.CTkFont(size=20, weight="bold"))
            
        self.logo_label.grid(row=0, column=0, padx=20, pady=(20, 10))
        
        self.btn_dash = ctk.CTkButton(self.sidebar, text="Dashboard", command=self.show_dashboard)
        self.btn_dash.grid(row=1, column=0, padx=20, pady=10)
        
        self.btn_settings = ctk.CTkButton(self.sidebar, text="Settings", command=self.show_settings)
        self.btn_settings.grid(row=2, column=0, padx=20, pady=10)
        
        self.btn_logs = ctk.CTkButton(self.sidebar, text="Logs", command=self.show_logs)
        self.btn_logs.grid(row=3, column=0, padx=20, pady=10)
        
        self.btn_work_log = ctk.CTkButton(self.sidebar, text="Work Log", command=self.show_work_log)
        self.btn_work_log.grid(row=4, column=0, padx=20, pady=10)

        self.btn_data = ctk.CTkButton(self.sidebar, text="Data Mgmt", command=self.show_data_mgmt)
        self.btn_data.grid(row=5, column=0, padx=20, pady=10)
        
        # Status indicator at bottom
        self.status_label = ctk.CTkLabel(self.sidebar, text="Ready", text_color="gray")

        # Auto-Start Upload Check
        if self.cfg.get('AUTO_UPLOAD') == 'true':
            self.log_queue.put("자동 업로드 설정이 켜져 있습니다. 5초 후 시작합니다...")
            self.after(5000, self.auto_start_upload)

    def auto_start_upload(self):
        """Called after delay if AUTO_UPLOAD is true"""
        if not self.is_uploading:
            self.log_queue.put("자동 업로드 시작!")
            self.on_start()

    def create_main_area(self):
        # Container for pages
        self.main_frame = ctk.CTkFrame(self, corner_radius=0, fg_color="transparent")
        self.main_frame.grid(row=0, column=1, sticky="nsew", padx=20, pady=20)
        self.main_frame.grid_rowconfigure(0, weight=1)
        self.main_frame.grid_columnconfigure(0, weight=1)

    def clear_main(self):
        for widget in self.main_frame.winfo_children():
            widget.destroy()

    # --- Views ---
    def show_dashboard(self):
        self.clear_main()
        
        # Hero Section (Progress)
        self.hero_frame = ctk.CTkFrame(self.main_frame)
        self.hero_frame.grid(row=0, column=0, sticky="ew", pady=(0, 20))
        
        self.lbl_big_status = ctk.CTkLabel(self.hero_frame, text="Waiting...", font=ctk.CTkFont(size=24, weight="bold"))
        self.lbl_big_status.pack(pady=(20, 10))
        
        self.prog_bar = ctk.CTkProgressBar(self.hero_frame, width=400)
        self.prog_bar.pack(pady=10)
        self.prog_bar.set(0)
        
        self.lbl_prog_text = ctk.CTkLabel(self.hero_frame, text="0.0% (0/0)")
        self.lbl_prog_text.pack(pady=(0, 20))
        
        # Active Tasks Section
        self.tasks_frame = ctk.CTkScrollableFrame(self.main_frame, label_text="Task Status")
        self.tasks_frame.grid(row=1, column=0, sticky="nsew")
        self.main_frame.grid_rowconfigure(1, weight=1)
        
        self.task_labels = {} # {filename: label_widget}
        
        # Action Buttons
        self.action_frame = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        self.action_frame.grid(row=2, column=0, sticky="ew", pady=10)
        
        start_state = "disabled" if self.is_uploading else "normal"
        self.btn_start = ctk.CTkButton(self.action_frame, text="Start Upload", command=self.on_start, state=start_state, fg_color="#2CC985", hover_color="#26A670")
        self.btn_start.pack(side="right", padx=10)
        
        # Determine initial state based on current upload status
        pause_state = "normal" if self.is_uploading else "disabled"
        pause_text = "Resume" if self.is_uploading and not self.pause_event.is_set() else "Pause"
        
        self.btn_pause = ctk.CTkButton(self.action_frame, text=pause_text, command=self.on_pause, state=pause_state, fg_color="#E5C07B", hover_color="#D1A03D")
        self.btn_pause.pack(side="right", padx=10)
        
        ctk.CTkButton(self.action_frame, text="Preview", command=self.on_preview).pack(side="right", padx=10)

        # Start update loop
        self.update_dashboard_loop()

    def show_settings(self):
        self.clear_main()
        # Reset grid weights (dashboard sets row 1 weight)
        self.main_frame.grid_rowconfigure(0, weight=1)
        self.main_frame.grid_rowconfigure(1, weight=0) 

        
        # Scrollable settings container
        sf = ctk.CTkScrollableFrame(self.main_frame, label_text="환경 설정")
        sf.grid(row=0, column=0, sticky="nsew")
        
        # Variables (sync with self.cfg)
        self.var_url = tk.StringVar(value=self.cfg['SUPABASE_URL'])
        self.var_anon = tk.StringVar(value=self.cfg['SUPABASE_ANON_KEY'])
        self.var_edge = tk.StringVar(value=self.cfg['EDGE_FUNCTION_URL'])
        self.var_plc = tk.StringVar(value=self.cfg['PLC_DIR'])
        # self.var_temp = tk.StringVar(value=self.cfg['TEMP_DIR']) # Removed

        self.var_smart_sync = tk.BooleanVar(value=(str(self.cfg.get('SMART_SYNC', 'true')).lower() == 'true'))
        self.var_auto_upload = tk.BooleanVar(value=(str(self.cfg.get('AUTO_UPLOAD', 'false')).lower() == 'true'))
        self.var_range = tk.StringVar(value=self.cfg['RANGE_MODE'])
        
        # Custom Date Variable
        self.var_custom_date = tk.StringVar(value=self.cfg.get('CUSTOM_DATE', ''))

        # UI Helpers
        def add_entry(parent, label, var, row):
            ctk.CTkLabel(parent, text=label).grid(row=row, column=0, sticky="w", padx=10, pady=5)
            ctk.CTkEntry(parent, textvariable=var, width=400).grid(row=row, column=1, padx=10, pady=5)
            
        def add_path(parent, label, var, row, cmd):
            ctk.CTkLabel(parent, text=label).grid(row=row, column=0, sticky="w", padx=10, pady=5)
            ctk.CTkEntry(parent, textvariable=var, width=300).grid(row=row, column=1, padx=10, pady=5)
            ctk.CTkButton(parent, text="찾기", width=80, command=cmd).grid(row=row, column=2, padx=10, pady=5)

        # Connection
        grp_conn = ctk.CTkFrame(sf)
        grp_conn.pack(fill="x", padx=10, pady=10)
        ctk.CTkLabel(grp_conn, text="연결 설정", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, sticky="w", padx=10, pady=5)
        add_entry(grp_conn, "Supabase URL", self.var_url, 1)
        add_entry(grp_conn, "Anon Key", self.var_anon, 2)
        add_entry(grp_conn, "Edge URL", self.var_edge, 3)
        
        # Folders
        grp_folder = ctk.CTkFrame(sf)
        grp_folder.pack(fill="x", padx=10, pady=10)
        ctk.CTkLabel(grp_folder, text="폴더 설정", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, sticky="w", padx=10, pady=5)
        add_path(grp_folder, "데이터 폴더", self.var_plc, 1, self.pick_plc)
        # Temp folder removed

        
        # Options
        grp_opt = ctk.CTkFrame(sf)
        grp_opt.pack(fill="x", padx=10, pady=10)
        ctk.CTkLabel(grp_opt, text="옵션", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, sticky="w", padx=10, pady=5)
        
        ctk.CTkSwitch(grp_opt, text="Smart Sync (최신 데이터만 전송)", variable=self.var_smart_sync).grid(row=1, column=0, columnspan=2, sticky="w", padx=10, pady=10)
        ctk.CTkSwitch(grp_opt, text="앱 실행 시 자동 업로드 시작", variable=self.var_auto_upload).grid(row=2, column=0, columnspan=2, sticky="w", padx=10, pady=10)
        
        ctk.CTkLabel(grp_opt, text="업로드 범위").grid(row=3, column=0, sticky="w", padx=10, pady=5)
        
        def on_range_change(choice):
            if choice == 'custom':
                self.frame_custom_date.grid(row=3, column=2, sticky="w", padx=10, pady=5)
            else:
                self.frame_custom_date.grid_forget()

        ctk.CTkOptionMenu(grp_opt, variable=self.var_range, values=['today', 'yesterday', 'twodays', 'custom'], command=on_range_change).grid(row=3, column=1, sticky="w", padx=10, pady=5)
        
        # Custom Date Entry (Hidden by default)
        self.frame_custom_date = ctk.CTkFrame(grp_opt, fg_color="transparent")
        ctk.CTkLabel(self.frame_custom_date, text="날짜(YYYY-MM-DD):").pack(side="left", padx=5)
        ctk.CTkEntry(self.frame_custom_date, textvariable=self.var_custom_date, width=120).pack(side="left", padx=5)
        
        if self.var_range.get() == 'custom':
            self.frame_custom_date.grid(row=3, column=2, sticky="w", padx=10, pady=5)

        # Save Button
        ctk.CTkButton(self.main_frame, text="설정 저장", command=self.on_save).grid(row=2, column=0, pady=20)

    def show_logs(self):
        self.clear_main()
        self.log_box = ctk.CTkTextbox(self.main_frame, width=600)
        self.log_box.grid(row=0, column=0, sticky="nsew")
        self.main_frame.grid_rowconfigure(0, weight=1)
        
        # Restore history
        if hasattr(self, 'log_history'):
            self.log_box.insert("1.0", "\n".join(self.log_history) + "\n")
            self.log_box.see("end")

    def show_work_log(self):
        self.clear_main()
        
        # Header
        ctk.CTkLabel(self.main_frame, text="작업일보 업로드", font=ctk.CTkFont(size=20, weight="bold")).pack(pady=20)
        
        # File Selection
        frame_file = ctk.CTkFrame(self.main_frame)
        frame_file.pack(fill="x", padx=20, pady=10)
        
        self.lbl_work_log_file = ctk.CTkLabel(frame_file, text="선택된 파일 없음", text_color="gray")
        self.lbl_work_log_file.pack(side="left", padx=10, expand=True)
        
        ctk.CTkButton(frame_file, text="파일 선택 (Excel)", command=self.on_select_work_log).pack(side="right", padx=10)
        
        # Upload Button
        self.btn_upload_work_log = ctk.CTkButton(self.main_frame, text="업로드 시작", command=self.on_upload_work_log, state="disabled", fg_color="#2CC985")
        self.btn_upload_work_log.pack(pady=20)
        
        # Log area
        self.work_log_box = ctk.CTkTextbox(self.main_frame, width=600, height=300)
        self.work_log_box.pack(fill="both", expand=True, padx=20, pady=10)
        
    def on_select_work_log(self):
        f = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx *.xls *.xlsm")])
        if f:
            self.selected_work_log_path = f
            self.lbl_work_log_file.configure(text=os.path.basename(f), text_color="white")
            self.btn_upload_work_log.configure(state="normal")
            
    def on_upload_work_log(self):
        if not hasattr(self, 'selected_work_log_path') or not self.selected_work_log_path:
            return
            
        self.btn_upload_work_log.configure(state="disabled")
        path = self.selected_work_log_path
        
        def _run():
            self.log_to_box(f"파일 분석 시작: {os.path.basename(path)}")
            try:
                df = core_work_log.parse_work_log_excel(path)
                self.log_to_box(f"분석 완료: {len(df)}건 발견")
                
                url = self.cfg['SUPABASE_URL']
                anon = self.cfg['SUPABASE_ANON_KEY']
                
                self.log_to_box("업로드 중...")
                ok = core_upload.upload_work_log_data(url, anon, df, self.log_to_box)
                
                if ok:
                    self.log_to_box("작업 완료!")
                    messagebox.showinfo("성공", "작업일보 업로드가 완료되었습니다.")
                else:
                    self.log_to_box("업로드 실패.")
                    messagebox.showerror("실패", "업로드 중 오류가 발생했습니다.")
                    
            except Exception as e:
                self.log_to_box(f"오류 발생: {e}")
                messagebox.showerror("오류", str(e))
            finally:
                self.btn_upload_work_log.configure(state="normal")
                
        threading.Thread(target=_run, daemon=True).start()

    def log_to_box(self, msg):
        self.after(0, lambda: self._append_work_log_msg(msg))
        
    def _append_work_log_msg(self, msg):
        if hasattr(self, 'work_log_box') and self.work_log_box.winfo_exists():
            self.work_log_box.insert("end", msg + "\n")
            self.work_log_box.see("end")

    # --- Data Management View ---
    def show_data_mgmt(self):
        self.clear_main()
        
        ctk.CTkLabel(self.main_frame, text="데이터 관리 (Data Management)", font=ctk.CTkFont(size=20, weight="bold")).pack(pady=20)
        
        frame_actions = ctk.CTkFrame(self.main_frame)
        frame_actions.pack(fill="x", padx=20, pady=10)
        
        ctk.CTkLabel(frame_actions, text="사이클 분석 (Cycle Segmentation)", font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=10, pady=5)
        ctk.CTkLabel(frame_actions, text="새로 추가된 센서 데이터를 분석하여 사이클 단위로 변환하고 DB에 저장합니다.", text_color="gray").pack(anchor="w", padx=10)
        
        # Range selection (similar to Settings)
        range_frame = ctk.CTkFrame(frame_actions)
        range_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(range_frame, text="처리 범위:").pack(side="left", padx=5)
        self.var_cycle_range = tk.StringVar(value="incremental")
        self.cycle_range_menu = ctk.CTkOptionMenu(
            range_frame, 
            variable=self.var_cycle_range,
            values=['incremental', 'all', 'today', 'yesterday', 'custom'],
            command=self.on_cycle_range_change
        )
        self.cycle_range_menu.pack(side="left", padx=5)
        
        # Custom date entry (hidden by default)
        self.cycle_custom_date_frame = ctk.CTkFrame(range_frame)
        self.var_cycle_custom_date = tk.StringVar(value="2025-01-01")
        ctk.CTkLabel(self.cycle_custom_date_frame, text="시작일:").pack(side="left", padx=5)
        ctk.CTkEntry(self.cycle_custom_date_frame, textvariable=self.var_cycle_custom_date, width=100).pack(side="left", padx=5)
        
        self.btn_run_analysis = ctk.CTkButton(frame_actions, text="분석 실행", command=self.on_run_analysis, fg_color="#2CC985")
        self.btn_run_analysis.pack(pady=10)
        
        # Progress bar
        self.data_progress_bar = ctk.CTkProgressBar(self.main_frame, width=400)
        self.data_progress_bar.pack(pady=10)
        self.data_progress_bar.set(0)
        
        self.data_progress_label = ctk.CTkLabel(self.main_frame, text="0%")
        self.data_progress_label.pack()
        
        # Log area
        self.data_log_box = ctk.CTkTextbox(self.main_frame, width=600, height=300)
        self.data_log_box.pack(fill="both", expand=True, padx=20, pady=10)
        
    def on_cycle_range_change(self, choice):
        """Show/hide custom date entry based on selection"""
        if choice == 'custom':
            self.cycle_custom_date_frame.pack(side="left", padx=5)
        else:
            self.cycle_custom_date_frame.pack_forget()
        
    def on_run_analysis(self):
        self.btn_run_analysis.configure(state="disabled")
        self.data_log_box.delete("1.0", "end")
        self.data_progress_bar.set(0)
        self.data_progress_label.configure(text="0%")
        
        range_mode = self.var_cycle_range.get()
        custom_date = self.var_cycle_custom_date.get() if range_mode == 'custom' else None
        
        self.log_to_data_box(f"분석 시작 (범위: {range_mode})...")
        
        def _run():
            try:
                processor = core_cycle.CycleProcessor(
                    log_callback=self.log_to_data_box,
                    progress_callback=self.update_data_progress
                )
                
                if range_mode == 'incremental':
                    processor.run_incremental()
                else:
                    # Full history or date range processing
                    processor.run_range(mode=range_mode, custom_date=custom_date)
                    
            except Exception as e:
                self.log_to_data_box(f"오류 발생: {e}")
                import traceback
                self.log_to_data_box(traceback.format_exc())
            finally:
                self.after(0, lambda: self.btn_run_analysis.configure(state="normal"))
                
        threading.Thread(target=_run, daemon=True).start()
        
    def log_to_data_box(self, msg):
        self.after(0, lambda: self._append_data_msg(msg))
        
    def _append_data_msg(self, msg):
        if hasattr(self, 'data_log_box') and self.data_log_box.winfo_exists():
            self.data_log_box.insert("end", str(msg) + "\n")
            self.data_log_box.see("end")
    
    def update_data_progress(self, value):
        """Update progress bar from background thread"""
        def _update():
            if hasattr(self, 'data_progress_bar') and self.data_progress_bar.winfo_exists():
                self.data_progress_bar.set(value)
                self.data_progress_label.configure(text=f"{int(value * 100)}%")
        self.after(0, _update)

    # --- Logic Adapters ---
    def pick_plc(self):
        d = filedialog.askdirectory()
        if d: self.var_plc.set(d)
        
    # def pick_temp(self):
    #     d = filedialog.askdirectory()
    #     if d: self.var_temp.set(d)


    def on_save(self):
        vals = {
            'SUPABASE_URL': self.var_url.get(),
            'SUPABASE_ANON_KEY': self.var_anon.get(),
            'EDGE_FUNCTION_URL': self.var_edge.get(),
            'PLC_DIR': self.var_plc.get(),
            # 'TEMP_DIR': self.var_temp.get(), # Removed
            'AUTO_UPLOAD': str(self.var_auto_upload.get()).lower(),

            'SMART_SYNC': str(self.var_smart_sync.get()).lower(),
            'RANGE_MODE': self.var_range.get(),
            'CUSTOM_DATE': self.var_custom_date.get(),
            # Defaults for others
            'MTIME_LAG_MIN': self.cfg.get('MTIME_LAG_MIN', '15'),
            'CHECK_LOCK': self.cfg.get('CHECK_LOCK', 'true')
        }
        # Edge URL 기본값 보완
        if not vals['EDGE_FUNCTION_URL']:
            vals['EDGE_FUNCTION_URL'] = compute_edge_url(vals)
        ok_cfg, missing = validate_config(vals)
        if not ok_cfg:
            messagebox.showerror("설정 오류", f"필수 설정이 누락되었습니다: {', '.join(missing)}")
            return
        save_config(vals)
        self.cfg = vals # Update memory
        messagebox.showinfo("저장", "설정이 저장되었습니다.")

    def check_log_queue(self):
        """Check queue for new log messages and update GUI in main thread"""
        if not hasattr(self, 'log_history'):
            self.log_history = []

        try:
            while True:
                msg = self.log_queue.get_nowait()
                
                # Add to history
                self.log_history.append(msg)
                if len(self.log_history) > 2000:
                    self.log_history = self.log_history[-1500:] # Keep last 1500
                
                # Update UI if visible
                if hasattr(self, 'log_box') and self.log_box.winfo_exists():
                    self.log_box.insert("end", msg + "\n")
                    self.log_box.see("end")
                    
                    # Prevent infinite growth in widget (Sync with history size roughly)
                    if float(self.log_box.index("end")) > 2500:
                        self.log_box.delete("1.0", "1000.0")
        except queue.Empty:
            pass
        finally:
            # Schedule next check
            self.after(100, self.check_log_queue)

    def log(self, msg, level="INFO"):
        """Thread-safe log with timestamp/level, shown in GUI and printed to console."""
        ts = kst_now().isoformat(timespec="seconds")
        full = f"[{level.upper()} {ts}] {msg}"
        # Put message in queue (Thread-safe)
        self.log_queue.put(full)
        print(full) # Always print to console

    def update_dashboard_loop(self):
        if not hasattr(self, 'hero_frame') or not self.hero_frame.winfo_exists():
            return # Dashboard not active
            
        # Update Progress
        total = self.total_files if self.total_files > 0 else 1
        pct = self.processed_count / total
        self.prog_bar.set(pct)
        self.lbl_prog_text.configure(text=f"{pct*100:.1f}% ({self.processed_count}/{self.total_files})")
        
        if self.is_uploading:
            if not self.pause_event.is_set():
                self.lbl_big_status.configure(text="Paused", text_color="#E5C07B")
                self.status_label.configure(text="Paused", text_color="#E5C07B")
            else:
                self.lbl_big_status.configure(text="Uploading...", text_color="#3B8ED0")
                self.status_label.configure(text="Running", text_color="#2CC985")
        else:
            self.lbl_big_status.configure(text="Waiting...", text_color="gray")
            self.status_label.configure(text="Idle", text_color="gray")

        # Update Active Tasks List
        with self.progress_lock:
            current_files = set(self.active_progress.keys())
            
            # Remove old
            for fn in list(self.task_labels.keys()):
                if fn not in current_files:
                    self.task_labels[fn].destroy()
                    del self.task_labels[fn]
            
            # Add/Update new
            for fn, p in self.active_progress.items():
                text = f"{fn}: {p:.0f}%"
                if fn not in self.task_labels:
                    lbl = ctk.CTkLabel(self.tasks_frame, text=text, anchor="w")
                    lbl.pack(fill="x", padx=5, pady=2)
                    self.task_labels[fn] = lbl
                else:
                    self.task_labels[fn].configure(text=text)

        self.after(200, self.update_dashboard_loop)

    def on_preview(self):
        self.show_logs()
        self.log("미리보기 시작...")
        # Reuse existing preview logic, just redirect log
        threading.Thread(target=self._run_preview_logic, daemon=True).start()

    def _run_preview_logic(self):
        # Quick adaptation of original preview logic
        vals = self.cfg
        cutoff = compute_cutoff(vals['RANGE_MODE'], vals.get('CUSTOM_DATE', ''))
        items, excluded = preview_diagnostics(vals['PLC_DIR'], vals.get('TEMP_DIR'), cutoff, 15, vals['RANGE_MODE']=='today', True)
        self.log(f"업로드 대상: {len(items)}개 파일")
        for _, fn, _, _ in items[:20]:
            self.log(f" - {fn}")
        if len(items) > 20: self.log("...")
        
        if excluded:
            self.log(f"\n제외된 파일: {len(excluded)}개")
            for _, fn, reason in excluded[:20]:
                self.log(f" [X] {fn}: {reason}")
            if len(excluded) > 20: self.log("...")

    def on_pause(self):
        if not self.is_uploading:
            return
            
        if self.pause_event.is_set():
            # Pause it
            self.pause_event.clear()
            self.btn_pause.configure(text="Resume")
            self.log("일시정지 요청됨...")
        else:
            # Resume it
            self.pause_event.set()
            self.btn_pause.configure(text="Pause")
            self.log("작업 재개됨")

    def on_start(self):
        self.show_dashboard()
        self.is_uploading = True
        self.processed_count = 0
        self.total_files = 0
        with self.progress_lock:
            self.active_progress.clear()
            
        self.pause_event.set()
        self.btn_pause.configure(state="normal", text="일시정지")
        self.btn_start.configure(state="disabled")
            
        threading.Thread(target=self._run_upload, args=(self.cfg,), daemon=True).start()

    def _run_upload(self, vals: dict):
        import concurrent.futures

        ok_cfg, missing = validate_config(vals)
        if not ok_cfg:
            self.log(f"설정 오류: {', '.join(missing)}")
            self.is_uploading = False
            self.btn_pause.configure(state="disabled")
            self.btn_start.configure(state="normal")
            return

        url = vals['SUPABASE_URL'].strip()
        anon = vals['SUPABASE_ANON_KEY'].strip()
        edge = vals['EDGE_FUNCTION_URL'].strip() or compute_edge_url(vals)
        cutoff = compute_cutoff(vals['RANGE_MODE'], vals.get('CUSTOM_DATE', ''))
        include_today = (vals['RANGE_MODE'] == 'today')
        try:
            lag = int(vals.get('MTIME_LAG_MIN', '15'))
        except Exception:
            lag = 15
        check_lock = (vals.get('CHECK_LOCK', 'true') == 'true')
        enable_smart_sync = (vals.get('SMART_SYNC', 'true') == 'true')

        # Simplified: Scan only PLC_DIR (Data Folder)
        # Note: We ignore TEMP_DIR logic
        pdir = vals['PLC_DIR']
        self.log(f"스캔 폴더: {pdir}")
        items = list_candidates(pdir, None, cutoff, lag, include_today, check_lock)
        self.log(f"발견된 파일 수: {len(items)}")
        if not items and os.path.exists(pdir):
             self.log(f"폴더 내 파일: {os.listdir(pdir)[:5]}")
        
        self.total_files = len(items)
        
        if not items:
            self.is_uploading = False
            self.log("업로드 대상 없음 (데이터 폴더를 확인하세요)")
            return

        count_lock = threading.Lock()
        max_workers = 4

        def upload_single_file(item):
            folder, fn, path, kind = item
            key = f'{folder}/{fn}'
            
            # Always use integrated builder
            # kind is technically 'plc' coming from list_candidates
            
            def per_file_cb(done, total):
                if total > 0:
                    p = (done / total) * 100.0
                    with self.progress_lock:
                        self.active_progress[fn] = p

            ok = core_upload.upload_item(
                edge, anon, folder, fn, path, kind,
                build_plc=build_records_plc,
                build_temp=None, # unused
                get_resume_offset=get_resume_offset,
                set_resume_offset_fn=set_resume_offset,
                log_processed_fn=log_processed,
                log=self.log, # Redirect to GUI log
                batch_size=500,
                progress_cb=per_file_cb,
                enable_smart_sync=enable_smart_sync,
                pause_event=self.pause_event
            )
            
            with self.progress_lock:
                if fn in self.active_progress:
                    del self.active_progress[fn]
            return ok, key

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {executor.submit(upload_single_file, item): item for item in items}
            for future in concurrent.futures.as_completed(future_to_file):
                try:
                    ok, key = future.result()
                    with count_lock:
                        self.processed_count += 1
                except Exception as e:
                    self.log(f"Error: {e}")
        
        self.is_uploading = False
        self.log("모든 업로드 완료")
        self.btn_pause.configure(state="disabled")
        self.btn_start.configure(state="normal")


def list_candidates(plc_dir: str, temp_dir: str, cutoff: datetime, lag_min: int, include_today: bool, check_lock: bool):
    # GUI uses quick candidate selection (no content check)
    return core_files.list_candidates(plc_dir, temp_dir, cutoff, lag_min, include_today, check_lock, quick=True)

if __name__ == '__main__':
    import signal
    
    def handle_sigint(signum, frame):
        print("\nForce exiting...")
        os._exit(0)
        
    signal.signal(signal.SIGINT, handle_sigint)
    App().mainloop()

