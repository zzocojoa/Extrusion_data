from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.build_training_base import build_training_base
from scripts.build_training_dataset_v1 import build_training_dataset_v1
from scripts.verify_training_base import DEFAULT_SAMPLE_FILE, verify_training_base
from scripts.verify_training_dataset_v1 import verify_training_dataset_v1

MODE_OPTIONS: tuple[str, ...] = (
    "build-all",
    "build-base",
    "build-v1",
    "verify-base",
    "verify-v1",
)

MODE_HELP: dict[str, str] = {
    "build-all": "raw CSV를 읽어 training_base와 training_dataset_v1을 순서대로 생성합니다.",
    "build-base": "raw CSV를 읽어 training_base.parquet만 생성합니다.",
    "build-v1": "training_base.parquet를 읽어 training_dataset_v1.parquet만 생성합니다.",
    "verify-base": "integrated log CSV로 training_base 회귀 검증을 수행합니다.",
    "verify-v1": "training_base.parquet로 training_dataset_v1 회귀 검증을 수행합니다.",
}


@dataclass(frozen=True)
class TrainingGuiRequest:
    mode: str
    plc_file: str
    spot_file: str
    training_base_file: str
    base_output_path: str
    dataset_output_path: str
    verify_output_path: str
    filename_hint: str


def normalize_optional_path(value: str) -> Path | None:
    stripped_value = value.strip()
    if stripped_value == "":
        return None
    return Path(stripped_value).resolve()


def normalize_required_path(value: str, field_name: str) -> Path:
    normalized_path = normalize_optional_path(value)
    if normalized_path is None:
        raise ValueError(f"{field_name}을(를) 선택해야 합니다.")
    return normalized_path


def normalize_filename_hint(value: str, plc_file_path: Path | None) -> str:
    stripped_value = value.strip()
    if stripped_value != "":
        return stripped_value
    if plc_file_path is None:
        raise ValueError("파일명 힌트를 결정할 수 없습니다.")
    return plc_file_path.name


def run_training_task(request: TrainingGuiRequest) -> str:
    if request.mode not in MODE_OPTIONS:
        raise ValueError(f"지원하지 않는 모드입니다: {request.mode}")

    if request.mode == "build-all":
        plc_file_path = normalize_required_path(request.plc_file, "PLC CSV")
        base_output_path = normalize_required_path(
            request.base_output_path,
            "training_base 출력 경로",
        )
        dataset_output_path = normalize_required_path(
            request.dataset_output_path,
            "training_dataset_v1 출력 경로",
        )
        spot_file_path = normalize_optional_path(request.spot_file)
        filename_hint = normalize_filename_hint(request.filename_hint, plc_file_path)
        build_training_base(plc_file_path, base_output_path, filename_hint, spot_file_path)
        build_training_dataset_v1(base_output_path, dataset_output_path)
        return (
            "build-all 완료\n"
            f"- training_base: {base_output_path}\n"
            f"- training_dataset_v1: {dataset_output_path}"
        )

    if request.mode == "build-base":
        plc_file_path = normalize_required_path(request.plc_file, "PLC CSV")
        base_output_path = normalize_required_path(
            request.base_output_path,
            "training_base 출력 경로",
        )
        spot_file_path = normalize_optional_path(request.spot_file)
        filename_hint = normalize_filename_hint(request.filename_hint, plc_file_path)
        build_training_base(plc_file_path, base_output_path, filename_hint, spot_file_path)
        return f"build-base 완료\n- training_base: {base_output_path}"

    if request.mode == "build-v1":
        training_base_file_path = normalize_required_path(
            request.training_base_file,
            "training_base.parquet",
        )
        dataset_output_path = normalize_required_path(
            request.dataset_output_path,
            "training_dataset_v1 출력 경로",
        )
        build_training_dataset_v1(training_base_file_path, dataset_output_path)
        return f"build-v1 완료\n- training_dataset_v1: {dataset_output_path}"

    if request.mode == "verify-base":
        plc_file_path = (
            normalize_optional_path(request.plc_file)
            or DEFAULT_SAMPLE_FILE.resolve()
        )
        verify_output_path = normalize_optional_path(request.verify_output_path)
        exit_code = verify_training_base(plc_file_path, verify_output_path)
        if exit_code != 0:
            raise RuntimeError("verify-base가 실패했습니다.")
        if verify_output_path is None:
            return f"verify-base 완료\n- sample: {plc_file_path}"
        return (
            "verify-base 완료\n"
            f"- sample: {plc_file_path}\n"
            f"- output: {verify_output_path}"
        )

    training_base_file_path = normalize_required_path(
        request.training_base_file,
        "training_base.parquet",
    )
    verify_output_path = normalize_optional_path(request.verify_output_path)
    exit_code = verify_training_dataset_v1(training_base_file_path, verify_output_path)
    if exit_code != 0:
        raise RuntimeError("verify-v1이 실패했습니다.")
    if verify_output_path is None:
        return f"verify-v1 완료\n- training_base: {training_base_file_path}"
    return (
        "verify-v1 완료\n"
        f"- training_base: {training_base_file_path}\n"
        f"- output: {verify_output_path}"
    )


def choose_open_file(variable: tk.StringVar, file_types: list[tuple[str, str]]) -> None:
    selected_path = filedialog.askopenfilename(
        title="파일 선택",
        filetypes=file_types,
    )
    if selected_path != "":
        variable.set(selected_path)


def choose_save_file(variable: tk.StringVar, default_extension: str) -> None:
    selected_path = filedialog.asksaveasfilename(
        title="저장 경로 선택",
        defaultextension=default_extension,
        filetypes=[("Parquet files", "*.parquet"), ("All files", "*.*")],
    )
    if selected_path != "":
        variable.set(selected_path)


def set_default_base_output(
    plc_variable: tk.StringVar,
    base_output_variable: tk.StringVar,
) -> None:
    plc_value = plc_variable.get().strip()
    if plc_value == "":
        return
    plc_path = Path(plc_value)
    default_path = plc_path.with_name(f"{plc_path.stem}_training_base.parquet")
    base_output_variable.set(str(default_path))


def set_default_dataset_output(
    source_variable: tk.StringVar,
    dataset_output_variable: tk.StringVar,
) -> None:
    source_value = source_variable.get().strip()
    if source_value == "":
        return
    source_path = Path(source_value)
    default_path = source_path.with_name(f"{source_path.stem}_training_dataset_v1.parquet")
    dataset_output_variable.set(str(default_path))


def update_mode_help(mode_variable: tk.StringVar, help_variable: tk.StringVar) -> None:
    help_variable.set(MODE_HELP[mode_variable.get()])


def run_task_async(
    request: TrainingGuiRequest,
    run_button: ttk.Button,
    status_variable: tk.StringVar,
) -> None:
    def worker() -> None:
        try:
            result_message = run_training_task(request)
        except Exception as error:
            def show_error() -> None:
                run_button.config(state=tk.NORMAL)
                status_variable.set("실패")
                messagebox.showerror("TrainingDataBuilder", str(error))

            run_button.after(0, show_error)
            return

        def show_success() -> None:
            run_button.config(state=tk.NORMAL)
            status_variable.set("완료")
            messagebox.showinfo("TrainingDataBuilder", result_message)

        run_button.after(0, show_success)

    run_button.config(state=tk.DISABLED)
    status_variable.set("실행 중...")
    thread = threading.Thread(target=worker, daemon=True)
    thread.start()


def build_request(
    mode_variable: tk.StringVar,
    plc_variable: tk.StringVar,
    spot_variable: tk.StringVar,
    training_base_variable: tk.StringVar,
    base_output_variable: tk.StringVar,
    dataset_output_variable: tk.StringVar,
    verify_output_variable: tk.StringVar,
    filename_hint_variable: tk.StringVar,
) -> TrainingGuiRequest:
    return TrainingGuiRequest(
        mode=mode_variable.get(),
        plc_file=plc_variable.get(),
        spot_file=spot_variable.get(),
        training_base_file=training_base_variable.get(),
        base_output_path=base_output_variable.get(),
        dataset_output_path=dataset_output_variable.get(),
        verify_output_path=verify_output_variable.get(),
        filename_hint=filename_hint_variable.get(),
    )


def create_label(frame: ttk.Frame, row_index: int, text: str) -> None:
    label = ttk.Label(frame, text=text)
    label.grid(row=row_index, column=0, sticky="w", padx=8, pady=6)


def create_entry(frame: ttk.Frame, row_index: int, variable: tk.StringVar) -> ttk.Entry:
    entry = ttk.Entry(frame, textvariable=variable, width=70)
    entry.grid(row=row_index, column=1, sticky="ew", padx=8, pady=6)
    return entry


def create_open_button(
    frame: ttk.Frame,
    row_index: int,
    variable: tk.StringVar,
    file_types: list[tuple[str, str]],
) -> None:
    button = ttk.Button(
        frame,
        text="선택",
        command=lambda: choose_open_file(variable, file_types),
    )
    button.grid(row=row_index, column=2, sticky="ew", padx=8, pady=6)


def create_save_button(
    frame: ttk.Frame,
    row_index: int,
    variable: tk.StringVar,
) -> None:
    button = ttk.Button(
        frame,
        text="저장 위치",
        command=lambda: choose_save_file(variable, ".parquet"),
    )
    button.grid(row=row_index, column=2, sticky="ew", padx=8, pady=6)


def build_gui() -> tk.Tk:
    root = tk.Tk()
    root.title("TrainingDataBuilder")
    root.geometry("920x430")

    container = ttk.Frame(root, padding=16)
    container.pack(fill=tk.BOTH, expand=True)
    container.columnconfigure(1, weight=1)

    mode_variable = tk.StringVar(value="build-all")
    help_variable = tk.StringVar(value=MODE_HELP["build-all"])
    plc_variable = tk.StringVar()
    spot_variable = tk.StringVar()
    training_base_variable = tk.StringVar()
    base_output_variable = tk.StringVar()
    dataset_output_variable = tk.StringVar()
    verify_output_variable = tk.StringVar()
    filename_hint_variable = tk.StringVar()
    status_variable = tk.StringVar(value="대기")

    create_label(container, 0, "모드")
    mode_combo = ttk.Combobox(
        container,
        textvariable=mode_variable,
        values=list(MODE_OPTIONS),
        state="readonly",
    )
    mode_combo.grid(row=0, column=1, sticky="ew", padx=8, pady=6)
    ttk.Label(container, textvariable=help_variable).grid(
        row=1,
        column=1,
        columnspan=2,
        sticky="w",
        padx=8,
        pady=(0, 10),
    )

    create_label(container, 2, "PLC CSV")
    create_entry(container, 2, plc_variable)
    create_open_button(container, 2, plc_variable, [("CSV files", "*.csv")])

    create_label(container, 3, "SPOT CSV")
    create_entry(container, 3, spot_variable)
    create_open_button(container, 3, spot_variable, [("CSV files", "*.csv")])

    create_label(container, 4, "training_base.parquet")
    create_entry(container, 4, training_base_variable)
    create_open_button(container, 4, training_base_variable, [("Parquet files", "*.parquet")])

    create_label(container, 5, "training_base 출력")
    create_entry(container, 5, base_output_variable)
    create_save_button(container, 5, base_output_variable)

    create_label(container, 6, "training_dataset_v1 출력")
    create_entry(container, 6, dataset_output_variable)
    create_save_button(container, 6, dataset_output_variable)

    create_label(container, 7, "검증 parquet 출력")
    create_entry(container, 7, verify_output_variable)
    create_save_button(container, 7, verify_output_variable)

    create_label(container, 8, "파일명 힌트")
    create_entry(container, 8, filename_hint_variable)

    action_frame = ttk.Frame(container)
    action_frame.grid(row=9, column=0, columnspan=3, sticky="ew", padx=8, pady=(16, 0))
    action_frame.columnconfigure(1, weight=1)

    ttk.Label(action_frame, textvariable=status_variable).grid(
        row=0,
        column=0,
        sticky="w",
        padx=(0, 12),
    )

    run_button = ttk.Button(action_frame, text="실행")
    run_button.grid(row=0, column=2, sticky="e")

    ttk.Button(
        action_frame,
        text="PLC 기준 기본 출력 채우기",
        command=lambda: set_default_base_output(plc_variable, base_output_variable),
    ).grid(row=0, column=3, sticky="e", padx=(8, 0))

    ttk.Button(
        action_frame,
        text="base 기준 v1 출력 채우기",
        command=lambda: set_default_dataset_output(training_base_variable, dataset_output_variable),
    ).grid(row=0, column=4, sticky="e", padx=(8, 0))

    def handle_run() -> None:
        request = build_request(
            mode_variable,
            plc_variable,
            spot_variable,
            training_base_variable,
            base_output_variable,
            dataset_output_variable,
            verify_output_variable,
            filename_hint_variable,
        )
        run_task_async(request, run_button, status_variable)

    run_button.config(command=handle_run)
    mode_combo.bind(
        "<<ComboboxSelected>>",
        lambda _event: update_mode_help(mode_variable, help_variable),
    )
    return root


def main() -> None:
    root = build_gui()
    root.mainloop()


if __name__ == "__main__":
    main()
