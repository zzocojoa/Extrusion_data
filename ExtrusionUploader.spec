# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path

from PyInstaller.utils.hooks import collect_data_files


PROJECT_ROOT = Path(globals().get("SPECPATH", Path.cwd())).resolve()
ASSETS_DIR = PROJECT_ROOT / "assets"
I18N_DIR = ASSETS_DIR / "i18n"


def _required_data_file(source_path: Path, target_dir: str) -> tuple[str, str]:
    if not source_path.is_file():
        raise FileNotFoundError(f"필수 번들 자산을 찾을 수 없습니다: {source_path}")
    return (str(source_path), target_dir)


def _collect_i18n_datas() -> list[tuple[str, str]]:
    if not I18N_DIR.exists():
        return []

    locale_files = sorted(path for path in I18N_DIR.glob("*.json") if path.is_file())
    if not locale_files:
        raise FileNotFoundError(f"locale JSON 파일이 없습니다: {I18N_DIR}")

    return [(str(path), "assets/i18n") for path in locale_files]

datas = []
datas += collect_data_files('certifi')
datas += collect_data_files('pandas')
datas += collect_data_files('numpy')
datas += collect_data_files('numpy')
datas += collect_data_files('pyarrow')
datas += collect_data_files('customtkinter')
datas += [
    _required_data_file(ASSETS_DIR / "app.ico", "assets"),
    _required_data_file(ASSETS_DIR / "logo.png", "assets"),
]
datas += _collect_i18n_datas()

hiddenimports = []
hiddenimports += [
    'pyarrow',
    'pyarrow.lib',
    'pyarrow.parquet',
    'customtkinter',
    'PIL',
    'PIL.Image',
    'packaging',
    'jaraco.text',
    'jaraco.context',
    'psycopg2',
    'core.training_base',
    'core.training_dataset_v1',
    'scripts.build_training_base',
    'scripts.build_training_dataset_v1',
]


block_cipher = None


a = Analysis(
    ['uploader_gui_tk.py'],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # Jupyter / IPython
        'IPython',
        'jupyter',
        'notebook',
        'nbformat',
        'nbconvert',
        'jupyter_client',
        'jupyter_core',
        'traitlets',
        'tornado',
        # ML / scientific stacks not used by this app
        'tensorflow',
        'keras',
        'torch',
        'torchvision',
        'torchtext',
        'scipy',
        'sympy',
        'sklearn',
        # Visualization / imaging
        'matplotlib',
        'matplotlib',
        'imageio',
        # Testing frameworks
        'pytest',
        'hypothesis',
        'pyarrow.tests',
        'pyarrow.conftest',
        'pyarrow._pyarrow_cpp_tests',
        'pyarrow.benchmark',
        'pyarrow.cffi',
        'pyarrow.jvm',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

splash = Splash(
    'assets/logo.png',
    binaries=a.binaries,
    datas=a.datas,
    text_pos=None,
    text_size=12,
    minify_script=True,
    always_on_top=True,
)

exe = EXE(
    pyz,
    a.scripts,
    splash,
    splash.binaries,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='ExtrusionUploader',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['assets\\app.ico'],
)
