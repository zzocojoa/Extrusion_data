# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_data_files

datas = []
datas += collect_data_files('certifi')
datas += collect_data_files('pandas')
datas += collect_data_files('numpy')
datas += collect_data_files('pyarrow')

hiddenimports = []
hiddenimports += [
    'pyarrow',
    'pyarrow.lib',
    'pyarrow.parquet',
]

block_cipher = None


a = Analysis(
    ['scripts/training_data_builder_gui.py'],
    pathex=['.'],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'IPython',
        'jupyter',
        'notebook',
        'nbformat',
        'nbconvert',
        'jupyter_client',
        'jupyter_core',
        'traitlets',
        'tornado',
        'tensorflow',
        'keras',
        'torch',
        'torchvision',
        'torchtext',
        'scipy',
        'sympy',
        'sklearn',
        'matplotlib',
        'imageio',
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

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='TrainingDataBuilder',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    upx_exclude=[],
    runtime_tmpdir='.',
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['assets\\app.ico'],
)
