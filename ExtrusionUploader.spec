# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_data_files

datas = []
datas += collect_data_files('certifi')
datas += collect_data_files('pandas')
datas += collect_data_files('numpy')
datas += collect_data_files('numpy')
datas += collect_data_files('customtkinter')
datas += [('assets', 'assets')]


block_cipher = None


a = Analysis(
    ['uploader_gui_tk.py'],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=['customtkinter', 'PIL', 'PIL.Image', 'packaging', 'jaraco.text', 'jaraco.classes', 'jaraco.context'],
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
