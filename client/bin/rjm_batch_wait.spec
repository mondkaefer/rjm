# -*- mode: python -*-

block_cipher = None


a = Analysis(['rjm_batch_wait.py'],
             pathex=['/Users/martin/programming/python/rjm/client/bin'],
             binaries=[],
             datas=[],
             hiddenimports=[],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          name='rjm_batch_wait',
          debug=False,
          strip=False,
          upx=False,
          runtime_tmpdir=None,
          console=True )
