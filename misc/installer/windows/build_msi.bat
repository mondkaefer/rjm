candle.exe CeRToolkit.wxs CeRToolkit_InstallDirDlg.wxs CeRToolkit_WixUI_InstallDir.wxs
light.exe -ext WixUIExtension^
          -b exe_path=..\..\..\client\bin\dist^
          -b doc_path=..\..\..\doc^
          -b deps_path=.\deps^
          -dWixUIBannerBmp=bitmaps\banner.bmp^
          -dWixUIDialogBmp=bitmaps\dialog.bmp^
          -o CeRToolkit.msi^
          CeRToolkit_InstallDirDlg.wixobj CeRToolkit_WixUI_InstallDir.wixobj CeRToolkit.wixobj
