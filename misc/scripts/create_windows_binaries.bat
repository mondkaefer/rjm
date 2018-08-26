REM Create Windows binaries from Python modules
REM This script must be invoked in the top directory of the git repository
REM Prerequisites:
REM  * pyinstaller 

cd client\lib
python setup.py install
python setup.py install

cd ..\..
cd client\bin
pyinstaller -F rjm_authenticate.py
pyinstaller -F rjm_configure.py
pyinstaller -F rjm_batch_submit.py
pyinstaller -F rjm_batch_wait.py
pyinstaller -F rjm_batch_cancel.py
pyinstaller -F rjm_batch_clean.py
pyinstaller -F run_remote.py
