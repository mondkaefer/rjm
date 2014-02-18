cd client\lib
python setup.py install
python setup.py install
cd ..\..
cd client\bin
pyinstaller -F rjm_autenticate.py
pyinstaller -F rjm_batch_cancel.py
pyinstaller -F rjm_batch_clean.py
pyinstaller -F rjm_batch_submit.py
pyinstaller -F rjm_batch_wait.py
pyinstaller -F rjm_configure.py
