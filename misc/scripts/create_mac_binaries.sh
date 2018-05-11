#!/usr/bin/env bash

# Create Mac binaries from Python modules
# This script must be invoked in the top directory of the git repository
# Prerequisites:
#  * pyinstaller 

cd client/lib
python3 setup.py install
python3 setup.py install

cd ../..
cd client/bin
pyinstaller -F --clean --noupx rjm_authenticate.py
pyinstaller -F --clean --noupx rjm_batch_cancel.py
pyinstaller -F --clean --noupx rjm_batch_clean.py
pyinstaller -F --clean --noupx rjm_batch_submit.py
pyinstaller -F --clean --noupx rjm_batch_wait.py
pyinstaller -F --clean --noupx rjm_configure.py
pyinstaller -F --clean --noupx run_remote.py
