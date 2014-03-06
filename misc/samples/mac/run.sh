python rjm_batch_submit.py -c "bash script.sh" -m 1G -j serial -w 00:05:00 -f localdirs.txt
python rjm_batch_wait.py -f localdirs.txt -z 5
python rjm_batch_clean.py -f localdirs.txt

