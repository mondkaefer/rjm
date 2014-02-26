rjm_batch_submit.exe -c "bash script.sh" -m 1G -j serial -w 00:01:00 -f localdirs.txt
rjm_batch_wait.exe -f localdirs.txt -z 5 

