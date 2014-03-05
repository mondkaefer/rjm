rjm_batch_submit -c "bash script.sh" -m 1G -j serial -w 00:01:00 -f localdirs.txt
rjm_batch_wait -f localdirs.txt -z 5 
rjm_batch_clean -f localdirs.txt
