import os
import sys
import time
import argparse
import traceback
import cer.client.ssh as ssh
import cer.client.job as job
import cer.client.util as util

ssh_conn = None

# Set up SSH connection
try:
  parser = util.get_config_parser()
  host = parser.get('MAIN', 'remote_host')
  user = parser.get('MAIN', 'remote_user')
  privkey = parser.get('MAIN', 'ssh_priv_key_file')
  ssh_conn = ssh.open_connection_ssh_agent(host, user, privkey)
except:
  print >> sys.stderr, "Error: Failed to set up SSH connection"
  print >> sys.stderr, 'Details:'
  print >> sys.stderr, traceback.format_exc()
  sys.exit(1)
  
# Call remote script to prepare the job
jobmap = {}
try:
  jobmap = job.get_job_statuses(ssh_conn)
except:
  print >> sys.stderr, "Error: Remote command to prepare job failed."
  print >> sys.stderr, traceback.format_exc()
  sys.exit(1)

keys = jobmap.keys()

for key in jobmap.keys():
  print jobmap[key]
  
try:
  ssh.close_connection(ssh_conn)
except:
  pass

