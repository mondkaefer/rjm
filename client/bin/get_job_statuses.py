import sys
import traceback
import cer.client.ssh as ssh
import cer.client.job as job
import cer.client.util.config as config

ssh_conn = None

# Set up SSH connection
try:
  conf = config.get_config()
  cluster = conf['CLUSTER']
  ssh_conn = ssh.open_connection_ssh_agent(cluster['remote_host'], cluster['remote_user'], cluster['ssh_priv_key_file'])
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

