import os
import sys
import time
import argparse
import traceback
import ConfigParser
import cer.client.ssh as ssh
import cer.client.job as job
import cer.client.util as util

logfile = None
ssh_conn = None
sftp_conn = None

h = {
  'logfile':
    'Logfile. If not specified, all messages will be printed to stdout.', 
  'localjobdirfile':
    'File that contains the names of the local job directories, one name per line.',
  'waitforcancellation':
    'Wait for the jobs to have disappeared from the batch scheduling system.',
  'pollingintervalsec':
    'Number of seconds to wait between polling for job status. Only used when the waiting for cancellation.',
}

def cleanup():
  if logfile:
    try:
      logfile.close()
    except:
      pass

parser = argparse.ArgumentParser(description='')
parser.add_argument('-f','--localjobdirfile', help=h['localjobdirfile'], required=True, type=str)
parser.add_argument('-l','--logfile', help=h['logfile'], required=False, type=str)
parser.add_argument('-w','--wait', help=h['waitforcancellation'], action='store_true')
parser.add_argument('-p','--pollingintervalsec', help=h['pollingintervalsec'], required=False, type=int, default=15)
args = parser.parse_args()

if args.logfile:
  logfile = open(args.logfile,"w")
  sys.stdout = logfile

# Check existence of localjobdirfile and local job directories
if not os.path.isfile(args.localjobdirfile):
  print 'Error: File that contains local job directories does not exist: %s' % args.localjobdirfile
  sys.exit(1)

localdirs = [line.strip() for line in open(args.localjobdirfile)]
jobids = []
parser = ConfigParser.SafeConfigParser();

for localdir in localdirs:
  job_ini_file = '%s%s.job.ini' % (localdir, os.path.sep)
  if not os.path.isdir(localdir):
    print 'Error: File that contains local job directories does not exist: %s' % localdir
    sys.exit(1)
  if not os.path.isfile(job_ini_file):
    print 'Error: No ini-file %s for job found.' % job_ini_file
    sys.exit(1)
  # Set up dict with information about each job
  parser.read(job_ini_file)
  job_id = parser.get('JOB', 'id')
  jobids.append(job_id)

# Read connection parameters
try:
  parser = util.get_config_parser()
  host = parser.get('MAIN', 'remote_host')
  user = parser.get('MAIN', 'remote_user')
  privkey = parser.get('MAIN', 'ssh_priv_key_file')
except:
  print 'Error: Failed to read connection parameters'
  print traceback.format_exc()
  cleanup()
  sys.exit(1)

try:
  # Set up SSH connection
  ssh_conn = ssh.open_connection_ssh_agent(host, user, privkey)
  job.cancel_jobs(ssh_conn, jobids)
except:
  print 'Error: Failed to cancel jobs.'
  print traceback.format_exc()
  sys.exit(1) 
  
try:
  ssh.close_connection(ssh_conn)
except:
  pass
  
if args.wait:
  print 'Waiting for cancellation to finish (polling every %s seconds)...' % args.pollingintervalsec
  while True:
    try:
      # Set up SSH connection
      ssh_conn = ssh.open_connection_ssh_agent(host, user, privkey)
      jobids_new = {}
      jobmap = job.get_job_statuses(ssh_conn)
      ssh.close_connection(ssh_conn)
      for jobid in jobids:
        if jobid not in jobmap:
          print 'Job %s successfully cancelled.' % job_id
        else:
          jobids_new[jobid] = jobids[jobid]
    except:
      print 'Error: Failed to get job status.'
      print traceback.format_exc()
    if len(jobids_new) == 0:
      break
    time.sleep(args.pollingintervalsec)
    jobids = jobids_new
    
cleanup()
