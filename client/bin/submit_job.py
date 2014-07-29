import os
import sys
import time
import argparse
import traceback
import cer.client.ssh as ssh
import cer.client.job as job
import cer.client.util as util

h = {
  'cmd': 
    'Command to run.',
  'basedir':
    'Base directory of the job where the job directory for this job will be created. ' +
    'The value of the parameter jobname postfixed with the current date and time will ' +
    'be used to create the job directory.',
  'module':
    'Module to be loaded prior to job execution. Multiple module parameters can be specified',
  'inputfile' :
    'Input file to be uploaded prior to job execution.',
  'memgb':
    'Amount of memory [GigaBytes] required by this job.',
  'jobname':
    'Name of the job. The value of the parameter jobname postfixed with the current date and time will ' +
    'be used to create the job directory.',
  'pcode': 
    'Project code this job is run under, e.g. uoa00042',
  'queue': 
    'Queue to be used for this job',
  'jobtype':
    'Type of the job. Like serial serial:5, mpi:4, mpi:5:4, mpich:6',
  'walltimeh':
    'Wall clock time this job will run for.',
  'extension':
    'Additional scheduler directive',
  'pollingintervalsec':
    'Number of seconds to wait between polling for job status.',
}

ssh_conn = None
sftp_conn = None

def cleanup():
  if ssh_conn:
    try:
      ssh.close_connection(ssh_conn)
    except:
      pass

parser = argparse.ArgumentParser(description='')
parser.add_argument('-c','--cmd', help=h['cmd'], required=True, type=str, action='append')
parser.add_argument('-d','--basedir', help=h['basedir'], required=True, type=str)
parser.add_argument('-e','--module', help=h['module'], required=False, type=str, action='append')
parser.add_argument('-i','--inputfile', help=h['inputfile'], required=False, action='append')
parser.add_argument('-m','--memgb', help=h['memgb'], required=True, type=int)
parser.add_argument('-n','--jobname', help=h['jobname'], required=False, type=str, default='job')
parser.add_argument('-j','--jobtype', help=h['jobtype'], required=True, type=str)
parser.add_argument('-p','--pcode', help=h['pcode'], required=False, type=str)
parser.add_argument('-q','--queue', help=h['queue'], required=False, type=str)
parser.add_argument('-w','--walltimeh', help=h['walltimeh'], required=True, type=int)
parser.add_argument('-x','--extension', help=h['extension'], required=False, type=str, action='append')
parser.add_argument('-z','--pollingintervalsec', help=h['pollingintervalsec'], required=False, type=int)

args = parser.parse_args()

# Check existence of input files
if args.inputfile:
  for f in args.inputfile:
    if not os.path.exists(f):
      print >> sys.stderr, 'Error: Input file %s does not exist' % f
      sys.exit(1)
    elif not os.path.isfile(f):
      print >> sys.stderr, 'Error: Input file %s is not a file' % f
      sys.exit(1)
      
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
  cleanup()
  sys.exit(1)
  
# Call remote script to prepare the job
try:
  jobdir, jobscript = job.prepare_job(ssh_conn, args.basedir, args.jobname, args.cmd, args.module,
    args.memgb, args.walltimeh, args.jobtype)
  print 'Created job directory %s' % jobdir
except:
  print >> sys.stderr, "Error: Remote command to prepare job failed."
  print >> sys.stderr, traceback.format_exc()
  cleanup()
  sys.exit(1)
  
# Upload input files if any
if args.inputfile:
  try:
    sftp = ssh_conn.open_sftp()
    for f in args.inputfile:
      print 'Uploading file %s to job directory' % f
      sftp.put(f, '%s/%s' % (jobdir, os.path.basename(f)))
  except:
    print 'Failed to upload file %s' % f
    print >> sys.stderr, traceback.format_exc()
    cleanup()
    sys.exit(1)
  
# Submit job
try:
  print 'Submitting job...'  
  jobid = job.submit_job(ssh_conn, jobscript)
  print 'Job ID: %s ' % jobid
except:
  print >> sys.stderr, "Error: Failed to submit job."
  print >> sys.stderr, traceback.format_exc()
  cleanup()
  sys.exit(1)

try:
  ssh.close_connection(ssh_conn)
except:
  pass

# Wait for job to finish
if args.pollingintervalsec:
  finished = False
  print 'Waiting for job to finish (polling every %s seconds)...' % args.pollingintervalsec
  time.sleep(args.pollingintervalsec)
  while True:
    try:
      # Set up SSH connection
      ssh_conn = ssh.open_connection_ssh_agent(host, user, privkey)
      finished = job.has_finished(ssh_conn, jobid)
      ssh.close_connection(ssh_conn)
    except:
      print >> sys.stderr, "Error: Failed to get status of job."
      print >> sys.stderr, traceback.format_exc()
    if finished:
      break
    time.sleep(args.pollingintervalsec)
  print 'Job finished. Exiting'
