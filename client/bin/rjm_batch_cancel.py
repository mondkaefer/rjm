import os
import sys
import time
import argparse
import traceback
import cer.client.ssh as ssh
import cer.client.job as job
import cer.client.util as util
import cer.client.util.config as config
from cer.client.util import Retry

def cleanup():
  if ssh_conn:
    try:
      ssh.close_connection(ssh_conn)
    except:
      pass

# name of the file that contains the list of files to be downloaded after a job
ssh_conn = None
sftp_conn = None

# information displayed as help by argparse
h = {
  'logfile':
    'logfile. if not specified, all messages will be printed to the terminal.', 
  'loglevel':
    'level of log verbosity. default: %s. ' % util.DEFAULT_LOG_LEVEL.lower() +
    'the higher the log level, more information will be printed.',
  'localjobdirfile':
    'file that contains the names of the local job directories, one name per line.',
  'pollingintervalsec':
    'number of seconds to wait between each check for status of the cancellation.',
}

parser = argparse.ArgumentParser(description='cancel a batch of jobs and wait for the cancellation to complete.')
parser.add_argument('-f','--localjobdirfile', help=h['localjobdirfile'], required=True, type=str)
parser.add_argument('-l','--logfile', help=h['logfile'], required=False, type=str)
parser.add_argument('-ll','--loglevel', help=h['loglevel'], required=False, type=str, choices=['debug','info','warn','error','critical'])
parser.add_argument('-z','--pollingintervalsec', help=h['pollingintervalsec'], required=True, type=int)
args = parser.parse_args()

if args.logfile or args.loglevel:
  util.setup_logging(args.logfile, args.loglevel)
log = util.get_log()

# read central configuration file
try:
  conf = config.get_config()
except:
  log.critical('failed to read config file %s' % config.get_config_file())
  cleanup()
  sys.exit(1)

@Retry(conf['RETRY']['max_attempts'], conf['RETRY']['min_wait_s'], conf['RETRY']['max_wait_s'])
def get_local_job_directories(localjobdirfile):
  ''' get the list of local job directories from file. '''
  return util.get_local_job_directories(localjobdirfile)

@Retry(conf['RETRY']['max_attempts'], conf['RETRY']['min_wait_s'], conf['RETRY']['max_wait_s'])
def read_job_config_file(job_config_file):
  ''' read the local configuration file of a job (ini-format). '''
  return config.read_job_config_file(job_config_file)

@Retry(conf['RETRY']['max_attempts'], conf['RETRY']['min_wait_s'], conf['RETRY']['max_wait_s'])
def cancel_jobs(ssh_conn, jobids):
  ''' cancel jobs '''
  job.cancel_jobs(ssh_conn, jobids)
  
# read local job directories from file
try:
  localdirs = get_local_job_directories(args.localjobdirfile)
except:
  log.critical('failed to read list of local job directories or invalid entries in list')
  cleanup()
  sys.exit(1)

# stop if no job directories are listed
if len(localdirs) == 0:
  log.info('no job directories. nothing to cancel. exiting.')
  cleanup()
  sys.exit(0)
  
jobids = []

# read the configuration file for each job
# the configuration files contains the job id and the remote job directory
for localdir in localdirs:
  job_config_file = '%s%s.job.ini' % (localdir, os.path.sep)
  try:
    job_config = read_job_config_file(job_config_file)
    if 'id' in job_config['JOB']:
      jobids.append(job_config['JOB']['id'])
    else:
      log.warn('no job id for local directory %s. probably job has not started yet. skipping job.' % localdir)
  except:
    log.warn('failed to read job config file %s. skipping job.' % job_config_file)

# cancel jobs
# set up SSH connection
try:
  ssh_conn = ssh.open_connection_ssh_agent(conf['CLUSTER']['remote_host'], conf['CLUSTER']['remote_user'], conf['CLUSTER']['ssh_priv_key_file'])
except:
  log.critical('failed to set up ssh connection')
  log.critical(traceback.format_exc())
  cleanup()
  sys.exit(1)

cancel_jobs(ssh_conn, jobids)

try:
  ssh.close_connection(ssh_conn)
except:
  pass 

log.info('waiting for jobs to be cancelled (polling every %s seconds)...' % args.pollingintervalsec)
while True:
  try:
    log.info('waiting for cancellation of %s jobs...' % len(jobids))
    error_occured = False
    jobids_new = []
    ssh_conn = ssh.open_connection_ssh_agent(conf['CLUSTER']['remote_host'], conf['CLUSTER']['remote_user'], conf['CLUSTER']['ssh_priv_key_file'])
    log.debug('getting job statuses')
    jobmap = job.get_job_statuses(ssh_conn)
    for jobid in jobids:
      if jobid not in jobmap:
        log.info('job %s was cancelled.' % jobid)
      else:
        jobids_new.append(jobid)
  except:
    error_occured = True
    log.warn('failed to get status. only critical if happens repeatedly. %s' % traceback.format_exc().strip())
  finally:
    try:
      ssh.close_connection(ssh_conn)
    except:
      pass
    
  if len(jobids_new) > 0:
    jobids = jobids_new
  else:
    if not error_occured:
      break

  time.sleep(args.pollingintervalsec)

cleanup()
