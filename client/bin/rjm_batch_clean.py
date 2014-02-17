import os
import sys
import argparse
import traceback
import cer.client.ssh as ssh
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
    'Logfile. If not specified, all messages will be printed to stdout.', 
  'loglevel':
    'Loglevel. Default: %s' % util.DEFAULT_LOG_LEVEL.lower(),
  'localjobdirfile':
    'File that contains the names of the local job directories, one name per line.',
}

parser = argparse.ArgumentParser(description='')
parser.add_argument('-f','--localjobdirfile', help=h['localjobdirfile'], required=True, type=str)
parser.add_argument('-l','--logfile', help=h['logfile'], required=False, type=str)
parser.add_argument('-ll','--loglevel', help=h['loglevel'], required=False, type=str, choices=['debug','info','warn','error','critical'])
args = parser.parse_args()

if args.logfile or args.loglevel:
  util.setup_logging(args.logfile, args.loglevel)
log = util.get_log()

# read central configuration file
try:
  conf = config.get_config()
  cluster = conf['CLUSTER']
  retry = conf['RETRY']
except:
  log.critical('failed to read config file %s' % config.get_config_file())
  cleanup()
  sys.exit(1)

@Retry(retry['max_attempts'], retry['min_wait_s'], retry['max_wait_s'])
def get_local_job_directories(localjobdirfile):
  ''' get the list of local job directories from file. '''
  return util.get_local_job_directories(localjobdirfile)

@Retry(retry['max_attempts'], retry['min_wait_s'], retry['max_wait_s'])
def read_job_config_file(job_config_file):
  ''' read the local configuration file of a job (ini-format). '''
  return config.read_job_config_file(job_config_file)

@Retry(retry['max_attempts'], retry['min_wait_s'], retry['max_wait_s'])
def remove_directory(ssh_conn, remote_directory):
  ''' remote remote directory '''
  cmd = "rm -rf %s" % remote_directory
  log.debug('executing remotely: %s' % cmd)
  rc, stdout, stderr = ssh.run(cmd, ssh_conn)
  if rc != 0:
    msg = 'Error: Failed to remove remote directory %s%s' % (remote_directory, os.linesep)
    msg += '%s%s%s' % (os.linesep, stderr, os.linesep)
    raise Exception(msg)
  
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
  
remote_directories = []

# read the configuration file for each job
# the configuration files contains the job id and the remote job directory
for localdir in localdirs:
  job_config_file = '%s%s.job.ini' % (localdir, os.path.sep)
  try:
    job_config = read_job_config_file(job_config_file)
    if 'remote_directory' in job_config['JOB']:
      remote_directories.append(job_config['JOB']['remote_directory'])
    else:
      log.warn('no remote directory for local directory %s. skipping job.' % localdir)
  except:
    log.warn('failed to read job config file %s. skipping job.' % job_config_file)

try:
  log.info('cleaning up %s remote directories...' % len(remote_directories))
  ssh_conn = ssh.open_connection_ssh_agent(cluster['remote_host'], cluster['remote_user'], cluster['ssh_priv_key_file'])
  for remote_directory in remote_directories:
    jobids_new = {}
    log.info('removing remote directory %s.' % remote_directory)
    try:
      remove_directory(ssh_conn, remote_directory)
    except:
      log.error('failed to delete remote directory %s. %s' % (remote_directory, traceback.format_exc().strip()))
finally:
  cleanup()
