import os
import sys
import time
import argparse
import traceback
import ConfigParser
import cer.client.ssh as ssh
import cer.client.job as job
import cer.client.util as util
from cer.client.util import Retry

# name of the file that contains the list of files to be downloaded after a job
outfiles_file = 'gridfiles_out.txt'
logfile = None
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
  'pollingintervalsec':
    'Number of seconds to wait between polling for job status.',
}

def cleanup():
  if ssh_conn:
    try:
      ssh.close_connection(ssh_conn)
    except:
      pass

@Retry(max_attempts=5, mm=(0.5,5))
def get_local_job_directories(localjobdirfile):
  ''' get the list of local job directories from file. '''
  localdirs = []
  localdirstmp = util.read_lines_from_file(localjobdirfile)
  for localdir in localdirstmp:
    if os.path.exists(localdir):
      localdirs.append(localdir)
    else:
      log.warn('local job directory does not exist: %s. Skipping.' % localdir)
  return localdirs

@Retry(max_attempts=5, mm=(0.5,5))
def read_job_config_file(job_config_file):
  ''' read the local configuration file of a job (ini-format). '''
  if not os.path.isfile(job_config_file):
    raise Exception('no job config file found in %s.' % localdir)
  try:
    parser = ConfigParser.RawConfigParser()
    parser.read(job_config_file)
    job_id = parser.get('JOB', 'id')
    remote_job_directory = parser.get('JOB', 'remote_job_directory')
    log.debug('read from %s: (id=%s, remote_job_directory=%s)' % (job_config_file, job_id, remote_job_directory))
  except:
    raise Exception('failed to read from config file %s' % job_config_file)
  
  if not job_id or not remote_job_directory:
    raise Exception('got empty values from config file %s' % job_config_file)
  
  return (job_id, remote_job_directory)
  
@Retry(max_attempts=5, mm=(0.5,5))
def stage_out_file(sftp, remotefile, localfile):
  ''' download an individual file. '''
  log.debug('downloading remote file %s to local file %s' % (remotefile, localfile))
  sftp.get(remotefile, localfile)

@Retry(max_attempts=5, mm=(0.5,5))
def get_outputfile_names(localdir, remotedir):
  ''' get the full path of the remote files to be downloaded after a job is done '''
  files_out = '%s%s%s' % (localdir,os.path.sep,outfiles_file)
  filenamestmp = util.read_lines_from_file(files_out)
  filenames = ['%s/%s' % (remotedir, name) for name in filenamestmp]
  return filenames

@Retry(max_attempts=5, mm=(0.5,5))
def rename_file(old, new):
  ''' rename a file.
      note, that on Windows os.rename() causes an exception if the new file already exists.
      that's the reason for removing the existing file first.
  '''
  log.debug('renaming %s to %s' % (old, new))
  if os.path.exists(new):
    os.remove(new)
  os.rename(old, new)
  if not os.path.isfile(new):
    raise Exception('renaming file %s to %s failed.' % (old, new))

def stage_out(sftp, localdir, remotedir):
  ''' download all files for this job '''
  remotefiles = get_outputfile_names(localdir, remotedir)
  log.debug('files to download: %s' % str(remotefiles))
  if remotefiles:
    for remotefile in remotefiles:
      name = os.path.basename(remotefile)
      localtempfile = '%s%s.%s' % (localdir, os.path.sep, name)
      localfile = '%s%s%s' % (localdir, os.path.sep, name)
      try:
        stage_out_file(sftp, remotefile, localtempfile)
      except:
        log.warn('failed to download file %s to %s' % (remotefile, localfile))
        continue
      rename_file(localtempfile, localfile)
    log.info('done downloading results into directory %s' % localdir)

parser = argparse.ArgumentParser(description='')
parser.add_argument('-f','--localjobdirfile', help=h['localjobdirfile'], required=True, type=str)
parser.add_argument('-l','--logfile', help=h['logfile'], required=False, type=str)
parser.add_argument('-ll','--loglevel', help=h['loglevel'], required=False, type=str, choices=['debug','info','warn','error','critical'])
parser.add_argument('-z','--pollingintervalsec', help=h['pollingintervalsec'], required=True, type=int)
args = parser.parse_args()

# change default logging configuration if required
if args.logfile or args.loglevel:
  util.setup_logging(args.logfile, args.loglevel)
log = util.get_log()

# read local job directories from file
try:
  localdirs = get_local_job_directories(args.localjobdirfile)
except:
  log.critical('failed to read list of local job directories or invalid entries in list')
  cleanup()
  sys.exit(1)

# stop if no job directories are listed
if len(localdirs) == 0:
  log.info('no job directories. nothing to wait for. exiting')
  cleanup()
  sys.exit(0)
  
jobs = {}

# read the configuration file for each job
# the configuration files contains the job id and the remote job directory
for localdir in localdirs:
  job_config_file = '%s%s.job.ini' % (localdir, os.path.sep)
  try:
    job_id, remote_job_directory = read_job_config_file(job_config_file)
    jobs[job_id] = { 'remote_job_directory': remote_job_directory, 'local_job_directory': localdir }
  except:
    log.warn('failed to read job config file %s. skipping job.' % job_config_file)

# Read SSH connection parameters
try:
  parser = util.get_config_parser()
  host = parser.get('MAIN', 'remote_host')
  user = parser.get('MAIN', 'remote_user')
  privkey = parser.get('MAIN', 'ssh_priv_key_file')
  log.debug('SSH configuration: host=%s, user=%s, privkey=%s' % (host, user, privkey))
except:
  log.critical('failed to read connection parameters. %s' % traceback.format_exc().strip())
  cleanup()
  sys.exit(1)

log.info('waiting for jobs to finish (polling every %s seconds)...' % args.pollingintervalsec)
while True:
  try:
    log.info('checking for job status of %s jobs...' % len(jobs))
    jobs_new = {}
    ssh_conn = ssh.open_connection_ssh_agent(host, user, privkey)
    log.debug('getting job statuses')
    jobmap = job.get_job_statuses(ssh_conn)
    for job_id in jobs.keys():
      if job_id not in jobmap:
        log.info('job %s finished.' % job_id)
        try:
          stage_out(ssh_conn.open_sftp(), jobs[job_id]['local_job_directory'], jobs[job_id]['remote_job_directory'])
        except:
          log.warn('failed to download or rename some of the result files for job into %s' % jobs[job_id]['local_job_directory'])
      else:
        jobs_new[job_id] = jobs[job_id]
  except:
    log.warn('failed to get status. only critical if happens repeatedly. %s' % traceback.format_exc().strip())
  finally:
    try:
      ssh.close_connection(ssh_conn)
    except:
      pass
    
  if len(jobs_new) > 0:
    time.sleep(args.pollingintervalsec)
    jobs = jobs_new
  else:
    break

cleanup()
