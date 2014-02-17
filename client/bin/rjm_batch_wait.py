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
outfiles_file = 'gridfiles_out.txt'
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

parser = argparse.ArgumentParser(description='')
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
def stage_out_file(sftp, remotefile, localfile):
  ''' download an individual file. '''
  log.debug('downloading remote file %s to local file %s' % (remotefile, localfile))
  sftp.get(remotefile, localfile)

@Retry(retry['max_attempts'], retry['min_wait_s'], retry['max_wait_s'])
def get_outputfile_names(localdir, remotedir):
  ''' get the full path of the remote files to be downloaded after a job is done '''
  files_out = '%s%s%s' % (localdir,os.path.sep,outfiles_file)
  filenames = []
  if os.path.isfile(files_out):
    filenamestmp = util.read_lines_from_file(files_out)
    filenames = ['%s/%s' % (remotedir, name) for name in filenamestmp]
  return filenames
  

@Retry(retry['max_attempts'], retry['min_wait_s'], retry['max_wait_s'])
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

@Retry(retry['max_attempts'], retry['min_wait_s'], retry['max_wait_s'])
def create_or_update_job_config_file(localdir, props_dict):
  ''' create metadata file for job in local job directory (ini-format) '''
  config.create_or_update_job_config_file(localdir, props_dict)

def stage_out(sftp, localdir, remotedir):
  ''' download all files for this job, if they have not already been downloaded '''
  job_config = read_job_config_file('%s%s.job.ini' % (localdir, os.path.sep))
  if not eval(job_config['JOB']['download_done']):
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
      create_or_update_job_config_file(localdir, { 'JOB': { 'download_done': True } })
      log.info('done downloading results into directory %s' % localdir)
  else:
    log.info('results have already been downloaded for job in local directory %s' % localdir)

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
    job_config = read_job_config_file(job_config_file)
    job_id = job_config['JOB']['id']
    remote_directory = job_config['JOB']['remote_directory']
    jobs[job_id] = { 'remote_directory': remote_directory, 'local_directory': localdir }
  except:
    log.warn('failed to read job config file %s. skipping job.' % job_config_file)

log.info('waiting for jobs to finish (polling every %s seconds)...' % args.pollingintervalsec)
while True:
  try:
    log.info('checking for job status of %s jobs...' % len(jobs))
    jobs_new = {}
    ssh_conn = ssh.open_connection_ssh_agent(cluster['remote_host'], cluster['remote_user'], cluster['ssh_priv_key_file'])
    log.debug('getting job statuses')
    jobmap = job.get_job_statuses(ssh_conn)
    for job_id in jobs.keys():
      if job_id not in jobmap:
        log.info('job %s finished.' % jobs[job_id]['local_directory'])
        try:
          stage_out(ssh_conn.open_sftp(), jobs[job_id]['local_directory'], jobs[job_id]['remote_directory'])
        except:
          log.warn('failed to download or rename some of the result files for job into %s' % jobs[job_id]['local_directory'])
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
