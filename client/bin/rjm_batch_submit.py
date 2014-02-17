import os
import sys
import argparse
import traceback
import cer.client.ssh as ssh
import cer.client.job as job
import cer.client.util as util
import cer.client.util.config as config
from cer.client.util import Retry

def cleanup():
  ''' close ssh connection. '''
  if ssh_conn:
    try:
      ssh.close_connection(ssh_conn)
    except:
      pass

# information displayed as help by argparse
h = {
  'account':
    'Account code this job is run under, e.g. uoa00042, ' +
    'If not account is specified, the default account as specified in %s is used.' % config.get_config_file(),
  'cmd': 
    'Command to run.',
  'remotebasedir':
    'Remote base directory where the individual job directories for each job will be created. ' +
    'If no remote base directory is specified, the default remote base directory as specified in %s is used.' % config.get_config_file(),
  'mem':
    'Amount of memory required by this job. Has to be postfixed with one of the following units: M,G, ' +
    'indicating megabytes, gigabytes',
  'vmem':
    'Amount of virtual memory required by this job. Has to be postfixed with one of the following units: M,G, ' +
    'indicating megabytes, gigabytes',
  'jobname':
    'Name of the job. The value of the parameter job name postfixed with the current date and time will ' +
    'be used to create the job directory. If no job name is given, the job name is "job"',
  'jobtype':
    'Type of the job. The number of processes and threads is specified separated by colons. ' +
    'For serial/multi-threaded jobs: serial:<#threads>. ' +
    'For MPI jobs: mpi:<#processes>[:<#threads>]. ' +
    'Examples: serial, serial:5, mpi:4, mpi:5:4, mpich:6',
  'walltime':
    'Wall clock time this job will run for, specified in hours, minutes and seconds in format h[h*]:m[m]:s[s]. ' +
    'Examples: 24:0:0, 0:10:0, 6:0:0, 240:10:3',
  'localjobdirfile':
    'File that contains the names of the local job directories, one name per line.',
  'logfile':
    'Logfile. If not specified, all messages will be printed to stdout', 
  'loglevel':
    'Loglevel. Default: %s' % util.DEFAULT_LOG_LEVEL.lower(),
}

parser = argparse.ArgumentParser(description='')
parser.add_argument('-a','--account', help=h['account'], required=False, type=str)
parser.add_argument('-c','--cmd', help=h['cmd'], required=True, type=str, action='append')
parser.add_argument('-d','--remotebasedir', help=h['remotebasedir'], required=False, type=str)
parser.add_argument('-f','--localjobdirfile', help=h['localjobdirfile'], required=True, type=str)
parser.add_argument('-j','--jobtype', help=h['jobtype'], required=True, type=str)
parser.add_argument('-l','--logfile', help=h['logfile'], required=False, type=str)
parser.add_argument('-ll','--loglevel', help=h['loglevel'], required=False, type=str, choices=['debug','info','warn','error','critical'])
parser.add_argument('-m','--mem', help=h['mem'], required=True, type=str)
parser.add_argument('-v','--vmem', help=h['vmem'], required=False, type=str)
parser.add_argument('-w','--walltime', help=h['walltime'], required=True, type=str)
args = parser.parse_args()

if args.logfile or args.loglevel:
  util.setup_logging(args.logfile, args.loglevel)
log = util.get_log()

ssh_conn = None

# read central configuration file
try:
  conf = config.get_config()
  cluster = conf['CLUSTER']
  retry = conf['RETRY']
except:
  log.critical('failed to read config file %s' % config.get_config_file())
  log.critical(traceback.format_exc())
  cleanup()
  sys.exit(1)
  
@Retry(retry['max_attempts'], retry['min_wait_s'], retry['max_wait_s'])
def get_local_job_directories(localjobdirfile):
  ''' get the list of local job directories from file. '''
  return util.get_local_job_directories(localjobdirfile)

@Retry(retry['max_attempts'], retry['min_wait_s'], retry['max_wait_s'])
def prepare_job(ssh_conn, args):
  ''' create remote job directory and job description file. '''
  log.debug('Creating job directory...')
  remote_jobdir, remote_job_desc_file = job.prepare_job(ssh_conn, args.remotebasedir, args.jobname, args.cmd,
    args.mem, args.vmem, args.walltime, args.jobtype, args.account)
  log.debug('Remote job directory: %s' % remote_jobdir)
  return (remote_jobdir, remote_job_desc_file)

@Retry(retry['max_attempts'], retry['min_wait_s'], retry['max_wait_s'])
def stage_in_file(localfile, remotefile):
  ''' upload individual input file. '''
  log.debug('Uploading local file %s to remote file %s' % (localfile, remotefile))
  sftp.put(localfile, remotefile)

@Retry(retry['max_attempts'], retry['min_wait_s'], retry['max_wait_s'])
def get_inputfile_names(localdir):
  ''' get the names of the local files to be uploaded prior to starting the job. '''  
  files_in = '%s%s%s' % (localdir, os.path.sep, config.INPUT_FILES_FILE)
  filenames = []
  if os.path.isfile(files_in):
    filenamestmp = util.read_lines_from_file(files_in)
    for name in filenamestmp:
      if not os.path.isabs(name):
        name = '%s%s%s' % (localdir, os.path.sep, name)
        filenames.append(name)
  return filenames
  
@Retry(retry['max_attempts'], retry['min_wait_s'], retry['max_wait_s'])
def submit_job(ssh_conn, remote_job_desc_file):
  ''' submit a job. '''
  log.debug('Submitting job...')
  job_base_id = job.submit_job(ssh_conn, remote_job_desc_file)
  jobid = '%s.0' % job_base_id
  log.debug('Job ID: %s' % jobid)
  return jobid

@Retry(retry['max_attempts'], retry['min_wait_s'], retry['max_wait_s'])
def create_or_update_job_config_file(localdir, props_dict):
  ''' create metadata file for job in local job directory (ini-format) '''
  config.create_or_update_job_config_file(localdir, props_dict)

def stage_in(sftp, localdir, remotedir):
  ''' upload all input files, if any. '''
  localfiles = get_inputfile_names(localdir)
  log.debug('files to upload: %s' % str(localfiles))
  for localfile in localfiles:
    remotefile = '%s/%s' % (remotedir, os.path.basename(localfile))
    stage_in_file(localfile, remotefile)

# read local job directories from file
try:
  localdirs = get_local_job_directories(args.localjobdirfile)
except:
  log.critical('Failed to read list of local job directories')
  log.critical(traceback.format_exc())
  cleanup()
  sys.exit(1)
    
# set up SSH connection
try:
  ssh_conn = ssh.open_connection_ssh_agent(cluster['remote_host'], cluster['remote_user'], cluster['ssh_priv_key_file'])
except:
  log.critical('Failed to set up SSH connection')
  cleanup()
  sys.exit(1)

args.vmem = args.mem if not args.vmem else args.vmem
args.account = cluster['default_account'] if not args.account else args.account
args.remotebasedir = cluster['default_remote_base_directory'] if not args.remotebasedir else args.remotebasedir

sftp = ssh_conn.open_sftp()

# create remote job directories, stage files in, submit jobs
for localdir in localdirs:
  try:
    log.info('Submitting job from %s' % localdir)
    args.jobname = os.path.basename(localdir)
    remote_jobdir, remote_job_desc_file = prepare_job(ssh_conn, args)
    create_or_update_job_config_file(localdir, { 'JOB': { 'remote_directory': remote_jobdir, 'download_done': False } })
    stage_in(sftp, localdir, remote_jobdir)
    jobid = submit_job(ssh_conn, remote_job_desc_file)
    create_or_update_job_config_file(localdir, { 'JOB': { 'id': jobid } })
  except:
    log.error('Problem submitting job in local directory %s. skipping job.' % localdir)

cleanup()
