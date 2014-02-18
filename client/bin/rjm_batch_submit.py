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
    'account/project code this job is run under, e.g. uoa00042, ' +
    'if no account is specified, the default account as specified in %s is used.' % config.get_config_file(),
  'cmd': 
    'command to run. multiple commands can be specified, and they will be executed one after the other as part ' +
    'of the same job',
  'remotebasedir':
    'remote base directory where the individual job directories for each job will be created. ' +
    'if no remote base directory is specified, the default remote base directory as specified in %s is used.' % config.get_config_file(),
  'mem':
    'amount of memory required by this job. Has to be postfixed with one of the following units: M,G, ' +
    'indicating megabytes, gigabytes',
  'vmem':
    'amount of virtual memory required by this job. Has to be postfixed with one of the following units: M,G, ' +
    'indicating megabytes, gigabytes',
  'jobtype':
    'type of the job. the number of processes and threads is specified separated by colons. ' +
    'For serial/multi-threaded jobs: serial:<#threads>. ' +
    'For mpi jobs: mpi:<#processes>[:<#threads>]. ' +
    'Examples: serial, serial:5, mpi:4, mpi:5:4, mpich:6',
  'walltime':
    'wall clock time this job will run for, specified in hours, minutes and seconds in format h[h*]:m[m]:s[s]. ' +
    'examples: 24:0:0, 0:10:0, 6:0:0, 240:10:3',
  'logfile':
    'logfile. if not specified, all messages will be printed to the terminal.', 
  'loglevel':
    'level of log verbosity. default: %s. ' % util.DEFAULT_LOG_LEVEL.lower() +
    'the higher the log level, more information will be printed.',
  'localjobdirfile':
    'file that contains the names of the local job directories, one name per line.',
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
except:
  log.critical('failed to read config file %s' % config.get_config_file())
  log.critical(traceback.format_exc())
  cleanup()
  sys.exit(1)
  
@Retry(conf['RETRY']['max_attempts'], conf['RETRY']['min_wait_s'], conf['RETRY']['max_wait_s'])
def get_local_job_directories(localjobdirfile):
  ''' get the list of local job directories from file. '''
  return util.get_local_job_directories(localjobdirfile)

@Retry(conf['RETRY']['max_attempts'], conf['RETRY']['min_wait_s'], conf['RETRY']['max_wait_s'])
def prepare_job(ssh_conn, args):
  ''' create remote job directory and job description file. '''
  log.debug('Creating job directory...')
  remote_jobdir, remote_job_desc_file = job.prepare_job(ssh_conn, args.remotebasedir, args.jobname, args.cmd,
    args.mem, args.vmem, args.walltime, args.jobtype, args.account)
  log.debug('Remote job directory: %s' % remote_jobdir)
  return (remote_jobdir, remote_job_desc_file)

@Retry(conf['RETRY']['max_attempts'], conf['RETRY']['min_wait_s'], conf['RETRY']['max_wait_s'])
def stage_in_file(localfile, remotefile):
  ''' upload individual input file. '''
  log.debug('Uploading local file %s to remote file %s' % (localfile, remotefile))
  sftp.put(localfile, remotefile)

@Retry(conf['RETRY']['max_attempts'], conf['RETRY']['min_wait_s'], conf['RETRY']['max_wait_s'])
def get_inputfile_names(uploads_file):
  ''' get the names of the local files to be uploaded prior to starting the job. '''  
  filenames = []
  if os.path.isfile(uploads_file):
    filenamestmp = util.read_lines_from_file(uploads_file)
    for name in filenamestmp:
      if not os.path.isabs(name):
        name = '%s%s%s' % (localdir, os.path.sep, name)
        filenames.append(name)
  return filenames
  
@Retry(conf['RETRY']['max_attempts'], conf['RETRY']['min_wait_s'], conf['RETRY']['max_wait_s'])
def submit_job(ssh_conn, remote_job_desc_file):
  ''' submit a job. '''
  log.debug('Submitting job...')
  job_base_id = job.submit_job(ssh_conn, remote_job_desc_file)
  jobid = '%s.0' % job_base_id
  log.debug('Job ID: %s' % jobid)
  return jobid

@Retry(conf['RETRY']['max_attempts'], conf['RETRY']['min_wait_s'], conf['RETRY']['max_wait_s'])
def create_or_update_job_config_file(localdir, props_dict):
  ''' create metadata file for job in local job directory (ini-format) '''
  config.create_or_update_job_config_file(localdir, props_dict)

def stage_in(sftp, uploads_file, remotedir):
  ''' upload all input files, if any. '''
  localfiles = get_inputfile_names(uploads_file)
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
  ssh_conn = ssh.open_connection_ssh_agent(conf['CLUSTER']['remote_host'], conf['CLUSTER']['remote_user'], conf['CLUSTER']['ssh_priv_key_file'])
except:
  log.critical('Failed to set up SSH connection')
  cleanup()
  sys.exit(1)

args.vmem = args.mem if not args.vmem else args.vmem
args.account = conf['CLUSTER']['default_account'] if not args.account else args.account
args.remotebasedir = conf['CLUSTER']['default_remote_base_directory'] if not args.remotebasedir else args.remotebasedir

sftp = ssh_conn.open_sftp()

# create remote job directories, stage files in, submit jobs
for localdir in localdirs:
  try:
    log.info('Submitting job from %s' % localdir)
    args.jobname = os.path.basename(localdir)
    remote_jobdir, remote_job_desc_file = prepare_job(ssh_conn, args)
    create_or_update_job_config_file(localdir, { 'JOB': { 'remote_directory': remote_jobdir, 'download_done': False } })
    uploads_file = '%s%s%s' % (localdir, os.path.sep, conf['FILE_TRANSFER']['uploads_file'])
    stage_in(sftp, uploads_file, remote_jobdir)
    jobid = submit_job(ssh_conn, remote_job_desc_file)
    create_or_update_job_config_file(localdir, { 'JOB': { 'id': jobid } })
  except:
    log.error('Problem submitting job in local directory %s. skipping job.' % localdir)

cleanup()
