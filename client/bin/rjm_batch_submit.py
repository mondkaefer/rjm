import os
import sys
import argparse
import ConfigParser
import cer.client.ssh as ssh
import cer.client.job as job
import cer.client.util as util
from cer.client.util import Retry

# name of the file that contains the list of input files to be uploaded for a job
infiles_file = 'gridfiles_in.txt'
ssh_conn = None

# information displayed as help by argparse
h = {
  'account': 
    'account code this job is run under, e.g. uoa00042',
  'cmd': 
    'Command to run.',
  'remotebasedir':
    'Remote base directory where the individual job directories for each job will be created.',
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

def cleanup():
  ''' close ssh connection. '''
  if ssh_conn:
    try:
      ssh.close_connection(ssh_conn)
    except:
      pass

@Retry(max_attempts=5, mm=(0.5,5))
def get_local_job_directories(localjobdirfile):
  ''' get the list of local job directories from file. '''
  localdirs = util.read_lines_from_file(localjobdirfile)
  for localdir in localdirs:
    if not os.path.isdir(localdir):
      raise Exception('Local job directory does not exist: %s' % localdir)
  return localdirs

@Retry(max_attempts=5, mm=(0.5,5))
def prepare_job(ssh_conn, args):
  ''' create remote job directory and job description file. '''
  log.debug('Creating job directory...')
  remote_jobdir, remote_job_desc_file = job.prepare_job(ssh_conn, args.remotebasedir, args.jobname, args.cmd,
    args.mem, args.vmem, args.walltime, args.jobtype, args.account)
  log.debug('Remote job directory: %s' % remote_jobdir)
  return (remote_jobdir, remote_job_desc_file)

@Retry(max_attempts=5, mm=(0.5,5))
def stage_in_file(localfile, remotefile):
  ''' upload individual input file. '''
  log.debug('Uploading local file %s to remote file %s' % (localfile, remotefile))
  sftp.put(localfile, remotefile)

@Retry(max_attempts=5, mm=(0.5,5))
def get_inputfile_names(localdir):
  ''' get the names of the local files to be uploaded prior to starting the job. '''  
  files_in = '%s%s%s' % (localdir,os.path.sep,infiles_file)
  filenamestmp = util.read_lines_from_file(files_in)
  filenames = []
  for name in filenamestmp:
    if not os.path.isabs(name):
      name = '%s%s%s' % (localdir, os.path.sep, name)
      filenames.append(name)
  return filenames
  
@Retry(max_attempts=5, mm=(0.5,5))
def submit_job(ssh_conn, remote_job_desc_file):
  ''' submit a job. '''
  log.debug('Submitting job...')
  job_base_id = job.submit_job(ssh_conn, remote_job_desc_file)
  jobid = '%s.0' % job_base_id
  log.debug('Job ID: %s' % jobid)
  return jobid

@Retry(max_attempts=5, mm=(0.5,5))
def write_config_file(localdir, jobid, remotedir):
  ''' create metadata file for job in local job directory (ini-format) '''
  configfile = '%s%s.job.ini' % (localdir, os.path.sep)
  log.debug('Writing job config file into %s' % configfile)
  config = ConfigParser.RawConfigParser()
  config.add_section('JOB')
  config.set('JOB', 'id', jobid)
  config.set('JOB', 'remote_job_directory', remotedir)
  if os.path.exists(configfile):
    os.remove(configfile)
  if os.path.exists(configfile):
    raise Exception('Failed to delete configuration file %s' % configfile)
  with open(configfile, 'wb') as f:
    config.write(f)
  if not os.path.isfile(configfile):
    raise Exception("Creation of job config file %s failed." % configfile)

def stage_in(sftp, localdir, remotedir):
  ''' upload all input files, if any. '''
  localfiles = get_inputfile_names(localdir)
  for localfile in localfiles:
    remotefile = '%s/%s' % (remotedir, os.path.basename(localfile))
    stage_in_file(localfile, remotefile)

parser = argparse.ArgumentParser(description='')
parser.add_argument('-a','--account', help=h['account'], required=False, type=str)
parser.add_argument('-c','--cmd', help=h['cmd'], required=True, type=str, action='append')
parser.add_argument('-d','--remotebasedir', help=h['remotebasedir'], required=True, type=str)
parser.add_argument('-f','--localjobdirfile', help=h['localjobdirfile'], required=True, type=str)
parser.add_argument('-j','--jobtype', help=h['jobtype'], required=True, type=str)
parser.add_argument('-l','--logfile', help=h['logfile'], required=False, type=str)
parser.add_argument('-ll','--loglevel', help=h['loglevel'], required=False, type=str, choices=['debug','info','warn','error','critical'])
parser.add_argument('-m','--mem', help=h['mem'], required=True, type=str)
parser.add_argument('-v','--vmem', help=h['vmem'], required=False, type=str)
parser.add_argument('-w','--walltime', help=h['walltime'], required=True, type=str)

args = parser.parse_args()
args.vmem = args.mem if not args.vmem else args.vmem
if args.logfile or args.loglevel:
  util.setup_logging(args.logfile, args.loglevel)
log = util.get_log()

# read local job directories from file
try:
  localdirs = get_local_job_directories(args.localjobdirfile)
except:
  log.critical('Failed to read list of local job directories')
  cleanup()
  sys.exit(1)
    
# set up SSH connection
try:
  parser = util.get_config_parser()
  host = parser.get('MAIN', 'remote_host')
  user = parser.get('MAIN', 'remote_user')
  privkey = parser.get('MAIN', 'ssh_priv_key_file')
  ssh_conn = ssh.open_connection_ssh_agent(host, user, privkey)
except:
  log.critical('Failed to set up SSH connection')
  cleanup()
  sys.exit(1)

sftp = ssh_conn.open_sftp()

# create remote job directories, stage files in, submit jobs
for localdir in localdirs:
  try:
    log.info('Submitting job from %s' % localdir)
    args.jobname = os.path.basename(localdir)
    remote_jobdir, remote_job_desc_file = prepare_job(ssh_conn, args)
    stage_in(sftp, localdir, remote_jobdir)
    jobid = submit_job(ssh_conn, remote_job_desc_file)
    write_config_file(localdir, jobid, remote_jobdir)
  except:
    log.error('Problem submitting job in local directory %s. skipping job.' % localdir)

cleanup()
