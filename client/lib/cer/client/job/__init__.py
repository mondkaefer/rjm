import os
import cer.client.util as util
import cer.client.util.config as config
import cer.client.ssh as ssh

# get central configuration
conf = config.get_config()
cluster = conf['CLUSTER']

def prepare_job(ssh_conn, basedir, jobname, cmds, mem, vmem, walltime, jobtype, account):  
  commandline = '%s ' % cluster['remote_prepare_job'] + \
    '--basedir %s ' % basedir + \
    '--jobname %s ' % jobname + \
    '--mem %s ' % mem + \
    '--vmem %s ' % vmem + \
    '--walltime %s ' % walltime + \
    '--jobtype %s ' % jobtype + \
    '--account %s ' % account
  
  for cmd in cmds:
    commandline = '%s %s' % (commandline, '--cmd \'%s\' ' % cmd)

  rc, stdout, stderr = ssh.run(commandline, ssh_conn)
  if rc != 0:
    msg = 'Error: Failed to prepare job%s' % os.linesep
    msg += '%s%s%s' % (os.linesep, stderr, os.linesep)
    raise Exception(msg)
  jobdir, jobscript = stdout.split(',')
  return (jobdir.strip(), jobscript.strip())
  
def submit_job(ssh_conn, remote_job_description_file):
  cmd = '%s %s' % (cluster['remote_submit_job'], remote_job_description_file)
  rc, stdout, stderr = ssh.run(cmd, ssh_conn)
  if rc != 0:
    msg = 'Error: Failed to submit job%s' % os.linesep
    msg += '%s%s%s' % (os.linesep, stderr, os.linesep)
    raise Exception(msg)
  jobid = stdout.strip()
  return jobid
  
def has_finished(ssh_conn, jobid):
  cmd = '%s %s' % (cluster['remote_is_job_done'], jobid)
  rc, stdout, stderr = ssh.run(cmd, ssh_conn)
  if rc != 0:
    msg = 'Error: Failed to get job status for job %s%s' % (jobid,os.linesep)
    msg += '%s%s%s' % (os.linesep, stderr, os.linesep)
    raise Exception(msg)
  return eval(str(stdout).strip())
  
def get_job_statuses(ssh_conn):
  cmd = '%s' % (cluster['remote_get_job_statuses'])
  rc, stdout, stderr = ssh.run(cmd, ssh_conn)
  if rc != 0:
    msg = 'Error: Failed to get job statuses.%s' % (os.linesep)
    msg += '%s%s%s' % (os.linesep, stderr, os.linesep)
    raise Exception(msg)
  statusMap = {}
  for line in stdout.strip().splitlines(False):
    tokens = line.split(' ')
    if len(tokens) != 2:
      raise Exception('Too many columns in result from call to getting remote job statuses: "%s"' % line)
    statusMap[tokens[0]] = tokens[1]
  return statusMap

def cancel_jobs(ssh_conn, jobids):
  if jobids:
    cmd = '%s %s' % (cluster['remote_cancel_jobs'], ' '.join(jobids))
    rc, stdout, stderr = ssh.run(cmd, ssh_conn)
    if rc != 0:
      msg = 'Error: Failed to cancel jobs.%s' % (os.linesep)
      msg += '%s%s%s' % (os.linesep, stderr, os.linesep)
      raise Exception(msg)
    