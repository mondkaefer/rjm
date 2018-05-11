import os
import cer.client.util as util
try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import SafeConfigParser as ConfigParser

# name of the private SSH key
SSH_PRIV_KEY = 'auckland_pan_cluster'
# name of the configuration directory
CONFIG_DIR_NAME = '.remote_jobs'
# name of the configuration file
CONFIG_FILE_NAME = 'config.ini'
# default remote host
DEFAULT_REMOTE_HOST = 'login.uoa.nesi.org.nz'
# Name of the file that contains the list of files to be uploaded before the job starts
DEFAULT_UPLOAD = 'rjm_uploads.txt'
# Name of the file that contains the list of files to be downloaded after the job is done
DEFAULT_DOWNLOAD = 'rjm_downloads.txt'

class ConfigReader(ConfigParser):
  def as_dict(self):
    ''' return the configuration as dictionary '''
    d = dict(self._sections)
    for k in d:
      d[k] = dict(self._defaults, **d[k])
      d[k].pop('__name__', None)
    return d

def get_config_dir():
  ''' get the absolute path of the configuration directory. '''
  if util.platform_is_windows():
    directory = '%s%s%s' % (os.environ['USERPROFILE'], os.path.sep, CONFIG_DIR_NAME)
  else:
    directory = '%s%s%s' % (os.environ['HOME'], os.path.sep, CONFIG_DIR_NAME)
  return directory

def get_config_file():
  ''' get the absolute path of the configuration file. '''
  return '%s%s%s' % (get_config_dir(), os.path.sep, CONFIG_FILE_NAME)

def get_priv_ssh_key():
  ''' get the absolute path of the private ssh key. '''
  return '%s%s%s' % (get_config_dir(), os.path.sep, SSH_PRIV_KEY)
  
def get_pub_ssh_key():
  ''' get the absolute path of the public ssh key. '''
  return '%s%s%s.pub' % (get_config_dir(), os.path.sep, SSH_PRIV_KEY)
  
def get_config():
  ''' return the main configuration as dictionary. '''
  configFile = get_config_file()
  if not os.path.isfile(configFile):
    raise Exception('configuration file %s does not exist.' % configFile)
  cr = ConfigReader();
  cr.read(configFile)
  return cr.as_dict()

def create_config_dir():
  ''' create the configuration directory.
      if the directory does not yet exist, it will be created.
  '''
  config_dir = get_config_dir()
  if not os.path.exists(config_dir):
    os.mkdir(config_dir)
  else:
    if not os.path.isdir(config_dir):
      raise Exception('unexpected error: %s already exists and is not a directory.' % config_dir)
  
def create_config_file(host, user, fingerprint, default_project_code, default_remote_directory, rjm_upload, rjm_download):
  ''' create the configuration file.
      if the configuration directory does not exist, it will be created.
  '''
  create_config_dir()
  f = open(get_config_file(), "w+")
  f.write('[CLUSTER]%s' % os.linesep)
  f.write('remote_host=%s%s' % (host, os.linesep))
  f.write('remote_user=%s%s' % (user, os.linesep))
  f.write('ssh_priv_key_file=%s%s' % (get_priv_ssh_key(), os.linesep))
  f.write('ssh_fingerprint=%s%s' % (fingerprint, os.linesep))
  f.write('default_project_code=%s%s' % (default_project_code, os.linesep))
  f.write('default_remote_directory=%s%s' % (default_remote_directory, os.linesep))
  f.write('remote_prepare_job=%s%s' % ('/share/apps/remoteapi/0.3/prepare_job', os.linesep))
  f.write('remote_submit_job=%s%s' % ('/share/apps/remoteapi/0.3/submit_job', os.linesep))
  f.write('remote_is_job_done=%s%s' % ('/share/apps/remoteapi/0.3/is_job_done', os.linesep))
  f.write('remote_get_job_statuses=%s%s' % ('/share/apps/remoteapi/0.3/get_job_statuses', os.linesep))
  f.write('remote_cancel_jobs=%s%s' % ('/share/apps/remoteapi/0.3/cancel_jobs', os.linesep))
  f.write('%s' % os.linesep)
  f.write('[FILE_TRANSFER]%s' % os.linesep)
  f.write('uploads_file=%s%s' % (rjm_upload, os.linesep))
  f.write('downloads_file=%s%s' % (rjm_download, os.linesep))
  f.write('%s' % os.linesep)
  f.write('[RETRY]%s' % os.linesep)
  f.write('max_attempts=%s%s' % ('5', os.linesep))
  f.write('min_wait_s=%s%s' % ('0.5', os.linesep))
  f.write('max_wait_s=%s%s' % ('5', os.linesep))
  f.close()

def read_job_config_file(job_config_file):
  ''' read the local configuration file of a job (ini-format). '''
  if not os.path.isfile(job_config_file):
    raise Exception('config file not found: %s' % job_config_file)
  try:
    cr = ConfigReader()
    cr.read(job_config_file)
    crd = cr.as_dict()
    util.get_log().debug('read from %s: %s' % (job_config_file, str(crd)))
  except:
    raise Exception('failed to read from config file %s' % job_config_file)
  
  return crd

def create_or_update_job_config_file(localdir, props_dict):
  ''' create or update metadata file for job in local job directory (ini-format) '''
  configfile = '%s%s.job.ini' % (localdir, os.path.sep)
  config = ConfigParser()
  if os.path.isfile(configfile):
    util.get_log().debug('Updating job config file %s with %s' % (configfile, str(props_dict)))
    config.read(configfile)    
  else:
    util.get_log().debug('Writing job config file into %s with %s' % (configfile, str(props_dict)))

  for key1 in props_dict.keys():
    if not config.has_section(key1):
      config.add_section(key1)
    for key2 in props_dict[key1].keys():
      config.set(key1, key2, props_dict[key1][key2])
  
  with open(configfile, 'w+b') as f:
    config.write(f)
  
  if not os.path.isfile(configfile):
    raise Exception("Creation or update of job config file %s failed." % configfile)
