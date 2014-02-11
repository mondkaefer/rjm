import os
import re
import sys
import time
import shlex
import random
import logging
import StringIO
import traceback
import ConfigParser
from logging import StreamHandler, FileHandler
from logging.handlers import MemoryHandler
from datetime import datetime
from subprocess import Popen, PIPE

# name of the configuration directory
CONFIG_DIR_NAME = '.remote_jobs'
# name of the configuration file
CONFIG_FILE_NAME = 'config.ini'
# name of the private SSH key
SSH_PRIV_KEY = 'auckland_pan_cluster'

# default logging configuration
FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOGGER = logging.getLogger('RJM')
DEFAULT_LOG_LEVEL='WARN'
LOGGER.setLevel(eval("logging.%s" % DEFAULT_LOG_LEVEL))
handler = StreamHandler() 
handler.setFormatter(logging.Formatter(FORMAT))
LOGGER.addHandler(handler)

def platform_is_windows():
  ''' return True if code runs on Windows, otherwise False. '''
  if sys.platform.lower().startswith('win'):
    return True
  else:
    return False

def get_config_dir():
  ''' get the absolute path of the configuration directory. '''
  if platform_is_windows():
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
  
def get_config_parser():
  ''' get a ConfigParser object of the configuration file. '''
  parser = ConfigParser.SafeConfigParser();
  parser.read(get_config_file())
  return parser

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
  
def create_config_file(host, user, fingerprint):
  ''' create the configuration file.
      if the configuration directory does not exist, it will be created.
  '''
  create_config_dir()
  f = open(get_config_file(), "w+")
  f.write('[MAIN]%s' % os.linesep)
  f.write('remote_host=%s%s' % (host, os.linesep))
  f.write('remote_user=%s%s' % (user, os.linesep))
  f.write('ssh_priv_key_file=%s%s' % (get_priv_ssh_key(), os.linesep))
  f.write('ssh_fingerprint=%s%s' % (fingerprint, os.linesep))
  f.close()

def run(command_and_args, error_on_stderr=True, error_on_nonzero_rc=True):
  ''' run a local system call 
      behaviour on non-zero exit code or existence of stderr is controlled by the function parameters.
  '''
  try:
    process = Popen(shlex.split(command_and_args), shell=False, stdout=PIPE, stderr=PIPE)
    (stdout, stderr) = process.communicate()
    rc = process.returncode
  except:
    raise Exception("failed to run '%s': %s" % (command_and_args, sys.exc_info()[1]))

  if rc != 0 and error_on_nonzero_rc:
    raise Exception('\'%s\' returned exit code %d. stderr: %s' % (command_and_args, rc, stderr))

  if stderr != "" and error_on_stderr:
    raise Exception('error running command \'%s\': Got non-empty stderr: %s' % (command_and_args, stderr))

  return (stdout, stderr, rc)

def setup_logging(logfile, loglevel):
  ''' change default logging configuration (see above) '''
  if loglevel:
    LOGGER.setLevel(eval("logging.%s" % loglevel.upper()))
  if logfile:
    file_handler = FileHandler(filename=logfile, mode='w+b')
    file_handler.setFormatter(logging.Formatter(FORMAT))
    mem_handler = MemoryHandler(capacity=8192, flushLevel=logging.DEBUG, target=file_handler)
    for h in LOGGER.handlers:
      LOGGER.removeHandler(h)
    LOGGER.addHandler(mem_handler)

def get_log():
  ''' return the configured logger '''
  return LOGGER

class Retry(object):
  ''' decorator to allow for multiple attempts to call a function. 
      if the function call fails, sleep some time before the next attempt.
      the time to sleep is randomly determined from the interval [min, max] specified in mm.
      give up after max_attempts.
  '''
  def __init__(self, max_attempts, mm):
    self.max_attempts = max_attempts
    self.mm = mm
    self.log = get_log()

  def __call__(self, f):
    def wrapped_f(*args):
      for i in range(1,self.max_attempts+1):
        try:
          return f(*args)
        except:
          if i < self.max_attempts:
            self.log.warn("attempt #%s to call function '%s' with parameters %s failed. %s" %
                          (i, f.__name__, str(args), traceback.format_exc().strip()))
            time.sleep(random.uniform(self.mm[0], self.mm[1]))
          else:
            self.log.error("attempt #%s to call function '%s' with parameters %s failed. giving up. %s" %
                           (i, f.__name__, str(args), traceback.format_exc().strip()))
            raise
    return wrapped_f

def read_lines_from_file(filename):
  ''' read all lines from a file and return them as an array.
      leading and trailing whitespaces are deleted.
      empty lines, or lines consisting only of whitespaces are ignored.
  '''
  lines_tmp = None
  lines = []
  if not os.path.isfile(filename):
    raise Exception('file does not exist: %s' % filename)
    
  try:
    f = open(filename, 'r')
    lines_tmp = f.readlines()
  except:
    raise Exception('failed to read file %s' % filename)
  finally:
    if f:
      f.close()

  if lines_tmp:
    for line in lines_tmp:
      line = line.strip()
      if line:
        lines.append(line.strip())
  return lines
