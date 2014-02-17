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

# default logging configuration
FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOGGER = logging.getLogger('RJM')
DEFAULT_LOG_LEVEL='INFO'
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
  def __init__(self, max_attempts, min_wait_s, max_wait_s):
    self.max_attempts = int(max_attempts)
    self.min_wait_s = float(min_wait_s)
    self.max_wait_s = float(max_wait_s)
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
            time.sleep(random.uniform(self.min_wait_s, self.max_wait_s))
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

def get_local_job_directories(localjobdirfile):
  ''' get the list of local job directories from file. '''
  localdirs = []
  localdirstmp = read_lines_from_file(localjobdirfile)
  for localdir in localdirstmp:
    if os.path.exists(localdir):
      localdirs.append(localdir)
    else:
      get_log().warn('local job directory does not exist: %s. Skipping.' % localdir)
  return localdirs
