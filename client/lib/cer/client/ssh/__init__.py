import os
import sys
import time
import random
import getpass
import logging
import tempfile
import stat
import StringIO
import socket
import cer.client.util as util
import cer.client.util.config as config
from datetime import datetime
from Crypto.PublicKey import RSA
from Crypto import Random
from paramiko import SSHClient, SSHException, AutoAddPolicy, RSAKey

logging.getLogger('paramiko.transport').addHandler(logging.NullHandler())

def open_connection_username_password(host, user, password, port=22):
  '''
    Open an SSH connection and return the connection object.
  '''
  try:
    client = SSHClient()
    client.set_missing_host_key_policy(AutoAddPolicy())
    client.connect(hostname=host, port=port, username=user, password=password)
    return client
  except socket.gaierror:
    raise Exception('Unable to connect to remote host %s. Check network connectivity.' % host)

def open_connection_ssh_agent(host, user, ssh_priv_key, port=22):
  '''
    Open an SSH connection and return the connection object.
  '''
  # paramiko fix for windows: https://github.com/paramiko/paramiko/issues/193
  # https://github.com/akx/paramiko/commit/7dfa239d289b91d2040973213a8c49bdf1e07392
  try:
    client = __connect_with_agent(host, port, user)
  except socket.gaierror:
    raise Exception('Unable to connect to remote host %s. Check network connectivity.' % host)
  except:
    add_private_key_to_agent(ssh_priv_key)
    client = __connect_with_agent(host, port, user)
  return client

def __connect_with_agent(host, port, user):
  '''
    Open an SSH connection. If setting up the connection fails with 'Error reading SSH protocol banner'
    (see inline comment in code), wait a random amount of time and retry.
  '''
  max_attempts = 10
  attempts = 0

  while attempts < max_attempts:
    try:
      client = SSHClient()
      client.set_missing_host_key_policy(AutoAddPolicy())
      client.connect(hostname=host, port=port, username=user, allow_agent=True)
      break
    except SSHException, sshe:
      # NB. If you get errors like "Error reading SSH protocol banner ... Connection reset by peer",
      # the server may be enforcing a maximum number of concurrent connections (eg. MaxStartups in OpenSSH).
      # This sleeping and retrying should only be a workaround
      # Ideally, in a bulk submission scenario, all commands would reuse a single SSH connection
      # or an SSH connection pool
      if 'Error reading SSH protocol banner' in str(sshe):
        attempts += 1
        time.sleep(random.uniform(0.1, 10))
      else:
        raise sshe

  if attempts >= max_attempts:
    raise Exception('Repeated attempts to connect to %s failed' % host)
  return client
  

def add_private_key_to_agent(ssh_priv_key):
  '''
    Add the private key to an ssh-agent
  '''
  if sys.platform.lower().startswith('win'):
    # install SendKeys via exe on Windows
    # requires pywinauto 0.4.2
    from pywinauto import application
    from pywinauto import findwindows
    from pywinauto.timings import WaitUntil

    def check_passphrase_windows_closed():
      return (len(findwindows.find_windows(title=u'Pageant: Enter Passphrase', class_name='#32770')) == 0)
    
    # pageant locks the directory we're in, and we don't want that
    try:
      cwd = os.getcwd()
      os.chdir(os.environ['USERPROFILE'])
      application.Application.start('"%spageant.exe" "%s"' % (os.environ['CER_TOOLKIT_PATH'], os.path.sep, ssh_priv_key))
    except:
      raise
    finally:
      os.chdir(cwd)
      
    WaitUntil(6000, 1, check_passphrase_windows_closed)
  else:
    try:
      util.run('ssh-add %s' % ssh_priv_key, error_on_stderr=False)
    except:
      handle,path = tempfile.mkstemp()
      os.write(handle, '#!/usr/bin/env bash%s' % os.linesep)
      os.write(handle, 'echo ${SSH_ASKPASS_PASSWORD}')
      os.close(handle)
      os.chmod(path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
      os.environ['SSH_ASKPASS_PASSWORD'] = getpass.getpass('Enter passphrase for %s: ' % ssh_priv_key)
      os.environ['SSH_ASKPASS'] = path
      util.run('ssh-add %s' % ssh_priv_key, error_on_stderr=False)
      os.environ['SSH_ASKPASS_PASSWORD'] = ''
      os.remove(path)

def close_connection(connection):
  '''
    Close an SSH connection.
  '''
  if connection:
    connection.close()
  
def run(command_and_args, connection):
  '''
    Execute a command on a remote host via SSH.
    If a connection is provided, it will be used. The connection will not be closed after the remote
    command execution. Otherwise a new connection is created, and closed after the remote command execution.
  '''
  stdout = StringIO.StringIO()
  stderr = StringIO.StringIO()
  tmpstdin, tmpstdout, tmpstderr = connection.exec_command(command_and_args)
  stdout.write(tmpstdout.read())
  stderr.write(tmpstderr.read())
  rc = tmpstdout.channel.recv_exit_status()
  return (rc, stdout.getvalue(), stderr.getvalue())

def create_ssh_rsa_key_pair(passphrase, bits=2048):
  random_generator = Random.new().read
  keypair = RSA.generate(bits, random_generator)

  # extract keys
  privkey = keypair.exportKey('PEM', passphrase, pkcs=1)
  pubkey = '%s %s %s' % (keypair.publickey().exportKey("OpenSSH"), config.SSH_PRIV_KEY, str(datetime.now()))

  # writing keys to files
  priv_key_file = config.get_priv_ssh_key()
  pub_key_file = '%s.pub' % priv_key_file

  with open("%s" % priv_key_file, 'w', stat.S_IWUSR | stat.S_IRUSR) as f:
    f.write("%s" % privkey)
    f.close()
    os.chmod(priv_key_file, stat.S_IWUSR | stat.S_IRUSR)

  with open("%s" % pub_key_file, 'w') as f:
    f.write("%s" % pubkey)
    f.close()
  
  # Get fingerprint and encode it in format aa:bb:cc:dd:...
  fingerprint = RSAKey.from_private_key_file(priv_key_file, password=passphrase).get_fingerprint().encode('hex')
  fingerprint = ':'.join(fingerprint[i:i + 2] for i in xrange(0, len(fingerprint), 2))

  # If running under Windows, convert OpenSSH private key to PuTTY private key,
  # because pageant cannot load OpenSSH keys.
  # To do that, launch and drive puttygen.exe GUI via pywinauto, because the Windows
  # version of puttygen doesn't have a proper command-line interface and there doesn't
  # seem to be another easy way to convert an OpenSSH key into a PuTTY key
  if util.platform_is_windows():
    from pywinauto import application
    priv_key_file_putty = '%s.ppk' % priv_key_file
    cmd = '"%sputtygen.exe" "%s"' % (os.environ['CER_TOOLKIT_PATH'], os.path.sep, priv_key_file)
    puttygen = application.Application.start(cmd)
    puttygen.Dialog.TypeKeys(passphrase, with_spaces=True)
    puttygen.Dialog.OK.Click()
    puttygen.Dialog.OK.Click()
    puttygen.Dialog.Edit3.Select()
    puttygen.Dialog.Edit3.TypeKeys('{BACKSPACE}')
    puttygen.Dialog.Edit3.TypeKeys('Auckland Pan Cluster', with_spaces=True)
    puttygen.Dialog.SavePrivateKey.Click()
    save_dialog = puttygen['Save private key as:']
    save_dialog.Edit.TypeKeys(priv_key_file_putty, with_spaces=True)
    save_dialog.Save.SetFocus()
    save_dialog.Save.Click()
    puttygen.Dialog.MenuItem('File->Exit').Click()
    # remove OpenSSH private key file because it's no longer needed
    os.remove(priv_key_file)
    os.rename(priv_key_file_putty, priv_key_file)

  return fingerprint
