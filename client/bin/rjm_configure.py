import os
import re
import string
import getpass
import cer.client.util.config as config
import cer.client.ssh as ssh

default_host = 'login.uoa.nesi.org.nz'

def print_underscored(msg):
  print msg
  print '#' * len(msg)

def read_passphrase():
  passphrase = getpass.getpass(prompt='%sPassphrase for private key: ' % os.linesep)
  
  # check length
  if len(passphrase) < 8:
    print "Passphrase must be at least 8 characters long."
    return False

  # verify existence of lower-case letter
  if passphrase == passphrase.upper():
    print "Passphrase must contain at least one lower-case letter."
    return False
    
  # verify existence of upper-case letter
  if passphrase == passphrase.lower():
    print "Passphrase must contain at least one upper-case letter."
    return False
    
  # verify existence of digits
  _digits = re.compile('\d')
  if not _digits.search(passphrase):
    print "Passphrase must contain at least one digit."
    return False

  # verify existence of punctuation
  found = False
  for p in string.punctuation:
    if p in passphrase:
      found = True
      break
  if not found:
    print "Passphrase must contain at least one punctuation (%s)." % string.punctuation
    return False
  
  return passphrase

def read_config_file_input():
  host = raw_input("Name of login node [login.uoa.nesi.org.nz]: ").strip()

  while True:
    user = raw_input("Your cluster account name (UPI): ").strip()
    if user:
      break
    else:
      print "Cluster account must not be empty"

  while True:
    default_account = raw_input("Default account/project code: ").strip()
    if default_account:
      break
    else:
      print "Default account/project code must not be empty"

  suggestion = '/projects/%s/%s/rjm-jobs' % (default_account, user)
  default_remote_base_directory = raw_input("Default remote base job directory [%s]: " % suggestion).strip()
  if not default_remote_base_directory:
    default_remote_base_directory = suggestion
    
  if not host:
    host = default_host
  
  return (host, user, default_account, default_remote_base_directory)

passphrase1 = None
passphrase2 = None
old_pub_key = None    

print_underscored('Creating configuration directory')
config.create_config_dir()

if os.path.isfile(config.get_pub_ssh_key()):
  with open(config.get_pub_ssh_key(), "r") as f:
    old_pub_key = f.read().strip()

print ''
print_underscored('Creating SSH key pair')
print 'The passphrase for the private key must'
print ' * be at least 8 characters in length'
print ' * contain numbers'
print ' * contain at least one upper case letter'
print ' * contain at least one lower case letter'
print ' * contain at least one punctuation symbol (%s)' % string.punctuation

while True:
  passphrase1 = read_passphrase()
  if passphrase1:
    break
  
while True:
  passphrase2 = getpass.getpass(prompt='%sPlease repeat passphrase: ' % os.linesep)
  if passphrase1 == passphrase2:
    break
  else:
    print "Passphrases don't match."
  
print ''
print 'Generating SSH key pair. This may take a few seconds...'
fingerprint = ssh.create_ssh_rsa_key_pair(passphrase1)

print ''
print_underscored('Creating configuration file. Need some information.')
host, user, default_account, default_remote_base_directory = read_config_file_input()
config.create_config_file(host, user, fingerprint, default_account, default_remote_base_directory)

print ''
print_underscored('Uploading public key to login node')
f = open(config.get_pub_ssh_key(), "r")
pubkey = f.read()
f.close()
uni_password = getpass.getpass(prompt='Enter you University password: ')
conn = ssh.open_connection_username_password(host, user, uni_password)
authz_keys = '${HOME}/.ssh/authorized_keys'
tmpfile = '${HOME}/.ssh/authorized_keys.tmp'
if old_pub_key:
  ssh.run('''cat %s | grep -v '%s' > %s ''' % (authz_keys, old_pub_key, tmpfile), conn)
  ssh.run('''mv %s %s ''' % (tmpfile, authz_keys), conn)
ssh.run('''echo '%s' >> %s''' % (pubkey, authz_keys), conn)
ssh.close_connection(conn)

print ''
print 'Done'

