import os
import re
import string
import getpass
import cer.client.util.config as config
import cer.client.ssh as ssh

PASSPHRASE_CHECKS = {
 'min_length': 8,
 'contains_uppercase': False,
 'contains_lowercase': False,
 'contains_digit': False,
 'contains_punctuation': False,
}

def print_underscored(msg):
  print msg
  print '#' * len(msg)

def read_passphrase():
  passphrase = getpass.getpass(prompt='%sPassphrase for private key: ' % os.linesep)
  
  # check length
  if len(passphrase) < PASSPHRASE_CHECKS['min_length']:
    print "Passphrase must be at least 8 characters long."
    return False

  # verify existence of lower-case letter
  if PASSPHRASE_CHECKS['contains_lowercase'] and (passphrase == passphrase.upper()):
    print "Passphrase must contain at least one lower-case letter."
    return False
    
  # verify existence of upper-case letter
  if PASSPHRASE_CHECKS['contains_uppercase'] and (passphrase == passphrase.lower()):
    print "Passphrase must contain at least one upper-case letter."
    return False
    
  # verify existence of digits
  if PASSPHRASE_CHECKS['contains_digit'] and not re.compile('\d').search(passphrase):
    print "Passphrase must contain at least one digit."
    return False

  # verify existence of punctuation
  if PASSPHRASE_CHECKS['contains_punctuation']:
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
  host = raw_input("Name of cluster login node [%s]: " % config.DEFAULT_REMOTE_HOST).strip()
  if not host:
    host = config.DEFAULT_REMOTE_HOST

  while True:
    user = raw_input("Your cluster login name (UPI): ").strip()
    if user:
      break
    else:
      print "Cluster login name (UPI) must not be empty"

  while True:
    default_project_code = raw_input("Default project code: ").strip()
    if default_project_code:
      break
    else:
      print "Default project code must not be empty"

  suggestion = '/projects/%s/%s/rjm-jobs' % (default_project_code, user)
  default_remote_directory = raw_input("Default remote directory [%s]: " % suggestion).strip()
  if not default_remote_directory:
    default_remote_directory = suggestion

  uploads_file = raw_input("Name of file in each job directory to specify files to be uploaded [%s]: " % config.DEFAULT_UPLOAD).strip()
  if not uploads_file:
    uploads_file = config.DEFAULT_UPLOAD
    
  downloads_file = raw_input("Name of file in each job directory to specify files to be downloaded [%s]: " % config.DEFAULT_DOWNLOAD).strip()
  if not downloads_file:
    downloads_file = config.DEFAULT_DOWNLOAD
  
  return (host, user, default_project_code, default_remote_directory, uploads_file, downloads_file)

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
print 'The passphrase for the private key'
print ' * must be at least %s characters in length' % PASSPHRASE_CHECKS['min_length']
print ' * may contain punctuation, numbers, upper case letters and lower case letters.'
print ''
print 'Note: Weak passwords may result in your cluster account being compromised and your data being stolen.'

while True:
  passphrase1 = read_passphrase()
  if passphrase1:
    break
  
while True:
  passphrase2 = getpass.getpass(prompt='Repeat passphrase: ')
  if passphrase1 == passphrase2:
    break
  else:
    print "Passphrases don't match."

os.environ['PRIV_KEY_PASSWD'] = passphrase1  
print ''
print 'Generating SSH key pair. This may take up to a minute to complete...'
fingerprint = ssh.create_ssh_rsa_key_pair(passphrase1)

print ''
print_underscored('Creating configuration file. Need some information.')
host, user, default_project_code, default_remote_directory, uploads_file, downloads_file = read_config_file_input()
config.create_config_file(host, user, fingerprint, default_project_code, default_remote_directory, uploads_file, downloads_file)

print ''
print_underscored('Uploading public key to login node')
f = open(config.get_pub_ssh_key(), "r")
pubkey = f.read()
f.close()
uni_password = getpass.getpass(prompt='Enter the University Password which goes with your UPI: ')
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
