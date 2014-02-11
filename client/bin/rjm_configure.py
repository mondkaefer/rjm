import os
import re
import sys
import string
import getpass
import cer.client.util as util
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
    

print_underscored('Creating configuration directory')
util.create_config_dir()

passphrase1 = ''
passphrase2 = ''

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
print_underscored('Creating configuration file')
host = raw_input("Please enter the name of login node [login.uoa.nesi.org.nz]: ").strip()
if not host:
  host = default_host
user = raw_input("Please enter your UPI: ")
util.create_config_file(host, user, fingerprint)

print ''
print_underscored('Uploading public key to login node')
f = open(util.get_pub_ssh_key(), "r")
pubkey = f.read()
f.close()
uni_password = getpass.getpass(prompt='Enter you University password: ')
conn = ssh.open_connection_username_password(host, user, uni_password)
ssh.run('''echo "%s" >> ~/.ssh/authorized_keys''' % pubkey, conn)
ssh.close_connection(conn)

print ''
print 'Done'

