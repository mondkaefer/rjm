import sys
import traceback
import cer.client.util as util
import cer.client.ssh as ssh
from paramiko import Agent

fingerprint = None
parser = None

try:
  parser = util.get_config_parser()
  fingerprint = parser.get('MAIN', 'ssh_fingerprint')
except:
  print >> sys.stderr, "Error: Failed to load configuration file"
  print >> sys.stderr, traceback.format_exc()
  sys.exit(1)

if util.platform_is_windows():
  ssh.add_private_key_to_agent(util.get_priv_ssh_key())
  print 'Done'
else:
  found = False
  agent = Agent()
  agent_keys = agent.get_keys()
  if agent_keys:
    fingerprint = fingerprint.replace(':','')
    for key in agent_keys:
      if key.get_fingerprint().encode('hex') == fingerprint:
        found = True

  if found:
    print 'SSH key already registered with agent. Nothing to do.'
  else:
    print 'SSH key not registered with agent. Adding...'
    ssh.add_private_key_to_agent(util.get_priv_ssh_key())
    print 'Done'