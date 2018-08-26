import os
import sys
import traceback
import cer.client.ssh as ssh
import cer.client.util as util

log = util.get_log()

# Set up SSH connection
try:
    ssh_conn = ssh.open_connection_with_config()
except:
    log.error('Failed to set up SSH connection')
    log.error(traceback.format_exc())
    sys.exit(1)

cmd = ' '.join(sys.argv[1:])
rc, stdout, stderr = ssh.run(cmd, ssh_conn)
ssh.close_connection(ssh_conn)

print('[rc]:%s%s%s' % (os.linesep,rc,os.linesep))
print('[stdout]:%s%s%s' % (os.linesep,stdout.strip(),os.linesep))
print('[stderr]:%s%s%s' % (os.linesep,stderr.strip(),os.linesep))
