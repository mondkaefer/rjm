import os
import sys
import cer.client.util as util
import cer.client.ssh as ssh

parser = util.get_config_parser()

host = parser.get('MAIN', 'remote_host')
user = parser.get('MAIN', 'remote_user')
privkey = parser.get('MAIN', 'ssh_priv_key_file')

ssh_conn = ssh.open_connection_ssh_agent(host, user, privkey)
cmd = ' '.join(sys.argv[1:])
rc, stdout, stderr = ssh.run(cmd, ssh_conn)
ssh.close_connection(ssh_conn)

print 'rc:%s###%s%s%s' % (os.linesep,os.linesep,rc,os.linesep)
print 'stdout:%s#######%s%s%s' % (os.linesep,os.linesep,stdout.strip(),os.linesep)
print 'stderr:%s#######%s%s%s' % (os.linesep,os.linesep,stderr.strip(),os.linesep)
