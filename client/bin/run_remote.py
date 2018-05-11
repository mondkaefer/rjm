import os
import sys
import cer.client.util.config as config
import cer.client.ssh as ssh

conf = config.get_config()
cluster = conf['CLUSTER']

ssh_conn = ssh.open_connection_ssh_agent(cluster['remote_host'], cluster['remote_user'], cluster['ssh_priv_key_file'])
cmd = ' '.join(sys.argv[1:])
rc, stdout, stderr = ssh.run(cmd, ssh_conn)
ssh.close_connection(ssh_conn)

print('[rc]:%s%s%s' % (os.linesep,rc,os.linesep))
print('[stdout]:%s%s%s' % (os.linesep,stdout.strip(),os.linesep))
print('[stderr]:%s%s%s' % (os.linesep,stderr.strip(),os.linesep))
