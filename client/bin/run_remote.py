import os
import sys
import traceback
import argparse
import cer.client.ssh as ssh
import cer.client.util as util

# information displayed as help by argparse
h = {
  'logfile':
    'logfile. if not specified, all messages will be printed to the terminal.',
  'loglevel':
    'level of log verbosity. default: %s. ' % util.DEFAULT_LOG_LEVEL.lower() +
    'the higher the log level, more information will be printed.',
  'command_and_args':
    'command to run and arguments. needs to be in quotes.'
}

parser = argparse.ArgumentParser(description='cancel a batch of jobs and wait for the cancellation to complete.')
parser.add_argument('-l','--logfile', help=h['logfile'], required=False, type=str)
parser.add_argument('-ll','--loglevel', help=h['loglevel'], required=False, type=str, choices=['debug','info','warn','error','critical'])
parser.add_argument('command_and_args', nargs='*', help=h['command_and_args'])
args = parser.parse_args()

if len(args.command_and_args) == 0:
    print('No command specified')
    sys.exit(1)

if args.logfile or args.loglevel:
  util.setup_logging(args.logfile, args.loglevel)
log = util.get_log()

# Set up SSH connection
try:
    ssh_conn = ssh.open_connection_with_config()
except:
    log.error('Failed to set up SSH connection')
    log.error(traceback.format_exc())
    sys.exit(1)

rc, stdout, stderr = ssh.run(args.command_and_args[0], ssh_conn)
ssh.close_connection(ssh_conn)

print('[rc]:%s%s%s' % (os.linesep,rc,os.linesep))
print('[stdout]:%s%s%s' % (os.linesep,stdout.strip(),os.linesep))
print('[stderr]:%s%s%s' % (os.linesep,stderr.strip(),os.linesep))
