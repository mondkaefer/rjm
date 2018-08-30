import os
import sys
import ntpath
import argparse
import traceback
import cer.client.ssh as ssh
import cer.client.util as util

# information displayed as help by argparse
h = {
    'logfile':
        'logfile. if not specified, all messages will be printed to the terminal.',
    'loglevel':
        'level of log verbosity. default: %s. ' % util.DEFAULT_LOG_LEVEL.lower() +
        'the higher the log level, more information will be printed.',
    'remote_directory':
        'directory where the files are uploaded to.' +
        'will be created if it doesn\'t exist yet',
    'file':
        'file to be uploaded.'
}

parser = argparse.ArgumentParser(description='cancel a batch of jobs and wait for the cancellation to complete.')
parser.add_argument('-l', '--logfile', help=h['logfile'], required=False, type=str)
parser.add_argument('-ll', '--loglevel', help=h['loglevel'], required=False, type=str,
                    choices=['debug', 'info', 'warn', 'error', 'critical'])
parser.add_argument('-d', '--remote_directory', help=h['remote_directory'], required=True, type=str)
parser.add_argument('file', nargs='*', help=h['file'])
args = parser.parse_args()

if len(args.file) == 0:
    print('No file specified')
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

try:
    rc, stdout, stderr = ssh.run('mkdir -p %s' % args.remote_directory, ssh_conn)
    sftp = ssh_conn.open_sftp()
    for f in args.file:
        print('Uploading file %s to %s' % (f, args.remote_directory))
        destination = '%s/%s' % (args.remote_directory, ntpath.basename(f))
        sftp.put(f, destination)
except:
    log.error('An error occurred when uploading files')
    log.error(traceback.format_exc())
    sys.exit(1)
finally:
    ssh.close_connection(ssh_conn)
