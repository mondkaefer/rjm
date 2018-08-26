import sys
import argparse
import traceback
import cer.client.ssh as ssh
import cer.client.job as job
import cer.client.util as util

# information displayed as help by argparse
h = {
  'loglevel':
    'level of log verbosity. default: %s. ' % util.DEFAULT_LOG_LEVEL.lower() +
    'the higher the log level, more information will be printed.'
}

parser = argparse.ArgumentParser(description='get status of all jobs on NeSI cluster, except those that are finishing.')
parser.add_argument('-ll','--loglevel', help=h['loglevel'], required=False, type=str, choices=['debug','info','warn','error','critical'])
args = parser.parse_args()

if args.loglevel:
  util.setup_logging(None, args.loglevel)
log = util.get_log()

ssh_conn = None
log = util.get_log()

# Set up SSH connection
try:
    ssh_conn = ssh.open_connection_with_config()
except:
    log.error('Failed to set up SSH connection')
    log.error(traceback.format_exc())
    sys.exit(1)

# Call remote script to prepare the job
jobmap = {}
try:
    jobmap = job.get_job_statuses(ssh_conn)
except:
    log.error('Remote command to prepare job failed.')
    log.error(traceback.format_exc())
    sys.exit(1)

keys = jobmap.keys()

for key in jobmap.keys():
    print('%s: %s' % (key, jobmap[key]))

try:
    ssh.close_connection(ssh_conn)
except:
    pass
