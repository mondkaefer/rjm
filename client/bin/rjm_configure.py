import os
import sys
import getpass
import traceback
import cer.client.ssh as ssh
import cer.client.util.config as config

def print_underscored(msg):
    print(msg)
    print('#' * len(msg))


def read_config_file_input():
    while True:
        user = input("Your NeSI cluster username: ").strip()
        if user:
            break
        else:
            print("NeSI cluster username must not be empty")

    password1 = getpass.getpass(prompt='NeSI password: ')
    password2 = getpass.getpass(prompt='Repeat password: ')
    if password1 != password2:
        print('Passwords don\'t match. Please start over')
        sys.exit(1)

    qr_secret = getpass.getpass(prompt='QR code secret: ')

    while True:
        default_project_code = input("Default project code: ").strip()
        if default_project_code:
            break
        else:
            print("Default project code must not be empty")

    lander_host = input("Name of lander node [%s]: " % config.DEFAULT_LANDER_HOST).strip()
    if not lander_host:
        lander_host = config.DEFAULT_LANDER_HOST

    login_host = input("Name of cluster login node [%s]: " % config.DEFAULT_LOGIN_HOST).strip()
    if not login_host:
        login_host = config.DEFAULT_LOGIN_HOST

    suggestion = '/nesi/nobackup/%s/%s/rjm-jobs' % (default_project_code, user)
    default_remote_directory = input("Default remote directory [%s]: " % suggestion).strip()
    if not default_remote_directory:
        default_remote_directory = suggestion

    uploads_file = input(
        "Name of file in each job directory to specify files to be uploaded [%s]: " % config.DEFAULT_UPLOAD).strip()
    if not uploads_file:
        uploads_file = config.DEFAULT_UPLOAD

    downloads_file = input(
        "Name of file in each job directory to specify files to be downloaded [%s]: " % config.DEFAULT_DOWNLOAD).strip()
    if not downloads_file:
        downloads_file = config.DEFAULT_DOWNLOAD

    default_remote_bin_dir = '/nesi/project/%s/rjm_bin' % default_project_code
    remote_bin_dir = input(
        "Remote directory where server-side rjm binaries are stored [%s]: " % default_remote_bin_dir).strip()
    if not remote_bin_dir:
        remote_bin_dir = default_remote_bin_dir

    password_cache_ttl_sec = input(
        "Max time in days for the passwords to be cached [%s]: " % config.DEFAULT_PASSWORD_CACHE_TTL_DAYS).strip()
    if not password_cache_ttl_sec:
        password_cache_ttl_sec = config.DEFAULT_PASSWORD_CACHE_TTL_DAYS
    password_cache_ttl_sec = int(password_cache_ttl_sec) * 24 * 60 * 60

    return (lander_host, login_host, user, password1, qr_secret, default_project_code, default_remote_directory,
        uploads_file, downloads_file, remote_bin_dir, password_cache_ttl_sec)


print('')
print_underscored('Creating configuration file %s. Need some information.' % config.get_config_file())
lander_host, login_host, user, password, qr_secret, default_project_code, default_remote_directory, uploads_file, \
    downloads_file, remote_bin_dir, password_cache_ttl_sec = read_config_file_input()

config.create_config_file(lander_host, login_host, default_project_code, default_remote_directory,
                          uploads_file, downloads_file, remote_bin_dir)

print('')
print_underscored('Setting up password store (this may take a few seconds)')
config.setup_security()
config.setup_password_store(user, password, qr_secret, password_cache_ttl_sec)
print('')

print('')
print_underscored('Setting up remote job management scripts')
print('Do you want to copy the remote scripts onto the server?')
copy_scripts = input('Enter \'y\' for yes or any other key for no: ')
if copy_scripts == 'y':
    # check for existence of server-side scripts
    for acronym in config.SERVER_SIDE_SCRIPTS.keys():
        script = config.SERVER_SIDE_SCRIPTS[acronym]
        if not os.path.isfile(script):
            print('Cannot find file %s in current directory' % script)
            sys.exit(1)

    # set up SSH connection
    try:
        ssh_conn = ssh.open_connection_with_config()
    except:
        print('Failed to set up SSH connection')
        print(traceback.format_exc())
        sys.exit(1)

    ssh.run('mkdir -p %s ' % remote_bin_dir, ssh_conn)
    sftp = ssh_conn.open_sftp()

    for acronym in config.SERVER_SIDE_SCRIPTS.keys():
        script = config.SERVER_SIDE_SCRIPTS[acronym]
        print('Uploading file %s' % script)
        destination = '%s/%s' % (remote_bin_dir, script)
        sftp.put(script, destination)
        ssh.run('chmod ug+x %s' % destination, ssh_conn)

    ssh.close_connection(ssh_conn)
print('')

print('Done')
