import os
import sys
import getpass
import cer.client.util.config as config
from prompt_toolkit import prompt

def print_underscored(msg):
    print(msg)
    print('#' * len(msg))


def read_config_file_input():
    while True:
        user = input("Your NeSI username: ").strip()
        if user:
            break
        else:
            print("NeSI username must not be empty")

    password1 = prompt('NeSI password: ', is_password=True)
    password2 = prompt('Repeat password: ', is_password=True)
    if password1 != password2:
        print('Passwords don\'t match. Please start over')
        sys.exit(1)

    qr_secret = prompt('QR code secret: ', is_password=True)

    while True:
        default_project_code = input("Default project code: ").strip()
        if default_project_code:
            break
        else:
            print("Default NeSI project code must not be empty")

    use_defaults = input(
        'Use default values for the other configuration parameters?%s(Type y or Enter for yes, or any other key for no) [y]? ' % os.linesep)
    if not use_defaults:
        use_defaults = 'y'

    if use_defaults == 'y':
        uploads_file = config.DEFAULT_UPLOAD
        downloads_file = config.DEFAULT_DOWNLOAD
        lander_host = config.DEFAULT_LANDER_HOST
        login_host = config.DEFAULT_LOGIN_HOST
        password_cache_ttl_sec = int(config.DEFAULT_PASSWORD_CACHE_TTL_DAYS) * 24 * 60 * 60
        default_remote_directory = '/nesi/nobackup/%s/%s/rjm-jobs' % (default_project_code, user)
        remote_bin_dir = '/nesi/project/%s/rjm_bin' % default_project_code
    else:
        uploads_file = input(
            "File in each local job directory to specify uploads [%s]: " % config.DEFAULT_UPLOAD).strip()
        if not uploads_file:
            uploads_file = config.DEFAULT_UPLOAD

        downloads_file = input(
            "File in each local job directory to specify downloads [%s]: " % config.DEFAULT_DOWNLOAD).strip()
        if not downloads_file:
            downloads_file = config.DEFAULT_DOWNLOAD

        lander_host = input("Name of lander node [%s]: " % config.DEFAULT_LANDER_HOST).strip()
        if not lander_host:
            lander_host = config.DEFAULT_LANDER_HOST

        login_host = input("Name of cluster login node [%s]: " % config.DEFAULT_LOGIN_HOST).strip()
        if not login_host:
            login_host = config.DEFAULT_LOGIN_HOST

        password_cache_ttl_sec = input(
            "Max time in days for the passwords to be cached [%s]: " % config.DEFAULT_PASSWORD_CACHE_TTL_DAYS).strip()
        if not password_cache_ttl_sec:
            password_cache_ttl_sec = config.DEFAULT_PASSWORD_CACHE_TTL_DAYS
        password_cache_ttl_sec = int(password_cache_ttl_sec) * 24 * 60 * 60

        suggestion = '/nesi/nobackup/%s/%s/rjm-jobs' % (default_project_code, user)
        default_remote_directory = input("Default remote directory [%s]: " % suggestion).strip()
        if not default_remote_directory:
            default_remote_directory = suggestion

        default_remote_bin_dir = '/nesi/project/%s/rjm_bin' % default_project_code
        remote_bin_dir = input(
            "Remote directory where server-side rjm binaries are stored [%s]: " % default_remote_bin_dir).strip()
        if not remote_bin_dir:
            remote_bin_dir = default_remote_bin_dir

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
print('Done')
