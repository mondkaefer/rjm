import getpass
import cer.client.util.config as config
from six.moves import input

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

    password = getpass.getpass(prompt='NeSI password: ')
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

    return (lander_host, login_host, user, password, qr_secret, default_project_code, default_remote_directory,
            uploads_file, downloads_file)


print('')
print_underscored('Creating configuration file %s. Need some information.' % config.get_config_file())
lander_host, login_host, user, password, qr_secret, default_project_code, default_remote_directory, uploads_file, \
    downloads_file = read_config_file_input()

config.create_config_file(lander_host, login_host, default_project_code,
                          default_remote_directory, uploads_file, downloads_file)

print('')
print('Setting up password store (this may take a few seconds)')
config.setup_security()
config.setup_password_store(user, password, qr_secret)
print('')
print('Done')

