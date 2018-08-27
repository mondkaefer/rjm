import os
import cer.client.util as util
import shutil
import gnupg
import cer.client.pypass.passwordstore as pw_store
from configparser import ConfigParser

# name of the configuration directory
CONFIG_DIR_NAME = '.remote_jobs'
# name of the configuration file
CONFIG_FILE_NAME = 'config.ini'
# name of the gnupg dir
GPG_DIR_NAME = 'gnupg'
# name of the password store dir
PASSWORD_STORE_DIR_NAME = 'password_store'
# default lander host
DEFAULT_LANDER_HOST = 'lander02.nesi.org.nz'
# default login host
DEFAULT_LOGIN_HOST = 'login.mahuika.nesi.org.nz'
# name of the file that contains the list of files to be uploaded before the job starts
DEFAULT_UPLOAD = 'rjm_uploads.txt'
# name of the file that contains the list of files to be downloaded after the job is done
DEFAULT_DOWNLOAD = 'rjm_downloads.txt'
# default cache ttl
DEFAULT_PASSWORD_CACHE_TTL_DAYS = 7

class ConfigReader(ConfigParser):
    def as_dict(self):
        ''' return the configuration as dictionary '''
        d = dict(self._sections)
        for k in d:
            d[k] = dict(self._defaults, **d[k])
            d[k].pop('__name__', None)
        return d


def get_config_dir():
    """ get the absolute path of the configuration directory. """
    if util.platform_is_windows():
        directory = '%s%s%s' % (os.environ['USERPROFILE'], os.path.sep, CONFIG_DIR_NAME)
    else:
        directory = '%s%s%s' % (os.environ['HOME'], os.path.sep, CONFIG_DIR_NAME)
    return directory


def setup_security():
    """ set up environment variables used by pass and gnupg """
    os.environ['PASSWORD_STORE_DIR'] = '%s%s%s' % (get_config_dir(), os.path.sep, PASSWORD_STORE_DIR_NAME)
    os.environ['GNUPGHOME'] = '%s%s%s' % (get_config_dir(), os.path.sep, GPG_DIR_NAME)


def get_password_store_location():
    """ get the absolute path of the password store """
    return '%s%s%s' % (get_config_dir(), os.path.sep, PASSWORD_STORE_DIR_NAME)


def get_config_file():
    """ get the absolute path of the configuration file. """
    return '%s%s%s' % (get_config_dir(), os.path.sep, CONFIG_FILE_NAME)


def get_config():
    """ return the main configuration as dictionary. """
    config_file = get_config_file()
    if not os.path.isfile(config_file):
        raise Exception('configuration file %s does not exist.' % config_file)
    cr = ConfigReader()
    cr.read(config_file)
    return cr.as_dict()


def create_config_dir():
    """ create the configuration directory.
        if the directory does not yet exist, it will be created.
    """
    config_dir = get_config_dir()
    if not os.path.exists(config_dir):
        os.mkdir(config_dir)
    else:
        if os.path.isdir(config_dir):
            shutil.rmtree(config_dir, ignore_errors=True)
            os.mkdir(config_dir)
        else:
            raise Exception('unexpected error: %s already exists and is not a directory.' % config_dir)



def create_config_file(lander_host, login_host, default_project_code,
                       default_remote_directory, rjm_upload, rjm_download):
    """ create the configuration file.
        if the configuration directory does not exist, it will be created.
    """
    create_config_dir()
    f = open(get_config_file(), "w+")
    f.write('[CLUSTER]%s' % os.linesep)
    f.write('lander_host=%s%s' % (lander_host, os.linesep))
    f.write('login_host=%s%s' % (login_host, os.linesep))
    f.write('default_project_code=%s%s' % (default_project_code, os.linesep))
    f.write('default_remote_directory=%s%s' % (default_remote_directory, os.linesep))
    f.write('remote_prepare_job=%s%s' % ('/home/mfel395/bin/rjm/0.1/prepare_job', os.linesep))
    f.write('remote_submit_job=%s%s' % ('/home/mfel395/bin/rjm/0.1/submit_job', os.linesep))
    f.write('remote_is_job_done=%s%s' % ('/home/mfel395/bin/rjm/0.1/is_job_done', os.linesep))
    f.write('remote_get_job_statuses=%s%s' % ('/home/mfel395/bin/rjm/0.1/get_job_statuses', os.linesep))
    f.write('remote_cancel_jobs=%s%s' % ('/home/mfel395/bin/rjm/0.1/cancel_jobs', os.linesep))
    f.write('%s' % os.linesep)
    f.write('[FILE_TRANSFER]%s' % os.linesep)
    f.write('uploads_file=%s%s' % (rjm_upload, os.linesep))
    f.write('downloads_file=%s%s' % (rjm_download, os.linesep))
    f.write('%s' % os.linesep)
    f.write('[RETRY]%s' % os.linesep)
    f.write('max_attempts=%s%s' % ('5', os.linesep))
    f.write('min_wait_s=%s%s' % ('0.5', os.linesep))
    f.write('max_wait_s=%s%s' % ('5', os.linesep))
    f.close()


def read_job_config_file(job_config_file):
    """ read the local configuration file of a job (ini-format). """
    if not os.path.isfile(job_config_file):
        raise Exception('config file not found: %s' % job_config_file)
    try:
        cr = ConfigReader()
        cr.read(job_config_file)
        crd = cr.as_dict()
        util.get_log().debug('read from %s: %s' % (job_config_file, str(crd)))
    except:
        raise Exception('failed to read from config file %s' % job_config_file)

    return crd


def create_or_update_job_config_file(localdir, props_dict):
    """ create or update metadata file for job in local job directory (ini-format) """
    configfile = '%s%s.job.ini' % (localdir, os.path.sep)
    config = ConfigParser()
    if os.path.isfile(configfile):
        util.get_log().debug('Updating job config file %s with %s' % (configfile, str(props_dict)))
        config.read(configfile)
    else:
        util.get_log().debug('Writing job config file into %s with %s' % (configfile, str(props_dict)))

    for key1 in props_dict.keys():
        if not config.has_section(key1):
            config.add_section(key1)
        for key2 in props_dict[key1].keys():
            config.set(key1, str(key2), str(props_dict[key1][key2]))

    with open(configfile, 'w') as f:
        config.write(f)

    if not os.path.isfile(configfile):
        raise Exception("Creation or update of job config file %s failed." % configfile)


def has_otp_been_used(otp):
    """ check if the given token has been used already """
    ps = pw_store.PasswordStore(get_password_store_location())
    password_list = ps.get_passwords_list()
    if 'nesi/last_otp_used' in password_list:
        last_otp_used = ps.get_decrypted_password('nesi/last_otp_used')
        if last_otp_used == otp:
            return True
        else:
            return False
    else:
        return False


def write_last_otp(otp):
    """ save the last OTP used, so that other scripts/processes don't try to reuse tokens.
        NeSI doesn't accept token reuse.
    """
    ps = pw_store.PasswordStore(get_password_store_location())
    ps.insert_password('nesi/last_otp_used', otp)


def setup_password_store(username, password, qr_secret, password_cache_ttl_sec):
    """ set up password store and store username, password and QR secret """
    gnupg_dir = '%s%s%s' % (get_config_dir(), os.path.sep, GPG_DIR_NAME)
    password_store_dir = '%s%s%s' % (get_config_dir(), os.path.sep, PASSWORD_STORE_DIR_NAME)

    shutil.rmtree(gnupg_dir, ignore_errors=True)
    shutil.rmtree(password_store_dir, ignore_errors=True)

    gpg_key_input = {
        'key_type': 'RSA',
        'key_length': 4096,
        'passphrase': password,
        'name_real': 'Password store for NeSI cluster',
        'name_email': '',
        'expire_date': '10y'
    }

    ### set up gpg
    # set env vars
    setup_security()

    # creating GPG directory
    gpg = gnupg.GPG(gpgbinary=pw_store.GPG_BIN, gnupghome=gnupg_dir, use_agent=True)
    input_data = gpg.gen_key_input(**gpg_key_input)

    # creating GPG key
    key = gpg.gen_key(input_data)
    fingerprint = key.fingerprint

    # create agent config file
    agent_config_file = '%s%s%s' % (gnupg_dir, os.sep, 'gpg-agent.conf')
    if os.path.exists(agent_config_file):
        os.remove(agent_config_file)
    with open(agent_config_file, 'w') as f:
        f.write('default-cache-ttl %s%s' % (password_cache_ttl_sec, os.linesep))
        f.write('max-cache-ttl %s%s' % (password_cache_ttl_sec, os.linesep))

    ### set up pass
    ps = pw_store.PasswordStore.init(fingerprint, password_store_dir)

    # storing NeSI username, password and qr secret
    ps.insert_password('nesi/username', username)
    ps.insert_password('nesi/password', password)
    ps.insert_password('nesi/qr_secret', qr_secret)
