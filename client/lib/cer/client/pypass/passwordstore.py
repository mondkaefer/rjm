# Taken from https://github.com/aviau/python-pass and adjusted as follows:
# - to work with Windows
# - removed git features
# - removed generate_password feature

import os
import subprocess
import re
import cer.client.util as util
from cer.client.util import config as config

from .entry_type import EntryType

GPG_BIN=None

# Find the right gpg binary
cmd = 'which'
if util.platform_is_windows():
    cmd = 'where'

if subprocess.call(
        [cmd, 'gpg2'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE) == 0:
    GPG_BIN = 'gpg2'
elif subprocess.call(
        [cmd, 'gpg'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE) == 0:
    GPG_BIN = 'gpg'
else:
    raise Exception("Could not find GPG")


class PasswordStore(object):
    """This is a Password Store

    :param path: The path of the password-store. By default,
                 '$home/.password-store'.
    """

    def __init__(
            self,
            path):
        self.path = path

        # Read the .gpg-id
        gpg_id_file = os.path.join(path, '.gpg-id')
        if os.path.isfile(gpg_id_file):
            self.gpg_id = open(gpg_id_file, 'r').read().strip()
        else:
            raise Exception("could not find .gpg-id file")


    def get_passwords_list(self):
        """Returns a list of the passwords in the store

        :returns: Example: ['Email/bob.net', 'example.com']
        """
        passwords = []

        for root, dirnames, filenames in os.walk(self.path):
            for filename in filenames:
                if filename.endswith('.gpg'):
                    path = os.path.join(root, filename.replace('.gpg', ''))
                    simplified_path = path.replace('%s%s' % (self.path, os.path.sep), '').replace(os.path.sep, '/')
                    passwords.append(simplified_path)

        return passwords

    def get_decrypted_password(self, path, entry=None):
        """Returns the content of the decrypted password file

        :param path: The path of the password to be decrypted. Example:
                     'email.com'
        :param entry: The entry to retreive. (EntryType enum)
        """
        passfile_path = os.path.realpath(
            os.path.join(
                self.path,
                path + '.gpg'
            )
        )

        gpg = subprocess.Popen(
            [
                GPG_BIN,
                '--homedir',
                '%s%s%s' % (config.get_config_dir(), os.path.sep, config.GPG_DIR_NAME),
                '--quiet',
                '--batch',
                '--use-agent',
                '-d',
                passfile_path
            ],
            shell=False,
            stdout=subprocess.PIPE,
        )
        gpg.wait()

        if gpg.returncode == 0:
            decrypted_password = gpg.stdout.read().decode()

            if entry == EntryType.username:
                usr = re.search(
                    '(?:username|user|login): (.+)',
                    decrypted_password
                )
                if usr:
                    return usr.groups()[0]
            elif entry == EntryType.password:
                pw = re.search('(?:password|pass): (.+)', decrypted_password)
                if pw:
                    return pw.groups()[0]
                else:  # If there is no match, password is the first line
                    return decrypted_password.split('\n')[0]
            elif entry == EntryType.hostname:
                hostname = re.search(
                    '(?:host|hostname): (.+)', decrypted_password
                )
                if hostname:
                    return hostname.groups()[0]
            else:
                return decrypted_password

    def insert_password(self, path, password):
        """Encrypts the password at the given path

        :param path: Where to insert the password. Ex: 'passwordstore.org'
        :param password: The password to insert, can be multi-line
        """

        passfile_path = os.path.realpath(
            os.path.join(self.path, path + '.gpg')
        )

        if not os.path.isdir(os.path.dirname(passfile_path)):
            os.makedirs(os.path.dirname(passfile_path))

        gpg = subprocess.Popen(
            [
                GPG_BIN,
                '--homedir',
                '%s%s%s' % (config.get_config_dir(), os.path.sep, config.GPG_DIR_NAME),
                '-e',
                '-r', self.gpg_id,
                '--batch',
                '--use-agent',
                '--no-tty',
                '--yes',
                '-o',
                passfile_path
            ],
            shell=False,
            stdin=subprocess.PIPE
        )

        gpg.stdin.write(password.encode())
        gpg.stdin.close()
        gpg.wait()

    @staticmethod
    def init(gpg_id, path):
        """Creates a password store to the given path

        :param gpg_id: Default gpg key identification used for encryption and
                       decryption. Example: '3CCC3A3A'
        :param path: Where to create the password store. By default, this is
                     $home/.password-store
        :returns: PasswordStore object
        """

        # Create a folder at the path
        if not os.path.exists(path):
            os.makedirs(path)

        gpg_id_path = os.path.join(path, '.gpg-id')
        if os.path.exists(gpg_id_path) is False:
            # Create .gpg_id and put the gpg id in it
            with open(os.path.join(path, '.gpg-id'), 'a') as gpg_id_file:
                gpg_id_file.write(gpg_id + '\n')

        return PasswordStore(path)
