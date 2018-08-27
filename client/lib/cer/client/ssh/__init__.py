import time
import pyotp
import logging
import socket
import paramiko
import cer.client.util as util
import cer.client.util.config as config
from cer.client.pypass.passwordstore import PasswordStore


try:
    from BytesIO import BytesIO
except ImportError:
    from io import BytesIO

logging.getLogger('paramiko.transport').addHandler(logging.NullHandler())

def open_connection(lander_host, login_host, user, password, qr_secret):
    """
    Open an SSH connection and return the connection object.
    """

    totp = pyotp.TOTP(qr_secret)

    def my_pw_gen():
        yield password
        otp = totp.now()
        while config.has_otp_been_used(otp):
            util.get_log().debug('waiting for otp to become invalid')
            time.sleep(1)
            otp = totp.now()
        config.write_last_otp(otp)
        yield otp
        yield password
        # NeSI doesn't allow for the same token to be used twice. So wait for new token.
        while totp.verify(otp):
            util.get_log().debug('waiting for otp to become invalid')
            time.sleep(1)
        otp = totp.now()
        config.write_last_otp(otp)
        yield otp

    pw_gen = my_pw_gen()

    def otp_handler(title, instructions, prompt_list):
        return [next(pw_gen) for (prompt, echo) in prompt_list]

    try:

        util.get_log().debug('setting up ssh connection to cluster')
        lander_client = paramiko.SSHClient()
        lander_client.set_missing_host_key_policy(paramiko.AutoAddPolicy)

        # try to connect to setup transport etc
        try:
            util.get_log().debug('connecting to %s' % lander_host)
            lander_client.connect(lander_host, username=user, password=password)
        except paramiko.ssh_exception.SSHException:
            pass
        else:
            # expected to fail with 2-factor
            raise RuntimeError("Expected to fail")

        lander_client.get_transport().auth_interactive(username=user, handler=otp_handler)

        # open channel to lander
        local_addr = (lander_host, 22)
        dest_addr = (login_host, 22)
        tunnel = lander_client.get_transport().open_channel('direct-tcpip', dest_addr, local_addr)

        # connect to login host
        mahuika_client = paramiko.SSHClient()
        mahuika_client.set_missing_host_key_policy(paramiko.AutoAddPolicy)

        # try to connect to setup transport etc
        try:
            util.get_log().debug('setting up tunnel to %s' % login_host)
            mahuika_client.connect(lander_host, username=user, sock=tunnel, password=password)
        except paramiko.ssh_exception.SSHException:
            # if 2-factor is required
            mahuika_client.get_transport().auth_interactive(username=user, handler=otp_handler)
        else:
            # if no 2-factor required
            pass
        return mahuika_client
    except socket.gaierror:
        raise Exception('Unable to connect to login host %s. Check network connectivity.' % login_host)


def open_connection_with_config():
    config.setup_security()
    conf = config.get_config()
    cluster = conf['CLUSTER']
    ps = PasswordStore(config.get_password_store_location())
    username = ps.get_decrypted_password('nesi/username')
    password = ps.get_decrypted_password('nesi/password')
    qr_secret = ps.get_decrypted_password('nesi/qr_secret')
    return open_connection(cluster['lander_host'], cluster['login_host'], username, password, qr_secret)


def close_connection(connection):
    """
    Close an SSH connection.
    """
    if connection:
        connection.close()


def run(command_and_args, connection):
    """
    Execute a command on a remote host via SSH.
    If a connection is provided, it will be used. The connection will not be closed after the remote
    command execution. Otherwise a new connection is created, and closed after the remote command execution.
    """
    stdout = BytesIO()
    stderr = BytesIO()
    tmpstdin, tmpstdout, tmpstderr = connection.exec_command(command_and_args)
    stdout.write(tmpstdout.read())
    stderr.write(tmpstderr.read())
    rc = tmpstdout.channel.recv_exit_status()
    return rc, stdout.getvalue().decode("utf-8"), stderr.getvalue().decode("utf-8")
