import time
import pyotp
import logging
import paramiko
import cer.client.util as util
import cer.client.util.config as config

from io import BytesIO
from cer.client.pypass.passwordstore import PasswordStore

logging.getLogger('paramiko.transport').addHandler(logging.NullHandler())

def open_connection(lander_host, login_host, user, password, qr_secret):
    """
    Open an SSH connection and return the transport object.

    Args:
        lander_host (str): name of the lander host (string)
        login_host (str): name of the login host (string)
        user (str) : user name (string)
        password (str): password (string
        qr_secret (str): secret of the QR code

    Returns:
        An open SSH connection (object of type paramiko.Transport)

    Raises:
        Exception if anything goes wrong while setting up the ssh connection
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
        # No 2FA required for second login
        yield ''

    pw_gen = my_pw_gen()

    def otp_handler(title, instructions, prompt_list):
        return [next(pw_gen) for (prompt, echo) in prompt_list]

    try:

        # connect to lander
        util.get_log().debug('setting up ssh connection to %s' % lander_host)
        lander_transport = paramiko.Transport((lander_host, 22))
        lander_transport.start_client()
        util.get_log().debug('authenticating to %s' % lander_host)
        lander_transport.auth_interactive(username=user, handler=otp_handler)

        util.get_log().debug("opening tunnel")
        local_addr = (lander_host, 22)
        dest_addr = (login_host, 22)
        tunnel = lander_transport.open_channel('direct-tcpip', dest_addr, local_addr)

        # connect to mahuika
        util.get_log().debug("connecting to %s through tunnel" % login_host)
        login_transport = paramiko.Transport(sock=tunnel)
        login_transport.start_client()
        util.get_log().debug('authenticating to %s' % login_host)
        login_transport.auth_interactive(username=user, handler=otp_handler)

        return login_transport
    except:
        raise Exception('Unable to setting up connection to cluster. Check network connectivity.')


def open_connection_with_config():
    """
    Open an SSH connection and return the transport object. Use the configuration to get all
    parameters required to set up the connection

    Returns:
        An open SSH connection (object of type paramiko.Transport)
    """

    config.setup_security()
    conf = config.get_config()
    cluster = conf['CLUSTER']
    ps = PasswordStore(config.get_password_store_location())
    username = ps.get_decrypted_password('nesi/username')
    password = ps.get_decrypted_password('nesi/password')
    qr_secret = ps.get_decrypted_password('nesi/qr_secret')
    return open_connection(cluster['lander_host'], cluster['login_host'], username, password, qr_secret)


def close_connection(transport):
    """
    Close an SSH connection.

    Args:
        transport (paramiko.Transport): connection to be closed
    """

    if transport:
        transport.close()


def exec_command(command_and_args, transport):
    """
    Execute a command on a remote host via SSH.
    If a connection is provided, it will be used. The connection will not be closed after the remote
    command execution. Otherwise a new connection is created, and closed after the remote command execution.

    Args:
        command_and_args (str): Command and arguments to be run
        transport (paramiko.Transport): existing and open ssh connection

    Returns:
        rc (int): exit code of the remote process
        stdout (str): Output of the command printed to stdout
        stderr (str): Output of the command printed to stderr
    """

    if not transport:
        raise Exception('transport is not defined. has ssh connection been set up?')
    if not command_and_args or 0 == len(command_and_args.strip()):
        raise Exception('no command specified')

    stdout = BytesIO()
    stderr = BytesIO()
    channel = transport.open_session()
    channel.exec_command(command_and_args)
    rc = channel.recv_exit_status()
    tmp_stdout = channel.makefile('r', -1)
    tmp_stderr = channel.makefile_stderr('r', -1)
    stdout.write(tmp_stdout.read())
    stderr.write(tmp_stderr.read())
    return rc, stdout.getvalue().decode("utf-8"), stderr.getvalue().decode("utf-8")
