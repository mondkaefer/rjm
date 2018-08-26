import cer.client.util.config as config
import cer.client.pypass.passwordstore as pw_store

# all this script does is unlock the gpg agent

config.setup_security()
conf = config.get_config()
ps = pw_store.PasswordStore(config.get_password_store_location())
ps.get_decrypted_password('nesi/username')
