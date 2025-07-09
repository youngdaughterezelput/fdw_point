import keyring

class SecurityManager:
    @staticmethod
    def store_password(key, password):
        keyring.set_password("hfpoint", key, password)
    
    @staticmethod
    def get_password(key):
        return keyring.get_password("hfpoint", key)
    
    @staticmethod
    def clear_credentials(connection_name):
        try:
            keyring.delete_password("hfpoint", f"{connection_name}_user")
            keyring.delete_password("hfpoint", f"{connection_name}_pass")
        except:
            pass

class AuthManager:
    @classmethod
    def save_credentials(cls, connection_name, user, password):
        SecurityManager.store_password(f"{connection_name}_user", user)
        SecurityManager.store_password(f"{connection_name}_pass", password)
    
    @classmethod
    def get_credentials(cls, connection_name):
        user = SecurityManager.get_password(f"{connection_name}_user")
        password = SecurityManager.get_password(f"{connection_name}_pass")
        return user, password
    
    @classmethod
    def delete_credentials(cls, connection_name):
        SecurityManager.clear_credentials(connection_name)