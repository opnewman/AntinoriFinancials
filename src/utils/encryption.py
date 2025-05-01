import os
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import logging

logger = logging.getLogger(__name__)

class EncryptionService:
    """
    Service for encrypting and decrypting sensitive financial data
    """
    def __init__(self):
        # Get encryption key from environment variable or generate a new one
        self.key = os.environ.get("ENCRYPTION_KEY")
        if not self.key:
            logger.warning("ENCRYPTION_KEY not found in environment variables. Using a default key (not recommended for production).")
            # Create a default key for development - this should be replaced in production
            salt = b'antinori_salt'
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=100000,
            )
            self.key = base64.urlsafe_b64encode(kdf.derive(b"default_key"))
        else:
            # If key is provided as a string, ensure it's properly encoded
            if isinstance(self.key, str):
                self.key = self.key.encode()
        
        # Initialize Fernet cipher
        self.cipher = Fernet(self.key)
    
    def encrypt(self, value):
        """
        Encrypts a value
        
        Args:
            value: The value to encrypt (string or numeric)
            
        Returns:
            str: Encrypted value as a string
        """
        if value is None:
            return None
            
        # Convert value to string if it's not already
        if not isinstance(value, str):
            value = str(value)
            
        # Encrypt the value
        try:
            encrypted_value = self.cipher.encrypt(value.encode())
            return encrypted_value.decode()
        except Exception as e:
            logger.error(f"Encryption error: {str(e)}")
            raise
    
    def decrypt(self, encrypted_value):
        """
        Decrypts an encrypted value
        
        Args:
            encrypted_value: The encrypted value as a string
            
        Returns:
            str: Decrypted value
        """
        if encrypted_value is None:
            return None
            
        try:
            # Decrypt the value
            decrypted_value = self.cipher.decrypt(encrypted_value.encode()).decode()
            return decrypted_value
        except Exception as e:
            logger.error(f"Decryption error: {str(e)}")
            raise
    
    def decrypt_to_float(self, encrypted_value):
        """
        Decrypts an encrypted value and converts it to float
        
        Args:
            encrypted_value: The encrypted value as a string
            
        Returns:
            float: Decrypted value as float
        """
        decrypted = self.decrypt(encrypted_value)
        if decrypted is None:
            return 0.0
            
        try:
            # Clean the value (remove $, commas, etc.) and convert to float
            cleaned_value = decrypted.replace('$', '').replace(',', '').strip()
            return float(cleaned_value)
        except ValueError as e:
            logger.error(f"Error converting decrypted value to float: {str(e)}")
            return 0.0

# Create a singleton instance
encryption_service = EncryptionService()
