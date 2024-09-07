import logging
import pandas as pd

logger = logging.getLogger(__name__)

class QBOperations:
    def __init__(self, connection=None):
        self.conn = connection
        self.cursor = None if connection is None else self.conn.cursor()

    def encode_input(self, value):
        """Utility to handle NaN, None, strings, bytes, and numbers appropriately."""
        # Handle NaN or None values by converting them to an empty string
        if isinstance(value, (list, tuple)):
            return [self.encode_input(item) for item in value]
        
        if pd.isna(value):
            return ""  # Return empty string for NaN or None
        
        # Handle bytes (convert to string)
        if isinstance(value, bytes):
            return value.decode('utf-8')  # Decode bytes to string
        
        # Handle strings (return as-is)
        if isinstance(value, str):
            return value
        
        # Handle integers and floats (convert to string)
        if isinstance(value, (int, float)):
            return str(value)
        
        # Handle dictionaries (recursively process each key and value)
        if isinstance(value, dict):
            return {self.encode_input(k): self.encode_input(v) for k, v in value.items()}
        
        # Return the value as-is for other data types
        return value



    def commit(self):
        """Commit the transaction."""
        self.conn.commit()

    def close(self):
        """Close the database connection."""
        self.cursor.close()
        self.conn.close()
