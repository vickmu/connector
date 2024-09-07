import logging
import pandas as pd

logger = logging.getLogger(__name__)

class QBOperations:
    def __init__(self, connection):
        self.conn = connection
        self.cursor = self.conn.cursor()

    def encode_input(self, value):
        """Utility to encode the input values to UTF-8, handling NaN and None."""
        encoding = 'utf-8'
        # Handle NaN or None values by converting them to an empty string
        if value is None or pd.isna(value):
            return ""  # Return empty string for NaN or None
        
        # Handle strings
        if isinstance(value, str):
            return value.encode(encoding)
        
        # Handle bytes (return as-is)
        elif isinstance(value, bytes):
            return value.decode(encoding)  # Convert bytes to string if needed
        
        # Handle integers and floats (convert to string and then encode)
        elif isinstance(value, (int, float)):
            return str(value).encode(encoding)
        
        # Handle lists and tuples (recursively encode each item)
        elif isinstance(value, (list, tuple)):
            return [self.encode_input(item) for item in value]
        
        # Handle dictionaries (recursively encode each key and value)
        elif isinstance(value, dict):
            return {self.encode_input(k): self.encode_input(v) for k, v in value.items()}
        
        # Return as-is for other types
        return value

    def commit(self):
        """Commit the transaction."""
        self.conn.commit()

    def close(self):
        """Close the database connection."""
        self.cursor.close()
        self.conn.close()
