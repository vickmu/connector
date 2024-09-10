import logging
import pandas as pd
import uuid

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

    def generate_unique_ref_number(self, original_ref_number):
        """Generate a unique RefNumber if the original is missing or invalid."""
        if not original_ref_number or str(original_ref_number).lower() == 'nan':
            new_uuid = str(uuid.uuid4())[:20]  # Generate UUID and truncate it to 20 characters
            logger.info(f"Original RefNumber is missing or NaN, generating a new one: {new_uuid}")
            return new_uuid
        return original_ref_number

    def format_date(self, date_value):
        """Format the date for QuickBooks insert (in {d'YYYY-MM-DD'} format)."""
        return f"{{d'{date_value.strftime('%Y-%m-%d')}'}}" if date_value else "NULL"
    
    def format_timestamp(self, datetime_value):
        """Format the timestamp for QuickBooks insert (in {ts'YYYY-MM-DD HH:MM:SS'} format)."""
        return f"{{ts'{datetime_value.strftime('%Y-%m-%d %H:%M:%S')}'}}" if datetime_value else "NULL"


    def commit(self):
        """Commit the transaction."""
        self.conn.commit()

    def close(self):
        """Close the database connection."""
        self.cursor.close()
        self.conn.close()
