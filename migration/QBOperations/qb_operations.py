import logging

logger = logging.getLogger(__name__)

class QBOperations:
    def __init__(self, connection):
        self.conn = connection
        self.cursor = self.conn.cursor()

    def encode_input(self, value):
        """Utility to encode the input values to UTF-8."""
        encoding = 'utf-8'
        if value is not None:
            if isinstance(value, str):
                return value.encode(encoding)
            if isinstance(value, bytes):
                return value
            if isinstance(value, (int, float)):
                return str(value).encode(encoding)
        return value

    def commit(self):
        """Commit the transaction."""
        self.conn.commit()

    def close(self):
        """Close the database connection."""
        self.cursor.close()
        self.conn.close()
