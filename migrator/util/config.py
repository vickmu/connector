import pyodbc
import logging

logger = logging.getLogger(__name__)

class QBConfig:
    def __init__(self, dsn_name="DNS-2"):
        self.dsn_name = dsn_name

    def connect(self):
        try:
            conn = pyodbc.connect(f'DSN={self.dsn_name}')
            logger.info(f"Connected to QuickBooks using DSN: {self.dsn_name}")
            return conn
        except pyodbc.Error as e:
            logger.error(f"Error connecting to QuickBooks: {e}")
            return None
