import logging
import pandas as pd

logger = logging.getLogger(__name__)

class CacheUtil:
    @staticmethod
    def load_or_fetch_cache(file_name, fetch_func, columns, log_name):
        try:
            logger.info(f"Opening {log_name} cache...")
            data_df = pd.read_excel(file_name)
            logger.info(f"{log_name} cache found")
        except FileNotFoundError:
            logger.info(f"{log_name} cache not found, fetching from database...")
            data = fetch_func()
            # Flatten tuples into individual columns
            data_df = pd.DataFrame([list(tup) if isinstance(tup, tuple) else tup for tup in data], columns=columns)
            data_df.to_excel(file_name, index=False)
            logger.info(f"{log_name} cache created")
        return data_df
