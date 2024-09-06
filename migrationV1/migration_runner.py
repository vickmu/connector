import logging
from config import QBConfig
from excel_loader import ExcelLoader
from migration import Migration
from logger_config import setup_logging

setup_logging()

logger = logging.getLogger(__name__)

def main():
    logger.info("Starting the migration process...")

    # Step 1: Load Excel data
    excel_loader = ExcelLoader('test_data/QB-DUMP-2-TEST-NEW-VENDOR.xlsx')
    vendors_df, items_df, bills_df, bill_items_df = excel_loader.load_data()
    logger.info("Excel data loaded successfully.")

    # Step 2: Connect to QuickBooks via QODBC
    qb_config = QBConfig('DSN2')
    conn = qb_config.connect()

    if conn:
        # Step 3: Run the migration
        migration = Migration(conn, vendors_df, items_df, bills_df, bill_items_df)
        migration.run_migration()

if __name__ == "__main__":
    main()
