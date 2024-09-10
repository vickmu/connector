import logging

from migrator.util.config import QBConfig
from migrator.util.excel_loader import ExcelLoader
from migrator.util.logger_config import setup_logging

from .services.migrations.migration_manager import MigrationManager
from .util.gui import get_dsn_name, get_file_location, get_migration_type
setup_logging()
logger = logging.getLogger(__name__)


def main():
    logger.info("Starting the migration process...")

    # Step 1: Get Excel file location
    file_path = get_file_location()
    if not file_path:
        logger.error("No file selected, exiting.")
        return

    logger.info(f"Excel file selected: {file_path}")

    # Step 2: Get DSN name
    dsn_name = get_dsn_name()
    if not dsn_name:
        logger.error("No DSN entered, exiting.")
        return

    logger.info(f"DSN entered: {dsn_name}")

    # Step 3: Get Migration type via buttons
    migration_type = get_migration_type()
    if not migration_type:
        logger.error("No migration type selected, exiting.")
        return

    logger.info(f"Migration type selected: {migration_type}")

    # Step 4: Load Excel data
    excel_loader = ExcelLoader(file_path)
    bills_df, bill_items_df, sales_receipts_df, sales_receipt_items_df, customers_df = (
        excel_loader.load_data()
    )
    logger.info("Excel data loaded successfully.")

    # Step 5: Connect to QuickBooks via QODBC
    qb_config = QBConfig(dsn_name)
    conn = qb_config.connect()

    if conn:
        # Prepare migration data as a dictionary
        migration_data = {
            'bills': bills_df,
            'bill_items': bill_items_df,
            'sales_receipts': sales_receipts_df,
            'sales_receipt_items': sales_receipt_items_df,
            'customers': customers_df,
        }

        # Step 6: Run the migration based on the selected type using MigrationManager
        migration_manager = MigrationManager(conn, migration_data)
        migration_manager.run_migration(migration_type)

if __name__ == "__main__":
    main()
