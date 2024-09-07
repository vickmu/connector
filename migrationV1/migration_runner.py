import logging
import tkinter as tk
from tkinter import filedialog, simpledialog
from util.config import QBConfig
from util.excel_loader import ExcelLoader
from migration import Migration
from util.logger_config import setup_logging

setup_logging()

logger = logging.getLogger(__name__)

def get_file_location():
    """Open a file dialog to select an Excel file."""
    root = tk.Tk()
    root.withdraw()  # Hide the main tkinter window
    file_path = filedialog.askopenfilename(
        title="Select Excel File",
        filetypes=[("Excel Files", "*.xlsx"), ("All Files", "*.*")]
    )
    return file_path

def get_dsn_name():
    """Prompt the user to enter the DSN name."""
    root = tk.Tk()
    root.withdraw()  # Hide the main tkinter window
    dsn_name = simpledialog.askstring("DSN", "Enter the DSN name:")
    return dsn_name

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

    # Step 3: Load Excel data
    excel_loader = ExcelLoader(file_path)
    vendors_df, items_df, bills_df, bill_items_df = excel_loader.load_data()
    logger.info("Excel data loaded successfully.")

    # Step 4: Connect to QuickBooks via QODBC
    qb_config = QBConfig(dsn_name)
    conn = qb_config.connect()

    if conn:
        # Step 5: Run the migration
        migration = Migration(conn, vendors_df, items_df, bills_df, bill_items_df)
        migration.run_migration()

if __name__ == "__main__":
    main()
