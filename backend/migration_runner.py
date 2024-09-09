import logging
import tkinter as tk
from tkinter import filedialog, simpledialog, ttk

import pandas as pd
from migrationV1.util.config import QBConfig
from migrationV1.util.excel_loader import ExcelLoader
from migrationV1.migration_type import MigrationType
from migrationV1.migration import Migration
from migrationV1.util.logger_config import setup_logging

setup_logging()

logger = logging.getLogger(__name__)


def get_file_location():
    """Open a file dialog to select an Excel file."""
    root = tk.Tk()
    root.withdraw()  # Hide the main tkinter window
    file_path = filedialog.askopenfilename(
        title="Select QB Data Export Excel File",
        filetypes=[("Excel Files", "*.xlsx"), ("All Files", "*.*")],
    )
    return file_path


def get_dsn_name():
    """Prompt the user to enter the DSN name."""
    root = tk.Tk()
    root.withdraw()  # Hide the main tkinter window
    dsn_name = simpledialog.askstring("DSN", "Enter the target DSN name:")
    return dsn_name


def get_migration_type():
    """Display a Tkinter window with buttons for each migration type."""
    root = tk.Tk()
    root.title("Select Migration Type")

    # Variable to store the selected migration type
    selected_migration_type = tk.StringVar()

    def select_migration_type(migration_value):
        selected_migration_type.set(migration_value)  # Set the selected type
        root.quit()  # Quit the main loop after selecting

    # Create a button for each migration type
    for mt in MigrationType:
        tk.Button(
            root,
            text=mt.value,
            command=lambda mt_value=mt.value: select_migration_type(mt_value),
        ).pack(pady=5)

    root.mainloop()  # Run the window until a button is pressed

    root.destroy()  # Close the window

    # Return the selected migration type
    return selected_migration_type.get()

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
        # Step 6: Run the migration based on the selected type
        if migration_type == MigrationType.BILLS.value:
            migration = Migration(conn, bills_df=bills_df, bill_items_df=bill_items_df)
        elif migration_type == MigrationType.SALES_RECEIPTS.value:
            migration = Migration(
                conn,
                sales_receipts_df=sales_receipts_df,
                sales_receipt_items_df=sales_receipt_items_df,
            )
        elif migration_type == MigrationType.CUSTOMERS.value:
            migration = Migration(conn, customers_df=customers_df)
        elif migration_type == MigrationType.FULL.value:
            migration = Migration(
                conn,
                bills_df=bills_df,
                bill_items_df=bill_items_df,
                sales_receipts_df=sales_receipts_df,
                sales_receipt_items_df=sales_receipt_items_df,
                customers_df=customers_df,
            )

        migration.run_migration(migration_type)

if __name__ == "__main__":
    main()
