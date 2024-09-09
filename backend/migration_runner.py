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
        filetypes=[("Excel Files", "*.xlsx"), ("All Files", "*.*")]
    )
    return file_path

def get_dsn_name():
    """Prompt the user to enter the DSN name."""
    root = tk.Tk()
    root.withdraw()  # Hide the main tkinter window
    dsn_name = simpledialog.askstring("DSN", "Enter the target DSN name:")
    return dsn_name

def get_migration_type():
    """Display a Tkinter dropdown dialog to select the type of migration."""
    root = tk.Tk()
    root.title("Select Migration Type")

    migration_type = tk.StringVar()
    migration_type.set(MigrationType.FULL.value)  # Set default value

    # Create a dropdown (combobox) for migration type selection
    dropdown = ttk.Combobox(root, textvariable=migration_type, state="readonly")
    dropdown['values'] = [mt.value for mt in MigrationType]
    dropdown.pack(pady=10)

    def on_submit():
        root.quit()  # Close the window when Submit is clicked

    # Add a Submit button
    submit_button = tk.Button(root, text="Submit", command=on_submit)
    submit_button.pack(pady=10)

    root.mainloop()

    return migration_type.get()

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

    # Step 3: Get Migration type
    migration_type = get_migration_type()
    logger.info(f"Migration type selected: {migration_type}")

    # Step 4: Load Excel data
    excel_loader = ExcelLoader(file_path)
    bills_df, bill_items_df, sales_receipts_df, sales_receipt_items_df, customers_df = excel_loader.load_data()
    logger.info("Excel data loaded successfully.")

    # Step 5: Connect to QuickBooks via QODBC
    qb_config = QBConfig(dsn_name)
    conn = qb_config.connect()

    if conn:
        # Step 6: Run the migration based on the selected type
        if migration_type == MigrationType.BILLS.value:
            migration = Migration(
                conn, 
                bills_df=bills_df,
                bill_items_df=bill_items_df
            )
        elif migration_type == MigrationType.SALES_RECEIPTS.value:
            migration = Migration(
                conn,
                sales_receipts_df=sales_receipts_df,
                sales_receipt_items_df=sales_receipt_items_df
            )
        elif migration_type == MigrationType.CUSTOMERS.value:
            migration = Migration(
                conn,
                customers_df=customers_df
            )
        elif migration_type == MigrationType.FULL.value:
            migration = Migration(
                conn, 
                bills_df=bills_df,
                bill_items_df=bill_items_df,
                sales_receipts_df=sales_receipts_df,
                sales_receipt_items_df=sales_receipt_items_df,
                customers_df=customers_df
            )

        migration.run_migration(migration_type)

if __name__ == "__main__":
    main()
