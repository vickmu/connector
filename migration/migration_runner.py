import logging
from config import QBConfig
from excel_loader import ExcelLoader
from qb_operations import QBOperations
from logger_config import setup_logging
setup_logging()

logger = logging.getLogger(__name__)


def main():
    logger.info("Starting the migration process...")
    
    # Step 1: Load Excel data
    excel_loader = ExcelLoader('QB-DUMP-2-TEST-NEW-VENDOR.xlsx')
    vendors_df, items_df, bills_df, bill_items_df = excel_loader.load_data()
    logger.info("Excel data loaded successfully.")
    
    # Step 2: Connect to QuickBooks via QODBC
    qb_config = QBConfig('DSN2')
    conn = qb_config.connect()

    if conn:
        qb_ops = QBOperations(conn)

        # Step 3: Insert Vendors
        vendors = qb_ops.list_vendors_by_name()
        vendors_name_list = [vendor[0] for vendor in vendors]

        for _, vendor in vendors_df.iterrows():
            if vendor['Name'] in vendors_name_list:
                logger.info(f"Vendor '{vendor['Name']}' already exists, skipping insertion.")
                continue
            logger.info(f"Inserting vendor: {vendor['Name']}")
            qb_ops.insert_vendor(vendor)
            break
        # Step 4: Insert Items
        for _, item in items_df.iterrows():
            qb_ops.insert_item(item)

        # Step 5: Insert Bills and BillItemLine
        txn_ids = {}
        for _, bill in bills_df.iterrows():
            txn_id = qb_ops.insert_bill(bill)
            if txn_id:
                txn_ids[bill['RefNumber']] = txn_id

        # Step 6: Insert BillItemLine for each Bill
        for _, bill_item in bill_items_df.iterrows():
            txn_id = txn_ids.get(bill_item['RefNumber'])
            if txn_id:
                qb_ops.insert_bill_item(bill_item, txn_id)

        # Commit the changes and close the connection
        qb_ops.commit()
        qb_ops.close()

if __name__ == "__main__":
    main()
