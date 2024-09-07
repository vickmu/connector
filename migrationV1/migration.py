import logging
from QBOperations.vendor_operations import VendorOperations
from QBOperations.item_operations import ItemOperations
from QBOperations.bill_operations import BillOperations
import traceback
logger = logging.getLogger(__name__)

class Migration:
    def __init__(self, connection, vendors_df, items_df, bills_df, bill_items_df):
        self.connection = connection
        self.vendors_df = vendors_df
        self.items_df = items_df
        self.bills_df = bills_df
        self.bill_items_df = bill_items_df
        self.vendor_ops = VendorOperations(connection)
        self.item_ops = ItemOperations(connection)
        self.bill_ops = BillOperations(connection)

    def migrate_vendors(self):
        logger.info("Starting vendor migration...")
        vendors = self.vendor_ops.list_vendors_by_name()
        vendors_name_list = [vendor[0] for vendor in vendors]

        for _, vendor in self.vendors_df.iterrows():
            if vendor['Name'] in vendors_name_list:
                continue
            logger.info(f"Inserting vendor: {vendor['Name']}")
            self.vendor_ops.insert_vendor(vendor)

    def migrate_items(self):
        logger.info("Starting item migration...")
        items = self.item_ops.list_items_by_name()
        items_name_list = [item[0] for item in items]

        for _, item in self.items_df.iterrows():
            if item['Name'] in items_name_list:
                logger.info(f"Item '{item['Name']}' already exists, skipping insertion.")
                continue
            logger.info(f"Inserting item: {item['Name']}")
            self.item_ops.insert_item(item)    
    
    def migrate_bills_and_items(self):
        """Migrate bills and their associated items."""
        logger.info("Starting bill and item migration...")
        txn_ids = {}

        bills = self.bill_ops.list_bills_by_ref_number()
        bills_ref_list = [bill[0] for bill in bills]
        
        # Migrate each bill and its items
        for _, bill in self.bills_df.iterrows():
            if bill['RefNumber'] in bills_ref_list:
                logger.info(f"Bill '{bill['RefNumber']}' already exists, skipping insertion.")
                continue

            # Insert Bill Item Lines first, save each line to cache except the last one
            bill_items = self.bill_items_df[self.bill_items_df['RefNumber'] == bill['RefNumber']]
            for index, bill_item in bill_items.iterrows():
                is_last_line = (index == bill_items.index[-1])
                self.bill_ops.insert_bill_item_line(bill_item, bill['RefNumber'], is_last_line=is_last_line)

            # Once all items are inserted, create the Bill
            txn_id = self.bill_ops.insert_bill(bill)
            if txn_id:
                txn_ids[bill['RefNumber']] = txn_id

        return txn_ids

    def run_migration(self):
        """Run the full migration process."""
        try:
            self.migrate_vendors()
            # self.migrate_items()
            self.migrate_bills_and_items()

            # Commit the transaction after all steps
            logger.info("Committing transaction...")
            self.vendor_ops.commit()  # Committing through vendor_ops or any operation class
            logger.info("Migration process completed successfully.")
        except Exception as e:
            logger.error(f"An error occurred during migration: {e}")
            logger.error(traceback.format_exc())
            self.vendor_ops.conn.rollback()
            logger.info("Transaction rolled back.")
        finally:
            self.vendor_ops.close()  # Ensure connection is closed
