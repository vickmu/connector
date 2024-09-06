import logging
from QBOperations.vendor_operations import VendorOperations
from QBOperations.item_operations import ItemOperations
from QBOperations.bill_operations import BillOperations

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
                logger.info(f"Vendor '{vendor['Name']}' already exists, skipping insertion.")
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

    def migrate_bills(self):
        logger.info("Starting bill migration...")
        txn_ids = {}

        for _, bill in self.bills_df.iterrows():
            txn_id = self.bill_ops.insert_bill(bill)
            if txn_id:
                txn_ids[bill['RefNumber']] = txn_id

        return txn_ids

    def migrate_bill_items(self, txn_ids):
        logger.info("Starting bill item migration...")

        for _, bill_item in self.bill_items_df.iterrows():
            txn_id = txn_ids.get(bill_item['RefNumber'])
            if txn_id:
                self.bill_ops.insert_bill_item(bill_item, txn_id)

    def run_migration(self):
        """Run the full migration process."""
        try:
            self.migrate_vendors()
            self.migrate_items()
            txn_ids = self.migrate_bills()
            self.migrate_bill_items(txn_ids)

            # Commit the transaction after all steps
            logger.info("Committing transaction...")
            self.vendor_ops.commit()  # Committing through vendor_ops or any operation class
            logger.info("Migration process completed successfully.")
        except Exception as e:
            logger.error(f"An error occurred during migration: {e}")
            self.vendor_ops.conn.rollback()  # Rollback in case of failure
            logger.info("Transaction rolled back.")
        finally:
            self.vendor_ops.close()  # Ensure connection is closed
