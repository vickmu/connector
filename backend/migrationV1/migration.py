import logging
import pandas as pd
from .QBOperations.customer_operations import CustomerOperations
from .QBOperations.vendor_operations import VendorOperations
from .QBOperations.item_operations import ItemOperations
from .QBOperations.bill_operations import BillOperations
from .QBOperations.sales_receipt_operations import SalesReceiptOperations
from .QBOperations.qb_operations import QBOperations
from migrationV1.migration_type import MigrationType

import traceback
logger = logging.getLogger(__name__)

class Migration:
    def __init__(
        self,
        connection,
        vendors_df=pd.DataFrame(), 
        items_df=pd.DataFrame(),
        bills_df=pd.DataFrame(), 
        bill_items_df=pd.DataFrame(),
        sales_receipts_df=pd.DataFrame(),
        sales_receipt_items_df=pd.DataFrame(),
        customers_df=pd.DataFrame()
    ):
        
        self.connection = connection
        self.vendors_df = vendors_df
        self.items_df = items_df
        self.bills_df = bills_df
        self.bill_items_df = bill_items_df
        self.sales_receipt_df = sales_receipts_df
        self.customers_df = customers_df
        self.sales_receipt_items_df = sales_receipt_items_df
        
        self.vendor_ops = VendorOperations(connection)
        self.item_ops = ItemOperations(connection)
        self.bill_ops = BillOperations(connection)
        self.sales_receipt_ops = SalesReceiptOperations(connection)
        self.customer_ops = CustomerOperations(connection)

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

    
    def migrate_sales_receipts_and_items(self): 
        logger.info("Starting sales receipt and item migration...")
        sales_receipts = self.sales_receipt_ops.list_sales_receipts_by_ref_number()        
        sales_receipts_ref_list = [receipt[0] for receipt in sales_receipts]
        
        for _, receipt in self.sales_receipt_df.iterrows():
            if receipt['RefNumber'] in sales_receipts_ref_list:
                logger.info(f"Sales Receipt '{receipt['RefNumber']}' already exists, skipping insertion.")
                continue

            # Insert Sales Receipt Item Lines first, save each line to cache except the last one
            receipt_items = self.sales_receipt_items_df[self.sales_receipt_items_df['RefNumber'] == receipt['RefNumber']]
            for index, receipt_item in receipt_items.iterrows():
                is_last_line = (index == receipt_items.index[-1])
                
                self.sales_receipt_ops.insert_sales_receipt_item_line(receipt_item, receipt['RefNumber'], is_last_line=is_last_line)
    
    def migrate_customers(self): 
        logger.info("Starting customer migration...")
        customers = self.customer_ops.list_customers_by_fullname()
        customers_name_list = [customer[0] for customer in customers]
        customers_types_list = self.customer_ops.list_customer_types()
        id_type = {key: value for key, value in customers_types_list}
        id_type['Default Customer Type'] = 'DWK'
        for _, customer in self.customer_df.iterrows():
            if customer['FullName'] in customers_name_list:
                continue            
            customer['CustomerTypeRefListID'] = id_type[
                customer.get('CustomerTypeRefFullName', 'Default Customer Type')
            ]
            self.customer_ops.insert_customer(customer)
        
    def run_migration(self, migration_type: MigrationType):
        """Run the full migration process."""
        try: 
            if migration_type == MigrationType.VENDORS.value:
                self.migrate_vendors()
                QBOperations(self.connection).commit()
                
            elif migration_type == MigrationType.ITEMS.value:
                self.migrate_items()
                QBOperations(self.connection).commit()
            
            elif migration_type == MigrationType.BILLS.value:
                self.migrate_bills_and_items()
                QBOperations(self.connection).commit()
                
            elif migration_type == MigrationType.SALES_RECEIPTS.value:
                self.migrate_sales_receipts_and_items()
                QBOperations(self.connection).commit()
            
            elif migration_type == MigrationType.CUSTOMERS.value:
                self.migrate_customers()
                QBOperations(self.connection).commit()
                
        except Exception as e: 
            logger.error(f"An error occurred during migration: {e}")
            logger.error(traceback.format_exc())
            self.vendor_ops.conn.rollback()
        finally:
            self.vendor_ops.close()