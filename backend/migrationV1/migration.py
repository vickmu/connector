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
        # Try to open salesreciptitem-cache first to save time
        try:
            logger.info(f"Opening salesreceiptitem cache...")
            target_salesreceiptitem = pd.read_excel('salesreceiptitem-cache.xlsx')
            logger.info(f"salesreceiptitem cache found")
            
        except FileNotFoundError:
            logger.info(f"salesreceiptitem cache not found, fetching from database...")
            salesreceiptitem = self.sales_receipt_ops.get_sales_receipt_id_after_date('2023-01-01')
            target_salesreceiptitem = pd.DataFrame(salesreceiptitem, columns=['RefNumber'])
            target_salesreceiptitem.to_excel('salesreceiptitem-cache.xlsx', index=False)
            logger.info(f"Customers cache created")
        
        try: 
            logger.info(f"Starting Item Retrieval")
            target_items_df = pd.read_excel('items-cache.xlsx')
            logger.info("items cache found")

        except FileNotFoundError:
            logger.info(f"item cache not found, fetching from database...")
            item = self.item_ops.list_items_by_name()
            target_items_df = pd.DataFrame(item, columns=['Name', 'ListID'])
            target_items_df.to_excel('item-cache.xlsx', index=False)
            logger.info(f"items cache created")

        if target_items_df.empty: 
            logger.error(f'Could not retrieve items. {target_items_df.size}')
            return 

        items_dict = target_items_df.to_dict('records')

        for _, receipt in self.sales_receipt_items_df.iterrows():
            if receipt['RefNumber'] in target_salesreceiptitem:
                logger.info(f"Sales Receipt '{receipt['RefNumber']}' already exists, skipping insertion.")
                continue

            receipt_items = self.sales_receipt_items_df[self.sales_receipt_items_df['RefNumber'] == receipt['RefNumber']]
            for index, receipt_item in receipt_items.iterrows():
                is_last_line = (index == receipt_items.index[-1])
                receipt_item['SalesReceiptLineItemRefListID'] = items_dict[receipt_item['ListID']] 
                self.sales_receipt_ops.insert_sales_receipt_item_line(receipt_item, receipt['RefNumber'], is_last_line=is_last_line)
    
    def migrate_customers(self): 
        logger.info("Starting customer migration...")
        
        # Try to open customers-cache first to save time
        try:
            logger.info(f"Opening customers cache...")
            target_customers = pd.read_excel('customers-cache.xlsx')
            logger.info(f"Customers cache found")
            
        except FileNotFoundError:
            logger.info(f"Customers cache not found, fetching from database...")
            customers = self.customer_ops.list_customers_by_fullname()        
            target_customers = pd.DataFrame(customers, columns=['Name'])
            target_customers.to_excel('customers-cache.xlsx', index=False)
            logger.info(f"Customers cache created")
        
        customers_name_list = target_customers['Name'].to_list()    
        
        customers_types_list = self.customer_ops.list_customer_types()        
        logger.info(f"Constructing CustomerType: ID")
        id_type = {key[1]: key[0] for key in customers_types_list}
        id_type['Default Customer Type'] = 'DWK'
        logger.info(f'Customers list: {customers_name_list[:30]}')
        for _, customer in self.customers_df.iterrows():
            logger.info(f"customer['Name']: {customer['Name']}")

            if customer['Name'] in customers_name_list:
                logger.info(f'Skipping {customer['Name']}, exists in target_customers')
                continue
            customer['CustomerTypeRefListID'] = id_type[
                customer.get('CustomerTypeRefFullName', 'Default Customer Type')
            ]
            self.customer_ops.insert_customer(customer)
    
    def migrate_customers_and_sales_receipt_items(self): 
        """
        Iterates through the sales receipt items, checks if a customer ListID exists 
            If customer does not exist, creates a custdomer
        
        """
        logger.info('Migrating customers and sales receipts')
        customers = self.customer_ops.list_customers_by_fullname()
        customers_names_list = [customer[0] for customer in customers]

        for _, receipt_line_item in self.sales_receipt_items_df: 
            if receipt_line_item['RefNumber'] : 
                pass
            
        
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