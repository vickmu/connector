from .bill import BillMigration
from .item import ItemMigration
from .sale_receipt import SalesReceiptMigration
from .vendor import VendorMigration

from ...operations.qb.qb import QBOperations
from ...operations.qb.bill import BillOperations
from ...operations.qb.customer import CustomerOperations
from ...operations.qb.sales_receipt import SalesReceiptOperations
from ...operations.qb.vendor import VendorOperations
from ...operations.qb.item import ItemOperations

from ...models.migration_type import MigrationType

import logging
import traceback

logger = logging.getLogger(__name__)

class MigrationManager:
    def __init__(self, connection, migration_data):
        self.vendor_migration = VendorMigration(VendorOperations(connection), migration_data['vendors'])
        self.item_migration = ItemMigration(ItemOperations(connection), migration_data['items'])
        self.bill_migration = BillMigration(BillOperations(connection), migration_data['bills'], migration_data['bill_items'])
        self.sales_receipt_migration = SalesReceiptMigration(SalesReceiptOperations(connection), ItemOperations(connection), migration_data['sales_receipt_items'])

    def run_migration(self, migration_type: MigrationType):
        try:
            if migration_type == MigrationType.VENDORS.value:
                self.vendor_migration.migrate()

            elif migration_type == MigrationType.ITEMS.value:
                self.item_migration.migrate()

            elif migration_type == MigrationType.BILLS.value:
                self.bill_migration.migrate()

            elif migration_type == MigrationType.SALES_RECEIPTS.value:
                self.sales_receipt_migration.migrate()

            QBOperations(self.connection).commit()
            
        except Exception as e: 
            logger.error(f"An error occurred during migration: {e}")
            logger.error(traceback.format_exc())
            QBOperations(self.connection).rollback()
        finally:
            QBOperations(self.connection).close()
