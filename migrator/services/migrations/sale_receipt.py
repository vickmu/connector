import logging 
from ...util.cache import CacheUtil

logger = logging.getLogger(__name__)

class SalesReceiptMigration:
    def __init__(self, sales_receipt_ops, item_ops, sales_receipt_items_df):
        self.sales_receipt_ops = sales_receipt_ops
        self.item_ops = item_ops
        self.sales_receipt_items_df = sales_receipt_items_df

    def migrate(self):
        logger.info("Starting sales receipt and item migration...")

        target_salesreceiptitem = CacheUtil.load_or_fetch_cache(
            'salesreceiptitem-cache.xlsx', 
            lambda: self.sales_receipt_ops.get_sales_receipt_id_after_date('2023-01-01'), 
            ['RefNumber'], 
            'salesreceiptitem'
        )

        target_items_df = CacheUtil.load_or_fetch_cache(
            'items-cache.xlsx', 
            self.item_ops.list_items_by_name, 
            ['Name', 'ListID'], 
            'items'
        )

        if target_items_df.empty: 
            logger.error(f'Could not retrieve items. {target_items_df.size}')
            return 

        items_dict = {row['ListID']: row['Name'] for _, row in target_items_df.iterrows()}

        for _, receipt in self.sales_receipt_items_df.iterrows():
            if receipt['RefNumber'] in target_salesreceiptitem['RefNumber'].values:
                logger.info(f"Sales Receipt '{receipt['RefNumber']}' already exists, skipping insertion.")
                continue

            receipt_items = self.sales_receipt_items_df[self.sales_receipt_items_df['RefNumber'] == receipt['RefNumber']]
            for index, receipt_item in receipt_items.iterrows():
                is_last_line = (index == receipt_items.index[-1])
                receipt_item['SalesReceiptLineItemRefListID'] = items_dict.get(receipt_item['ListID'], None)
                if receipt_item['SalesReceiptLineItemRefListID'] is None:
                    logger.warning(f"Item with ListID '{receipt_item['ListID']}' not found in items dictionary.")
                    continue

                self.sales_receipt_ops.insert_sales_receipt_item_line(receipt_item, receipt['RefNumber'], is_last_line=is_last_line)
