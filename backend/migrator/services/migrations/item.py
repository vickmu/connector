import logging

logger = logging.getLogger(__name__)

class ItemMigration:
    def __init__(self, item_ops, items_df):
        self.item_ops = item_ops
        self.items_df = items_df

    def migrate(self):
        logger.info("Starting item migration...")
        items = self.item_ops.list_items_by_name()
        items_name_list = [item[0] for item in items]

        for _, item in self.items_df.iterrows():
            if item['Name'] in items_name_list:
                logger.info(f"Item '{item['Name']}' already exists, skipping insertion.")
                continue
            logger.info(f"Inserting item: {item['Name']}")
            self.item_ops.insert_item(item)
