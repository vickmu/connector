import logging 

logger = logging.getLogger(__name__)

class BillMigration:
    def __init__(self, bill_ops, bills_df, bill_items_df):
        self.bill_ops = bill_ops
        self.bills_df = bills_df
        self.bill_items_df = bill_items_df

    def migrate(self):
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
