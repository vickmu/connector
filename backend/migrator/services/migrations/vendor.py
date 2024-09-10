import logging 

logger = logging.getLogger(__name__)

class VendorMigration:
    def __init__(self, vendor_ops, vendors_df):
        self.vendor_ops = vendor_ops
        self.vendors_df = vendors_df

    def migrate(self):
        logger.info("Starting vendor migration...")
        vendors = self.vendor_ops.list_vendors_by_name()
        vendors_name_list = [vendor[0] for vendor in vendors]

        for _, vendor in self.vendors_df.iterrows():
            if vendor['Name'] in vendors_name_list:
                continue
            logger.info(f"Inserting vendor: {vendor['Name']}")
            self.vendor_ops.insert_vendor(vendor)
