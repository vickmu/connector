import pandas as pd
import logging 

logger = logging.getLogger(__name__)
class ExcelLoader:
    def __init__(self, file_path):
        self.file_path = file_path

    def load_sheet(self, sheet_name):
        try:
            return pd.read_excel(self.file_path, sheet_name=sheet_name)
        except Exception as e:
            print(f"Error loading {sheet_name}: {e}")
            return None

    def load_data(self):
        # Handle non existing tabs
        if not self.load_sheet('Vendor'):
            logger.warning("Vendor sheet not found in the Excel file.")
        if not self.load_sheet('Item'):
            logger.warning("Item sheet not found in the Excel file.")
        if not self.load_sheet('Bill'):
            logger.warning("Bill sheet not found in the Excel file.")
        if not self.load_sheet('BillItemLine'):
            logger.warning("BillItemLine sheet not found in the Excel file.")
        
        vendors_df = self.load_sheet('Vendor')
        items_df = self.load_sheet('Item')
        bills_df = self.load_sheet('Bill')
        bill_items_df = self.load_sheet('BillItemLine')
        return vendors_df, items_df, bills_df, bill_items_df
