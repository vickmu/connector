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
        bills_df = self.load_sheet('Bill')
        bill_items_df = self.load_sheet('BillItemLine')
        sales_receipts_df = self.load_sheet('SalesReceipt')
        sales_receipt_items_df = self.load_sheet('SalesReceiptItemLine')
        customers_df = self.load_sheet('Customer')
        
        return bills_df, bill_items_df, sales_receipts_df, sales_receipt_items_df, customers_df
