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
            return pd.DataFrame()

    def load_data(self):
        available_sheet_names = pd.ExcelFile(self.file_path).sheet_names
        logger.info(f"Available sheets: {available_sheet_names}")
        
        logger.info(f'Loading Bill table')
        bills_df = self.load_sheet('Bill') if 'Bill' in available_sheet_names else pd.DataFrame()
        logger.info(f'Loading BillItem table')
        bill_items_df = self.load_sheet('BillItemLine') if 'BillItemLine' in available_sheet_names else pd.DataFrame()
        logger.info(f'Loading SalesReceipt table')
        sales_receipts_df = self.load_sheet('SalesReceipt') if 'SalesReceipt' in available_sheet_names else pd.DataFrame()
        logger.info(f'Loading SalesReceiptItemLine table')
        sales_receipt_items_df = self.load_sheet('SalesReceiptItemLine') if 'SalesReceiptItemLine' in available_sheet_names else pd.DataFrame()
        logger.info(f'Loading Customer table')
        customers_df = self.load_sheet('Customer') if 'Customer' in available_sheet_names else pd.DataFrame()
        
        return bills_df, bill_items_df, sales_receipts_df, sales_receipt_items_df, customers_df
