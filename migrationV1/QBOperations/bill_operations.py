from .qb_operations import QBOperations
import logging
import uuid

logger = logging.getLogger(__name__)

class BillOperations(QBOperations):
    def bill_exists(self, ref_number):
        """Check if a bill already exists based on RefNumber."""
        query = u"SELECT COUNT(*) FROM Bill WHERE RefNumber = ?"
        encoded_ref = self.encode_input(ref_number)
        self.cursor.execute(query, (encoded_ref,))
        result = self.cursor.fetchone()
        return result[0] > 0

    def generate_unique_ref_number(self, original_ref_number):
        """Generate a unique RefNumber if the original is missing or invalid."""
        if not original_ref_number or str(original_ref_number).lower() == 'nan':
            new_uuid = str(uuid.uuid4())[:20]  # Generate UUID and truncate it to 20 characters
            logger.info(f"Original RefNumber is missing or NaN, generating a new one: {new_uuid}")
            return new_uuid
        return original_ref_number

    def format_date(self, date_value):
        """Format the date for QuickBooks insert (in {d'YYYY-MM-DD'} format)."""
        return f"{{d'{date_value.strftime('%Y-%m-%d')}'}}" if date_value else "NULL"

    def insert_bill(self, bill_data):
        """Insert a bill into the database."""
        query = """
        INSERT INTO Bill (VendorRefListID, APAccountRefListID, TxnDate, RefNumber, TermsRefListID, DueDate)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        
        # Encode necessary fields
        vendor_ref = bill_data['VendorRefListID']  # No encoding needed, direct reference
        ap_account_ref = bill_data['APAccountRefListID']  # No encoding needed, direct reference
        txn_date = self.format_date(bill_data['TxnDate'])  # Format date to {d'YYYY-MM-DD'}
        due_date = self.format_date(bill_data['DueDate'])  # Format date to {d'YYYY-MM-DD'}
        
        # Handle RefNumber generation
        ref_number = self.generate_unique_ref_number(bill_data['RefNumber'])
        
        encoded_memo = self.encode_input(bill_data.get('Memo', ''))  # Default to empty string if missing
        terms_ref = bill_data.get('TermsRefListID', 'Net 30')  # Default terms

        # Log the data being inserted
        logger.info(f"Inserting bill with VendorRefListID={vendor_ref}, APAccountRefListID: {ap_account_ref}, TxnDate={txn_date},"
        f"DueDate={due_date}, RefNumber={ref_number}, Memo={encoded_memo}")

        try:
            # Insert the bill header
            self.cursor.execute(query, (vendor_ref, ap_account_ref, txn_date, ref_number, terms_ref, due_date))
            
            # Retrieve the generated transaction ID (TxnID)
            self.cursor.execute("SELECT @@IDENTITY AS TxnID")
            txn_id = self.cursor.fetchone().TxnID

        except Exception as e:
            logger.error(f"Error inserting bill: {str(e)}")
            raise

        return txn_id

    def insert_bill_item_line(self, bill_item_data, ref_number, is_last_line=False):
        """Insert a BillItemLine into the database."""
        query = """
        INSERT INTO BillItemLine (VendorRefListID, RefNumber, ItemLineItemRefListID, ItemLineDesc, ItemLineCost, ItemLineAmount, FQSaveToCache)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        # Encode necessary fields
        vendor_ref = bill_item_data['VendorRefListID']  # No encoding needed, direct reference
        item_ref = bill_item_data['ItemLineItemRefListID']  # Item reference
        description = bill_item_data['ItemLineDesc']  # Item description
        cost = bill_item_data['ItemLineCost']  # Cost per item
        amount = bill_item_data['ItemLineAmount']  # Total amount
        fq_save_to_cache = 0 if is_last_line else 1  # Set FQSaveToCache

        try:
            # Insert the bill item line
            self.cursor.execute(query, (vendor_ref, ref_number, item_ref, description, cost, amount, fq_save_to_cache))
            logger.info(f"Inserting BillItemLine with RefNumber={ref_number}, FQSaveToCache={fq_save_to_cache}")

        except Exception as e:
            logger.error(f"Error inserting BillItemLine: {str(e)}")
            raise
    
    def list_bills_by_ref_number(self): 
        """List all bills by RefNumber."""
        query = u"SELECT RefNumber FROM Bill"
        self.cursor.execute(query)
        return self.cursor.fetchall()