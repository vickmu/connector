from .qb_operations import QBOperations
import logging
import uuid

logger = logging.getLogger(__name__)

class BillOperations(QBOperations):
    def bill_exists(self, ref_number):
        """Check if a bill already exists based on RefNumber."""
        query = "SELECT COUNT(*) FROM Bill WHERE RefNumber = ?"
        self.cursor.execute(query, (ref_number,))
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
        """Insert a bill into the database and log the full query."""
        query = """
        INSERT INTO Bill (VendorRefListID, APAccountRefListID, TxnDate, RefNumber, DueDate)
        VALUES (?, ?, ?, ?, ?)
        """
        
        # Prepare necessary fields
        vendor_ref = bill_data['VendorRefListID']
        ap_account_ref = bill_data['APAccountRefListID']
        txn_date = self.format_date(bill_data['TxnDate'])
        due_date = self.format_date(bill_data['DueDate'])
        
        # Handle RefNumber generation
        ref_number = self.generate_unique_ref_number(bill_data['RefNumber'])
        
        memo = bill_data.get('Memo', '')  # Default to empty string if missing

        # Create full SQL query string with formatted values
        full_query = f"""
        INSERT INTO Bill (VendorRefListID, APAccountRefListID, TxnDate, RefNumber, DueDate)
        VALUES ('{vendor_ref}', '{ap_account_ref}', {txn_date}, '{ref_number}', {due_date})
        """
        logger.info(f"Executing SQL query:\n{full_query}")

        try:
            # Execute the bill insert query
            self.cursor.execute(query, (vendor_ref, ap_account_ref, txn_date, ref_number, due_date))
            
            # Retrieve the generated transaction ID (TxnID)
            self.cursor.execute("SELECT @@IDENTITY AS TxnID")
            txn_id = self.cursor.fetchone().TxnID

        except Exception as e:
            logger.error(f"Error inserting bill: {str(e)}")
            raise

        return txn_id

    def insert_bill_item_line(self, bill_item_data, ref_number, is_last_line=False):
        """Insert a BillItemLine into the database and log the full SQL query with values."""
        
        # Prepare necessary fields
        vendor_ref = bill_item_data['VendorRefListID']
        item_ref = bill_item_data['ItemLineItemRefListID']
        description = bill_item_data['ItemLineDesc'] 
        cost = bill_item_data['ItemLineCost']
        amount = bill_item_data['ItemLineAmount']
        fq_save_to_cache = 0 if is_last_line else 1

        # Create full SQL query string with formatted values
        full_query = f"""
        INSERT INTO BillItemLine (VendorRefListID, RefNumber, ItemLineItemRefListID, ItemLineDesc, ItemLineCost, ItemLineAmount, FQSaveToCache)
        VALUES ('{vendor_ref}', '{ref_number}', '{item_ref}', '{description}', {cost}, {amount}, {fq_save_to_cache})
        """
        logger.info(f"Executing SQL query:\n{full_query}")
        
        try:
            # Execute the bill item line insert query
            query = """
            INSERT INTO BillItemLine (VendorRefListID, RefNumber, ItemLineItemRefListID, ItemLineDesc, ItemLineCost, ItemLineAmount, FQSaveToCache)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            self.cursor.execute(query, (vendor_ref, ref_number, item_ref, description, cost, amount, fq_save_to_cache))

        except Exception as e:
            logger.error(f"Error inserting BillItemLine: {str(e)}")
            raise
    
    def list_bills_by_ref_number(self): 
        """List all bills by RefNumber."""
        query = "SELECT RefNumber FROM Bill"
        self.cursor.execute(query)
        return self.cursor.fetchall()
