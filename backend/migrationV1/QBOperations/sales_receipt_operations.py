from .qb_operations import QBOperations
import logging

logger = logging.getLogger(__name__)


class SalesReceiptOperations(QBOperations):
    def get_sales_receipts(self):
        query = "SELECT * FROM SalesReceipt"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_sales_receipt_by_id(self, id):
        query = "SELECT * FROM SalesReceipt WHERE Id = ?"
        self.cursor.execute(query, [id])
        return self.cursor.fetchone()

    def list_sales_receipts_by_ref_number(self):
        query = "SELECT RefNumber FROM SalesReceipt"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_sales_receipt_id_after_date(self, date):
        formatted_date = f"{{d'{date}'}}" if date else "NULL"
        query = f"SELECT Id FROM SalesReceipt WHERE TxnDate > '{formatted_date}"
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def insert_sales_receipt_item_line(
        self, sales_item_data, ref_number, is_last_line=False
    ):
        # Format the SQL query with the variables directly
        query = f"""
        INSERT INTO SalesReceiptLine (
            CustomerRefListID,
            TxnDate,
            RefNumber,
            PaymentMethodRefFullName,
            Subtotal,
            TotalAmount,
            Memo,
            SalesReceiptLineDesc,
            SalesReceiptLineQuantity,
            SalesReceiptLineAmount,
            SalesReceiptLineRate,
            FQSaveToCache
        ) 
        VALUES (
            '{self.encode_input(sales_item_data['CustomerRefListID'])}', 
            '{self.format_date(sales_item_data['TxnDate'])}', 
            '{self.encode_input(ref_number)}', 
            '{self.encode_input(sales_item_data['PaymentMethodRefListID'])}', 
            '{self.encode_input(sales_item_data['Subtotal'])}', 
            '{self.encode_input(sales_item_data['TotalAmount'])}', 
            '{self.encode_input(sales_item_data['Memo'])}', 
            '{self.encode_input(sales_item_data['SalesReceiptLineDesc'])}', 
            '{self.encode_input(sales_item_data['SalesReceiptLineQuantity'])}', 
            '{self.encode_input(sales_item_data['SalesReceiptLineAmount'])}', 
            '{self.encode_input(sales_item_data['SalesReceiptLineRate'])}', 
            '{0 if is_last_line else 1}'
        );
        """
        try:
            # Execute the query
            self.cursor.execute(query)
        except Exception as e:
            logger.error(f"Error inserting sales receipt item line: {str(e)}")
            raise
        logger.info(
            f"Sales receipt item line for {sales_item_data['SalesReceiptLineItemRefFullName']} inserted successfully."
        )