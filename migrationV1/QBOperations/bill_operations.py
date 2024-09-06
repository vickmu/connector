from qb_operations import QBOperations

class BillOperations(QBOperations):
    def bill_exists(self, ref_number):
        query = u"SELECT COUNT(*) FROM Bill WHERE RefNumber = ?"
        encoded_ref = self.encode_input(ref_number)
        self.cursor.execute(query, (encoded_ref,))
        result = self.cursor.fetchone()
        return result[0] > 0

    def insert_bill(self, bill_data):
        query = u"""
        INSERT INTO Bill (VendorRefFullName, TxnDate, DueDate, RefNumber, Memo, AmountDue)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        encoded_vendor = self.encode_input(bill_data['VendorRefFullName'])
        encoded_txn_date = bill_data['TxnDate']
        encoded_due_date = bill_data['DueDate']
        encoded_ref_number = self.encode_input(bill_data['RefNumber'])
        encoded_memo = self.encode_input(bill_data['Memo'])
        encoded_amount_due = bill_data['AmountDue']

        txn_id = None
        if not self.bill_exists(bill_data['RefNumber']):
            self.cursor.execute(query, (encoded_vendor, encoded_txn_date, encoded_due_date, encoded_ref_number, encoded_memo, encoded_amount_due))

            # Retrieve the generated TxnID
            self.cursor.execute("SELECT @@IDENTITY AS TxnID")
            txn_id = self.cursor.fetchone().TxnID
        else:
            print(f"Bill with RefNumber '{bill_data['RefNumber']}' already exists, skipping insertion.")
        return txn_id
