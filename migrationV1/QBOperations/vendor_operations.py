from qb_operations import QBOperations

class VendorOperations(QBOperations):
    def vendor_exists(self, vendor_name):
        query = u"SELECT COUNT(*) FROM Vendor WHERE Name = ?"
        encoded_name = self.encode_input(vendor_name)
        self.cursor.execute(query, (encoded_name,))
        result = self.cursor.fetchone()
        return result[0] > 0

    def list_vendors_by_name(self):
        query = u"SELECT Name FROM Vendor"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def insert_vendor(self, vendor_data):
        query = u"INSERT INTO Vendor (Name, CompanyName, IsActive) VALUES (?, ?, ?)"
        encoded_name = self.encode_input(vendor_data['Name'])
        encoded_company = self.encode_input(vendor_data['CompanyName'])
        encoded_active = self.encode_input(vendor_data['IsActive'])
        self.cursor.execute(query, [encoded_name, encoded_company, encoded_active])
