from .qb import QBOperations
import logging

logger = logging.getLogger(__name__)

class CustomerOperations(QBOperations):
    def customer_exists(self, name):
        """Check if a customer already exists based on Name."""
        query = "SELECT COUNT(*) FROM Customer WHERE Name = ?"
        self.cursor.execute(query, (name,))
        count = self.cursor.fetchone()[0]
        return count > 0
    
    def get_customer_by_name(self, name):
        """Retrieve a customer by Name."""
        query = "SELECT * FROM Customer WHERE Name = ?"
        self.cursor.execute(query, (name, ))
        return self.cursor.fetchone()

    def list_customers_by_fullname(self):
        """List all customers by Name."""
        query = "SELECT Name FROM Customer"
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def list_customers_by_id(self):
        """List all customers by ID."""
        query = "SELECT ListID FROM Customer"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def list_customer_types(self): 
        query = "SELECT ListID, Name FROM CustomerType"
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def insert_customer(self, customer_data): 
        """Inserts a customer"""
        
        # Extract and encode the customer data
        name = self.encode_input(customer_data.get('Name', ''))
        first_name = self.encode_input(customer_data.get('FirstName', ''))
        middle_name = self.encode_input(customer_data.get('MiddleName', ''))
        last_name = self.encode_input(customer_data.get('LastName', ''))
        
        bill_address_addr1 = self.encode_input(customer_data.get('BillAddressAddr1', ''))
        bill_address_addr2 = self.encode_input(customer_data.get('BillAddressAddr2', ''))
        bill_address_addr3 = self.encode_input(customer_data.get('BillAddressAddr3', ''))
        bill_address_addr4 = self.encode_input(customer_data.get('BillAddressAddr4', ''))
        bill_address_addr5 = self.encode_input(customer_data.get('BillAddressAddr5', ''))
        bill_address_city = self.encode_input(customer_data.get('BillAddressCity', ''))
        bill_address_state = self.encode_input(customer_data.get('BillAddressState', ''))
        bill_address_postal_code = self.encode_input(customer_data.get('BillAddressPostalCode', ''))
        bill_address_country = self.encode_input(customer_data.get('BillAddressCountry', ''))
        
        ship_address_addr1 = self.encode_input(customer_data.get('ShipAddressAddr1', ''))
        ship_address_addr2 = self.encode_input(customer_data.get('ShipAddressAddr2', ''))
        ship_address_addr3 = self.encode_input(customer_data.get('ShipAddressAddr3', ''))
        ship_address_addr4 = self.encode_input(customer_data.get('ShipAddressAddr4', ''))
        ship_address_addr5 = self.encode_input(customer_data.get('ShipAddressAddr5', ''))
        ship_address_city = self.encode_input(customer_data.get('ShipAddressCity', ''))
        ship_address_state = self.encode_input(customer_data.get('ShipAddressState', ''))
        ship_address_postal_code = self.encode_input(customer_data.get('ShipAddressPostalCode', ''))
        ship_address_country = self.encode_input(customer_data.get('ShipAddressCountry', ''))
        
        email = self.encode_input(customer_data.get('Email'))
        # Construct the SQL query
        query = f"""
        INSERT INTO Customer (
            Name,
            FirstName,
            MiddleName,
            LastName,
            BillAddressAddr1,
            BillAddressAddr2,
            BillAddressAddr3,
            BillAddressAddr4,
            BillAddressAddr5,
            BillAddressCity,
            BillAddressState,
            BillAddressPostalCode,
            BillAddressCountry,
            ShipAddressAddr1,
            ShipAddressAddr2,
            ShipAddressAddr3,
            ShipAddressAddr4,
            ShipAddressAddr5,
            ShipAddressCity,
            ShipAddressState,
            ShipAddressPostalCode,
            ShipAddressCountry,
            Email
        ) VALUES (
            '{name}', 
            '{first_name}', 
            '{middle_name}', 
            '{last_name}', 
            '{bill_address_addr1}', 
            '{bill_address_addr2}', 
            '{bill_address_addr3}', 
            '{bill_address_addr4}', 
            '{bill_address_addr5}', 
            '{bill_address_city}', 
            '{bill_address_state}', 
            '{bill_address_postal_code}', 
            '{bill_address_country}', 
            '{ship_address_addr1}', 
            '{ship_address_addr2}', 
            '{ship_address_addr3}', 
            '{ship_address_addr4}', 
            '{ship_address_addr5}', 
            '{ship_address_city}', 
            '{ship_address_state}', 
            '{ship_address_postal_code}', 
            '{ship_address_country}',
            '{email}'
        )
        """
        try:
            # Execute the query
            self.cursor.execute(query)
            self.conn.commit()
            logger.error(f"Customer {name} inserted successfully.")
        except Exception as e:
            logger.error(f"Error inserting customer: {str(e)}")
            logger.info(f"Executing query: {query}")  # Log the query
            # raise
            # self.conn.rollback()
