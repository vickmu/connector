from .qb_operations import QBOperations
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

    def list_customers_by_name(self):
        """List all customers by Name."""
        query = "SELECT Name FROM Customer"
        self.cursor.execute(query)
        return self.cursor.fetchall()
    