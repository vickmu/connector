from .qb import QBOperations
import logging

logger = logging.getLogger(__name__)

class ItemOperations(QBOperations):
    def item_exists(self, item_name):
        query = u"SELECT COUNT(*) FROM Item WHERE Name = ?"
        encoded_name = self.encode_input(item_name)
        self.cursor.execute(query, (encoded_name,))
        result = self.cursor.fetchone()
        return result[0] > 0

    def list_items_by_name(self):
        query = u"SELECT Name, ListID FROM Item"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def insert_item(self, item_data):
        query = u"INSERT INTO Item (Name, Type, IsActive) VALUES (?, ?, ?)"
        encoded_name = self.encode_input(item_data['Name'])
        encoded_type = self.encode_input(item_data['Type'])
        encoded_active = self.encode_input(item_data['IsActive'])

        logger.info(f"Inserting item: {item_data['Name']}")
        logger.info(f"Query: {query}")
        self.cursor.execute(query, (encoded_name, encoded_type, encoded_active))