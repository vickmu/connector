from enum import Enum

class MigrationType(Enum):
    BILLS = "Bills"
    SALES_RECEIPTS = "Sales Receipts"
    CUSTOMERS = "Customers"
    FULL = "Full Migration"
    VENDORS = "Vendors"
    ITEMS = "Items"