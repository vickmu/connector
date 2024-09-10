Importing customers from VickHub: 
- Check if customer exists: 
    - If does, return ListID 
    - Otherwise, create customer 

- Create SalesReceiptItem 
- Map fields from VickHub to QB 

Importing Existing Sales Receipts: 
- Filter by sales between 2023 and 2024 from the source file 
- Put them in Excel 
- Update the customer ref ID with the targetCustomer ID. We will use the name to match 
- SELECT RefNumber from SalesReceipt (target)
- Iterate through the source sales receipt items in excel and if the ref number is in the target, then skip
