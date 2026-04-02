from pymongo import MongoClient

# Kết nối local
client = MongoClient("mongodb://127.0.0.1:27017/")

print("--- DANH SÁCH DATABASE ĐANG CÓ ---")
dbs = client.list_database_names()
for db_name in dbs:
    print(f"Database: {db_name}")
    db = client[db_name]
    cols = db.list_collection_names()
    for col_name in cols:
        count = db[col_name].count_documents({})
        print(f"  └─ Collection: {col_name} (Số bản ghi: {count})")
