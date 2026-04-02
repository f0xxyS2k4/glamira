import pymongo

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "countly"
SOURCE_COLLECTION = "summary"
TARGET_COLLECTION = "product_catalog"

def rebuild_catalog():
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    raw_col = db[SOURCE_COLLECTION]
    
    # 1. Khởi tạo bảng mới và tạo Index
    target_col = db[TARGET_COLLECTION]
    target_col.create_index("product_id", unique=True)

    print("🚀 Đang quét dữ liệu từ bảng summary để xây dựng Catalog...")

    # Nhóm 1: Các hành động lấy current_url
    # Ưu tiên product_id, nếu thiếu lấy viewing_product_id
    group1_actions = [
        "view_product_detail", 
        "select_product_option", 
        "select_product_option_quality", 
        "add_to_cart_action",
        "product_detail_recommendation_visible",
        "product_detail_recommendation_noticed"
    ]

    pipeline1 = [
        {"$match": {"collection": {"$in": group1_actions}}},
        {"$project": {
            "pid": {"$ifNull": ["$product_id", "$viewing_product_id"]},
            "url": "$current_url"
        }},
        {"$match": {"pid": {"$ne": None}, "url": {"$ne": None}}},
        {"$group": {"_id": "$pid", "url": {"$first": "$url"}}}
    ]

    # Nhóm 2: Hành động đặc biệt lấy referrer_url
    pipeline2 = [
        {"$match": {"collection": "product_view_all_recommend_clicked"}},
        {"$project": {
            "pid": "$viewing_product_id",
            "url": "$referrer_url"
        }},
        {"$match": {"pid": {"$ne": None}, "url": {"$ne": None}}},
        {"$group": {"_id": "$pid", "url": {"$first": "$url"}}}
    ]

    total_inserted = 0
    for i, pipe in enumerate([pipeline1, pipeline2], 1):
        print(f"📦 Đang xử lý Nhóm {i}...")
        results = raw_col.aggregate(pipe, allowDiskUse=True)
        for res in results:
            try:
                target_col.insert_one({
                    "product_id": str(res["_id"]),
                    "url": res["url"],
                    "source_group": i
                })
                total_inserted += 1
            except pymongo.errors.DuplicateKeyError:
                continue 

    print(f"✅ Xong! Tổng số sản phẩm duy nhất thu được: {total_inserted}")
    client.close()

if __name__ == "__main__":
    rebuild_catalog()
