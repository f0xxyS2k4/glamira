import pymongo
import re

def extract_name_from_url(url):
    if not url: return "Unknown Product"
    try:
        # Tìm phần tên giữa dấu / cuối cùng và .html
        # Ví dụ: https://www.glamira.fr/glamira-pendant-viktor.html -> glamira-pendant-viktor
        match = re.search(r'/([^/?#]+)\.html', url)
        if match:
            slug = match.group(1)
            # Thay dấu gạch ngang bằng dấu cách và viết hoa từng chữ cái
            clean_name = slug.replace('-', ' ').title()
            return clean_name
    except:
        pass
    return "Unknown Product"

def main():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["countly"]
    col = db["product_catalog"]

    # Lấy tất cả sản phẩm chưa có tên hoặc crawl thất bại
    products = list(col.find({
        "$or": [
            {"product_name": {"$exists": False}},
            {"crawl_status": "failed"},
            {"product_name": "Unknown Product"}
        ]
    }))

    print(f"🛠️ Đang trích xuất tên cho {len(products)} sản phẩm bằng thuật toán URL...")

    updates = 0
    for p in products:
        name = extract_name_from_url(p.get('url', ''))
        col.update_one(
            {"_id": p['_id']},
            {"$set": {"product_name": name, "crawl_status": "extracted"}}
        )
        updates += 1
        if updates % 1000 == 0:
            print(f"✅ Đã xử lý {updates} sản phẩm...")

    print(f"🎉 Xong! Đã cập nhật {updates} tên sản phẩm thành công.")
    client.close()

if __name__ == "__main__":
    main()
