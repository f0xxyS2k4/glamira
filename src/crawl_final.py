import pymongo
from curl_cffi import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import time
import random
import re
import json

# --- CẤU HÌNH ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "countly"
COLLECTION = "product_catalog"
MAX_WORKERS = 10 

client = pymongo.MongoClient(MONGO_URI)
col = client[DB_NAME][COLLECTION]
session = requests.Session(impersonate="chrome110")

def extract_react_data(html_content):
    try:
        match = re.search(r'var\s+react_data\s*=\s*(\{.*?\});', html_content, re.DOTALL)
        if match:
            return json.loads(match.group(1))
    except Exception:
        return None
    return None

def crawl_one_product(pid, url, is_retry=False):
    try:
        time.sleep(random.uniform(0.5, 1.2))
        resp = session.get(url, timeout=20)
        
        # Tự sửa lỗi: Nếu link gốc tèo, thử link hệ thống /view/id/
        if resp.status_code != 200 and not is_retry:
            backup_url = f"https://www.glamira.com/catalog/product/view/id/{pid}"
            return crawl_one_product(pid, backup_url, is_retry=True)

        if resp.status_code == 200:
            data = extract_react_data(resp.text)
            if data:
                # DANH SÁCH CÁC TRƯỜNG ANH YÊU CẦU (CHỈ LẤY ĐÚNG ĐỐNG NÀY)
                fields_to_get = [
                    "product_id", "name", "sku", "attribute_set_id", "attribute_set",
                    "type_id", "price", "min_price", "max_price", "min_price_format",
                    "max_price_format", "gold_weight", "none_metal_weight", "fixed_silver_weight",
                    "material_design", "qty", "collection", "collection_id", "product_type",
                    "product_type_value", "category", "category_name", "store_code",
                    "platinum_palladium_info_in_alloy", "bracelet_without_chain",
                    "show_popup_quantity_eternity", "visible_contents", "gender"
                ]

                # Lọc dữ liệu: Chỉ lấy những key có trong list fields_to_get
                update_payload = { field: data.get(field) for field in fields_to_get }
                
                # Thêm trạng thái để quản lý tiến độ
                update_payload["crawl_status"] = "success"
                update_payload["last_updated"] = time.ctime()

                # Cập nhật vào DB
                col.update_many({"product_id": pid}, {"$set": update_payload})
                print(f"✅ [ID {pid}] Done: SKU {data.get('sku')} | Price: {data.get('price')}")
                return True
        
        col.update_many({"product_id": pid}, {"$set": {"crawl_status": f"failed_{resp.status_code}"}})
    except Exception as e:
        print(f"❌ [ID {pid}] Error: {str(e)}")
    return False

def process_item(item):
    crawl_one_product(item['product_id'], item['url'])

def main():
    # Tìm những thằng CHƯA có trường 'sku' để bổ sung đủ bộ thông tin
    pipeline = [
        {"$match": {"sku": {"$exists": False}}}, 
        {"$group": {"_id": "$product_id", "url": {"$first": "$url"}}},
        {"$limit": 500} 
    ]
    
    items = list(col.aggregate(pipeline))
    if not items: return False

    print(f"\n🚀 Đang quất {len(items)} ID. Mục tiêu: Lấy đúng 28 trường dữ liệu...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = [{"product_id": i["_id"], "url": i["url"]} for i in items]
        executor.map(process_item, tasks)
    return True

if __name__ == "__main__":
    while main():
        time.sleep(1)
