import pymongo
from curl_cffi import requests # Vũ khí vượt 403
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import time
import random

# --- CẤU HÌNH ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "countly"
COLLECTION = "product_catalog"
MAX_WORKERS = 5  # Giữ 5 luồng là đẹp nhất với curl_cffi, nhanh và mượt

# 1. Khởi tạo DB 1 lần
client = pymongo.MongoClient(MONGO_URI, maxPoolSize=50)
col = client[DB_NAME][COLLECTION]

# 2. Dùng Session của curl_cffi giả lập y hệt Chrome 110 để lừa server
session = requests.Session(impersonate="chrome110")

def get_product_name(url):
    try:
        # Nghỉ nhịp nhàng như người thật thao tác
        time.sleep(random.uniform(1.0, 2.5))
        
        # Gửi request xịn
        response = session.get(url, timeout=15)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Ưu tiên lấy H1
            name_tag = soup.find('h1')
            if name_tag:
                return name_tag.get_text().strip()
            
            # Không có H1 thì lấy Title
            elif soup.title and soup.title.string:
                return soup.title.string.split('|')[0].strip()
        
        elif response.status_code in [403, 429, 503]:
            return "RETRY_NEEDED"
            
        return None
    except Exception as e:
        return None

def process_item(item):
    pid = item['product_id']
    url = item['url']
    
    name = get_product_name(url)
    
    if name == "RETRY_NEEDED":
        # Chỉ đánh lỗi cho URL này. Vòng sau (nếu có URL khác cùng ID) nó sẽ bốc URL khác đi thử
        col.update_one({"url": url}, {"$set": {"crawl_status": "failed_403"}})
        print(f"⚠️ [ID {pid}] Bị chặn 403. Tạm note lại.")
    elif name:
        # THÀNH CÔNG: Update TẤT CẢ các document có chung product_id
        col.update_many(
            {"product_id": pid}, 
            {"$set": {"product_name": name, "crawl_status": "success"}}
        )
        print(f"✅ [ID {pid}] -> {name} (Đã update toàn bộ link trùng ID)")
    else:
        # URL chết hẳn
        col.update_one({"url": url}, {"$set": {"crawl_status": "dead_link"}})
        print(f"❌ [ID {pid}] URL chết hoặc trống")

def main():
    # 3. Gom nhóm (Aggregate) - ĐÚNG YÊU CẦU LẤY 1 TÊN / 1 ID
    pipeline = [
        {"$match": {
            "crawl_status": {"$ne": "success"},
            "url": {"$regex": "^http"}
        }},
        {"$group": {
            "_id": "$product_id", 
            "url": {"$first": "$url"} # Bốc bừa 1 URL đại diện của ID đó mang đi dò
        }},
        {"$limit": 500} # Chạy 500 ID một mẻ
    ]
    
    items_cursor = col.aggregate(pipeline)
    items = [{"product_id": doc["_id"], "url": doc["url"]} for doc in items_cursor]
    
    if not items:
        return False

    print(f"\n🚀 Đang quất mẻ {len(items)} ID DUY NHẤT với {MAX_WORKERS} luồng...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(process_item, items)
    return True

if __name__ == "__main__":
    try:
        while True:
            has_more = main()
            if not has_more:
                print("\n🎉 ĐÃ CRAWL XONG TOÀN BỘ CATALOG! CHÚC MỪNG ANH!")
                break
            time.sleep(2)
    except KeyboardInterrupt:
        print("\n🛑 Anh vừa bấm dừng cưỡng bức!")
    finally:
        client.close()
        print("💾 Đã đóng kết nối Database an toàn.")
