import pymongo
import requests
from bs4 import BeautifulSoup
import time
import csv
import random

# Kết nối DB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["countly"]
products_col = db["product_mapping_raw"]

OUTPUT_FILE = "glamira_crawled_final.csv"

def get_name_via_google_cache(url):
    # Tạo đường dẫn qua Google Cache
    cache_url = f"https://webcache.googleusercontent.com/search?q=cache:{url}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    }
    
    try:
        # Gọi qua Google Cache (Google hiếm khi chặn IP của anh)
        response = requests.get(cache_url, headers=headers, timeout=15)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            # Thử tìm H1 (Tên sản phẩm)
            name_tag = soup.find('h1')
            if name_tag:
                return name_tag.get_text().strip()
            return "Name Not Found in Cache"
        
        elif response.status_code == 404:
            return "Not Indexed by Google"
        else:
            return f"Error {response.status_code}"
            
    except Exception as e:
        return f"Request Error: {str(e)[:15]}"

# --- Chạy chính ---
all_products = list(products_col.find())
print(f"Bắt đầu crawl {len(all_products)} sản phẩm qua Google Cache...")

with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(["id", "name", "url"])
    
    for i, p in enumerate(all_products):
        name = get_name_via_google_cache(p['url'])
        writer.writerow([p['_id'], name, p['url']])
        
        print(f"[{i+1}/{len(all_products)}] -> {name}")
        
        f.flush()
        # Nghỉ lâu một chút (2-3 giây) để Google không tưởng mình là bot phá hoại
        time.sleep(random.uniform(2, 4))

print("--- HOÀN THÀNH ---")
