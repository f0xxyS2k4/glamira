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

# Danh sách Proxy miễn phí (Anh có thể tìm thêm các list proxy sống trên mạng)
# Đây là ví dụ, thực tế anh nên dùng một hàm để lấy proxy tự động
PROXY_LIST = [
    "http://64.225.4.81:9999",
    "http://165.225.114.5:80",
    "http://192.111.135.18:4145",
    # Thêm thật nhiều IP vào đây...
]

def get_product_name_pro(url):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    
    # Thử tối đa 5 proxy khác nhau cho mỗi URL
    for _ in range(5):
        proxy = random.choice(PROXY_LIST)
        proxies = {"http": proxy, "https": proxy}
        try:
            # Dùng proxy để "vượt rào"
            response = requests.get(url, headers=headers, proxies=proxies, timeout=15)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                name_tag = soup.find('h1')
                return name_tag.get_text().strip() if name_tag else "No Name"
            elif response.status_code == 403:
                print(f"Proxy {proxy} bị chặn, đang đổi proxy khác...")
                continue
        except:
            continue
    return "Failed All Proxies"

# --- Phần chạy chính ---
all_products = list(products_col.find())
with open("glamira_crawled_real.csv", 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(["id", "name", "url"])
    
    for i, p in enumerate(all_products):
        name = get_product_name_pro(p['url'])
        writer.writerow([p['_id'], name, p['url']])
        print(f"[{i+1}/{len(all_products)}] -> {name}")
        time.sleep(1)
