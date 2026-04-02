import pymongo
import requests
from bs4 import BeautifulSoup
import time
import csv
import os
import random

# --- CẤU HÌNH ---
DB_NAME = "countly"
COL_NAME = "product_mapping_raw"
OUTPUT_FILE = "glamira_product_names_pro.csv"
MAX_RETRIES = 2  
TIMEOUT = 10     

# Danh sách User-Agents để giả dạng các trình duyệt khác nhau
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1"
]

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client[DB_NAME]
products_col = db[COL_NAME]

def get_product_name(url):
    for i in range(MAX_RETRIES):
        try:
            # Mỗi lần thử lại sẽ chọn một danh tính mới
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            response = requests.get(url, headers=headers, timeout=TIMEOUT)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                # Thử lấy H1, nếu không có thử lấy Title của trang
                name_tag = soup.find('h1')
                if name_tag:
                    return name_tag.get_text().strip()
                return soup.title.string.split('|')[0].strip() if soup.title else "No Name Found"
            
            elif response.status_code == 403:
                # Nếu bị chặn, nghỉ lâu hơn (5s) rồi thử lại lần cuối
                time.sleep(5)
                continue
        except:
            time.sleep(2)
            continue
    return "Error 403: Blocked"

# 1. Checkpoint
done_ids = set()
if os.path.isfile(OUTPUT_FILE):
    with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if row: done_ids.add(row[0])

# 2. Lấy data chưa làm
all_products = list(products_col.find())
todo_products = [p for p in all_products if str(p['_id']) not in done_ids]

print(f"Tổng: {len(all_products)} | Đã xong: {len(done_ids)} | Cần làm: {len(todo_products)}")

# 3. Chạy
with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    if not os.path.isfile(OUTPUT_FILE) or os.stat(OUTPUT_FILE).st_size == 0:
        writer.writerow(["product_id", "product_name", "url"])

    for index, p in enumerate(todo_products):
        pid = str(p['_id'])
        url = p.get('url', '')
        
        if url and url.startswith('http'):
            name = get_product_name(url)
        else:
            name = "Invalid URL"
            
        writer.writerow([pid, name, url])
        f.flush() # Ghi ngay lập tức
        
        # In tiến độ kèm theo tên để anh dễ theo dõi
        print(f"[{index+1}/{len(todo_products)}] {name[:50]}...")
        
        # Thời gian nghỉ ngẫu nhiên từ 0.7s đến 1.5s để giống người thật
        time.sleep(random.uniform(0.7, 1.5))

print("--- HOÀN THÀNH ---")
