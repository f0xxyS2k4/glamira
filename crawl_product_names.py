import pymongo
import requests
from bs4 import BeautifulSoup
import time
import csv

# Kết nối MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["countly"]
# Đảm bảo anh đã chạy script extract_product_info.py trước đó để có bảng này
products_col = db["product_mapping_raw"]

output_file = "glamira_product_names.csv"

def get_product_name(url):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            name_tag = soup.find('h1') 
            if name_tag:
                return name_tag.get_text().strip()
    except Exception as e:
        return f"Error: {e}"
    return "Unknown"

# Lấy danh sách sản phẩm
products = list(products_col.find())
total = len(products)
print(f"Bat dau crawl {total} san pham...")

with open(output_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(["product_id", "product_name", "url"])

    for index, p in enumerate(products):
        pid = p['_id']
        url = p.get('url', '')
        
        if url and url.startswith('http'):
            name = get_product_name(url)
        else:
            name = "Invalid URL"
        
        writer.writerow([pid, name, url])
        
        if (index + 1) % 10 == 0:
            print(f"Da xong: {index + 1}/{total}...")
        
        # Nghi 0.5 giay de tranh bi Glamira chan IP
        time.sleep(0.5)

print("--- HOAN THANH ---")
