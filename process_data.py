import pymongo
import IP2Location
import csv
from datetime import datetime

# --- CẤU HÌNH ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "countly"
SOURCE_COL = "summary"
DEST_COL = "ip_location_results" # Collection mới
OUTPUT_CSV = "glamira_full_result.csv" # File CSV mới
IP_BIN_PATH = "IP-COUNTRY-REGION-CITY.BIN"

def main():
    print(f"[{datetime.now()}] Đang khởi tạo kết nối...")
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    # Làm sạch Collection cũ và chuẩn bị Collection mới
    db[DEST_COL].drop()
    dest = db[DEST_COL]
    source = db[SOURCE_COL]
    
    ip_tool = IP2Location.IP2Location(IP_BIN_PATH)

    # 1. Lấy danh sách IP duy nhất (Dùng Aggregate để tránh lỗi 16MB)
    pipeline = [
        {"$group": {"_id": "$ip"}},
        {"$project": {"ip": "$_id", "_id": 0}}
    ]
    
    print(f"[{datetime.now()}] Đang quét IP duy nhất từ 20 triệu dòng...")
    cursor = source.aggregate(pipeline, allowDiskUse=True)

    # 2. Mở file CSV và chuẩn bị ghi song song
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["IP", "Country_Code", "Country_Name", "Region", "City"])

        batch_db = []
        count = 0
        
        for doc in cursor:
            ip = doc.get('ip')
            if not ip or ip in ["::1", "127.0.0.1"]:
                continue
                
            try:
                res = ip_tool.get_all(ip)
                # Dữ liệu chuẩn hóa
                data = {
                    "ip": ip,
                    "country_code": res.country_short,
                    "country_name": res.country_long,
                    "region": res.region,
                    "city": res.city,
                    "processed_at": datetime.now()
                }
                
                # Ghi vào FILE CSV
                writer.writerow([ip, res.country_short, res.country_long, res.region, res.city])
                
                # Ghi vào BATCH để chuẩn bị lưu DATABASE
                batch_db.append(data)
                
            except:
                continue

            # Mỗi 10,000 dòng thì đẩy vào DB một lần
            if len(batch_db) >= 10000:
                dest.insert_many(batch_db)
                count += len(batch_db)
                print(f"[{datetime.now()}] Đã xong {count} IP (vừa lưu DB, vừa ghi CSV)...")
                batch_db = []

        # Lưu nốt số còn lại vào DB
        if batch_db:
            dest.insert_many(batch_db)
            count += len(batch_db)

    print(f"--- TẤT CẢ ĐÃ HOÀN THÀNH ---")
    print(f"1. Database Collection: {DEST_COL}")
    print(f"2. File CSV: {OUTPUT_CSV}")
    print(f"Tổng số bản ghi: {count}")

if __name__ == "__main__":
    main()
