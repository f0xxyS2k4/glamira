import IP2Location  
import pymongo
import os

# --- CẤU HÌNH ---
BIN_FILE_PATH = "IP-COUNTRY-REGION-CITY.BIN"
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "countly"                 
SOURCE_COLLECTION = "summary"       
TARGET_COLLECTION = "ip_locations"  
BATCH_SIZE = 1000 

def process_ip_locations():
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        raw_col = db[SOURCE_COLLECTION]
        loc_col = db[TARGET_COLLECTION]

        loc_col.create_index("ip", unique=True)

        if not os.path.exists(BIN_FILE_PATH):
            print(f"❌ Lỗi: Không tìm thấy file {BIN_FILE_PATH}")
            return
            
        ip_tool = IP2Location.IP2Location(BIN_FILE_PATH) 

        print(f"🔍 Đang quét IP bằng Aggregation (để tránh lỗi 16MB)...")
        
        # Thay thế distinct() bằng aggregation pipeline
        pipeline = [
            {"$group": {"_id": "$ip"}},
            {"$project": {"ip": "$_id", "_id": 0}}
        ]
        
        # cursor=True giúp xử lý dữ liệu theo luồng, không bị tràn ram
        all_ips_cursor = raw_col.aggregate(pipeline, allowDiskUse=True)
        all_unique_ips = {doc['ip'] for doc in all_ips_cursor if doc.get('ip')}
        
        # Check checkpoint
        processed_ips = set(loc_col.distinct("ip"))
        ips_to_process = list(all_unique_ips - processed_ips)
        
        total_need = len(ips_to_process)
        if total_need == 0:
            print("✅ Xong rồi! Không có IP mới.")
            return

        print(f"🚀 Tổng cộng: {len(all_unique_ips)} IP. Cần xử lý mới: {total_need}")

        results = []
        for i, ip in enumerate(ips_to_process):
            try:
                rec = ip_tool.get_all(ip)
                results.append({
                    "ip": ip,
                    "country_code": rec.country_short,
                    "country_name": rec.country_long,
                    "region": rec.region,
                    "city": rec.city
                })
                
                if len(results) >= BATCH_SIZE or i == total_need - 1:
                    loc_col.insert_many(results, ordered=False)
                    print(f"💾 Tiến độ: {i+1}/{total_need}")
                    results = []
            except Exception as e:
                if "duplicate key" not in str(e):
                    print(f"⚠️ Lỗi tại IP {ip}: {e}")

        print("🎉 HOÀN THÀNH!")

    except Exception as e:
        print(f"❌ Lỗi hệ thống: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    process_ip_locations()
