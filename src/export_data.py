import pymongo
import json
import os
import logging
from bson import json_util # Dùng để xử lý ObjectId của MongoDB
from datetime import datetime

# --- CẤU HÌNH ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "countly"
COLLECTION_NAME = "product_catalog"
EXPORT_DIR = "exports"
BATCH_SIZE = 5000 # Bốc mỗi lần 5k bản ghi để tối ưu RAM

# Thiết lập logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    filename=f'logs/export_{datetime.now().strftime("%Y%m%d")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def export_to_jsonl():
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        col = db[COLLECTION_NAME]

        # 1. Tạo thư mục chứa file export
        os.makedirs(EXPORT_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = f"{EXPORT_DIR}/{COLLECTION_NAME}_{timestamp}.jsonl"

        # 2. Đếm tổng số bản ghi
        total_docs = col.count_documents({})
        logging.info(f"🚀 Bắt đầu export {total_docs} bản ghi từ {COLLECTION_NAME}")
        print(f"Đang xuất {total_docs} bản ghi ra file: {file_path}")

        # 3. Extract theo Batches
        count = 0
        with open(file_path, 'w', encoding='utf-8') as f:
            cursor = col.find({}) # Lấy toàn bộ
            
            for doc in cursor:
                # Chuyển đổi BSON (ObjectId, Date) sang JSON chuẩn
                json_record = json.loads(json_util.dumps(doc))
                
                # Ghi từng dòng (JSON Lines format)
                f.write(json.dumps(json_record) + '\n')
                
                count += 1
                if count % BATCH_SIZE == 0:
                    logging.info(f"✅ Đã xử lý: {count}/{total_docs}")
                    print(f"Progress: {count}/{total_docs}")

        logging.info(f"✨ Hoàn thành! File lưu tại: {file_path}")
        print(f"--- THÀNH CÔNG: {count} bản ghi ---")
        return file_path

    except Exception as e:
        logging.error(f"❌ Lỗi khi export: {str(e)}")
        print(f"Lỗi rồi anh: {e}")
        return None

if __name__ == "__main__":
    export_to_jsonl()
