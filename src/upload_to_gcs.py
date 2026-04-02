import os
import json
import logging
import subprocess
from datetime import datetime
from pymongo import MongoClient

# --- CẤU HÌNH ---
MONGO_URI = "mongodb://127.0.0.1:27017/"
DB_NAME = "countly"           # Thay bằng tên DB chuẩn của bạn
COLLECTION_NAME = "product_catalog"      # Thay bằng tên Collection chuẩn của bạn
BUCKET_NAME = "project5_lam"
SOURCE_FOLDER = "exports"
BATCH_SIZE = 5000                 # Bước 2: Kích thước mỗi batch

# --- [BƯỚC 5 & LOGGING FUNCTIONALITY]: LOG OPERATIONS ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_export_process.log"), # Lưu nhật ký vào file
        logging.StreamHandler()                         # Hiện ra màn hình terminal
    ]
)

def export_to_gcs(is_test=False):
    """
    Quy trình Data Export 5 bước chuẩn theo Pseudocode
    """
    client = None
    if not os.path.exists(SOURCE_FOLDER):
        os.makedirs(SOURCE_FOLDER)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "_SAMPLE" if is_test else ""
    file_name = f"{COLLECTION_NAME}_{timestamp}{suffix}.jsonl"
    local_path = os.path.join(SOURCE_FOLDER, file_name)

    # --- [IMPLEMENT ERROR HANDLING]: Bắt đầu khối xử lý lỗi ---
    try:
        logging.info("--- BẮT ĐẦU QUY TRÌNH DATA EXPORT ---")
        
        # 1. CONNECT TO MONGODB
        logging.info(f"[BƯỚC 1] Đang kết nối tới MongoDB...")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        col = db[COLLECTION_NAME]
        
        # [TEST WITH SAMPLE DATA]: Giới hạn 5 dòng nếu đang test
        limit_val = 5 if is_test else 0
        total_docs = col.count_documents({}) if not is_test else 5
        logging.info(f"Tổng số bản ghi sẽ xử lý: {total_docs}")

        # 2. EXTRACT DATA IN BATCHES
        # 3. CONVERT TO APPROPRIATE FORMAT (JSONL)
        logging.info(f"[BƯỚC 2&3] Đang trích xuất theo Batch và chuyển sang JSONL...")
        
        cursor = col.find({}).limit(limit_val)
        
        with open(local_path, 'w', encoding='utf-8') as f:
            count = 0
            for doc in cursor:
                # Xử lý ObjectId
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
                
                # Ghi dữ liệu vào file
                f.write(json.dumps(doc, ensure_ascii=False) + '\n')
                
                count += 1
                if count % BATCH_SIZE == 0:
                    logging.info(f"  > Đã trích xuất: {count}/{total_docs}")

        # 4. UPLOAD TO GCS (Sử dụng gsutil để đảm bảo SSL thông suốt)
        logging.info(f"[BƯỚC 4] Đang upload file lên GCS: gs://{BUCKET_NAME}/")
        gcs_dest = f"gs://{BUCKET_NAME}/{SOURCE_FOLDER}/{file_name}"
        
        # Lệnh gsutil thần thánh
        upload_command = f"gsutil cp {local_path} {gcs_dest}"
        subprocess.run(upload_command, shell=True, check=True, capture_output=True, text=True)
        
        logging.info(f"[SUCCESS] File đã nằm trên GCS: {gcs_dest}")

        # Tự động dọn dẹp file local sau khi upload
        if os.path.exists(local_path):
            os.remove(local_path)
            logging.info("Đã xóa file tạm ở máy ảo.")

    # --- [IMPLEMENT ERROR HANDLING]: Xử lý khi có lỗi ---
    except Exception as e:
        logging.error(f"!!! QUY TRÌNH THẤT BẠI: {str(e)}")
    
    finally:
        # 5. LOG OPERATIONS: Đóng kết nối và ghi log kết thúc
        if client:
            client.close()
        logging.info("--- [BƯỚC 5] ĐÃ ĐÓNG KẾT NỐI & KẾT THÚC ---")

if __name__ == "__main__":
    # --- [YÊU CẦU TEST WITH SAMPLE DATA] ---
    # Chỉnh thành True để test 5 dòng, False để chạy toàn bộ
    mode_test = False
    
    if mode_test:
        print(">>> CHẾ ĐỘ TEST: Chỉ lấy 5 bản ghi...")
    else:
        print(">>> CHẾ ĐỘ CHẠY THẬT: Đang lấy toàn bộ dữ liệu...")
        
    export_to_gcs(is_test=mode_test)
