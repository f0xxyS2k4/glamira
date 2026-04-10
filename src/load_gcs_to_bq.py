from google.cloud import bigquery

PROJECT_ID = "project-336a1718-7c59-40b1-acf"
DATASET_ID = "glamira_raw"
client = bigquery.Client(project=PROJECT_ID)

def load_table(table_name, gcs_path, schema):
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ignore_unknown_values=True,
        schema=schema,
        autodetect=False 
    )
    print(f"⏳ Đang nạp bảng: {table_name}...")
    uri = f"gs://project5_lam/{gcs_path}"
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    try:
        load_job.result()
        print(f"✅ Thành công bảng {table_name}!")
    except Exception as e:
        print(f"❌ Lỗi bảng {table_name}: {str(e)}")

if __name__ == "__main__":
    # 1. SUMMARY
    all_summary_columns = ["api_version", "cart_products", "cat_id", "collect_id", "collection", "currency", "current_url", "device_id", "email_address", "ip", "is_paypal", "key_search", "local_time", "option", "order_id", "price", "product_id", "recommendation", "recommendation_clicked_position", "recommendation_product_id", "recommendation_product_position", "referrer_url", "resolution", "show_recommendation", "store_id", "time_stamp", "user_agent", "user_id_db", "utm_medium", "utm_source", "viewing_product_id"]
    summary_fields = [bigquery.SchemaField(col, "INTEGER" if col=="time_stamp" else "JSON" if col in ["cart_products", "option"] else "STRING") for col in all_summary_columns]

    # 2. PRODUCT CATALOG (Sửa lỗi Array bằng cách dùng JSON)
    all_product_columns = ["attribute_set","attribute_set_id","bracelet_without_chain","category","category_name","collection","collection_id","crawl_status","fixed_silver_weight","full_detail","gender","gold_weight","last_updated","material","material_design","max_price","max_price_format","min_price","min_price_format","name","none_metal_weight","platinum_palladium_info_in_alloy","price","product_id","product_name","product_type","product_type_value","qty","show_popup_quantity_eternity","sku","source_group","store_code","type_id","url","visible_contents"]
    product_fields = []
    for col in all_product_columns:
        # Nếu gặp thêm lỗi "Array" ở cột nào, anh cứ thêm tên cột đó vào list bên dưới
        if col in ["visible_contents", "full_detail", "platinum_palladium_info_in_alloy", "attribute_set", "category"]:
            product_fields.append(bigquery.SchemaField(col, "JSON"))
        else:
            product_fields.append(bigquery.SchemaField(col, "STRING"))

    # 3. IP LOCATIONS
    ip_fields = [bigquery.SchemaField(c, "STRING") for c in ["ip", "country_code", "country_name", "region", "city", "zip_code", "latitude", "longitude"]]

    load_table("summary", "exports/Data_raw/Data_raw_20260402_035023.jsonl", summary_fields)
    load_table("ip_locations", "exports/IP2Location/IP2Location_20260402_050340.jsonl", ip_fields)
    load_table("product_catalog", "exports/Product/Product_20260402_050449.jsonl", product_fields)
