# producer.py (Đã sửa lỗi "not iterable")

import requests
import json
import time
import os
import argparse
from sseclient import SSEClient
from datetime import datetime

# URL của luồng Wikipedia
WIKI_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

# URL Ingest Endpoint của Tinybird (lấy từ tài khoản của bạn)
TINYBIRD_URL = "https://api.tinybird.co/v0/events?name=wikipidia_test" 

# Lấy Token từ GitHub Secrets
TINYBIRD_TOKEN = os.environ.get("TINYBIRD_TOKEN")

# Thiết lập thời gian chạy (ví dụ: 840 giây = 14 phút)
parser = argparse.ArgumentParser()
parser.add_argument('--timeout', type=int, default=840, help='Thời gian chạy (giây)')
args = parser.parse_args()

print(f"--- Kịch bản sẽ chạy trong {args.timeout} giây ---") # Print để kiểm tra
start_time = time.time()
events_batch = [] # Nơi lưu dữ liệu tạm thời

# Kiểm tra xem Token đã được thiết lập chưa
if not TINYBIRD_TOKEN:
    print("Lỗi: TINYBIRD_TOKEN chưa được thiết lập trong GitHub Secrets.")
    exit(1) # Thoát với mã lỗi

try:
    # --- PHẦN SỬA LỖI ---
    # Thay vì: client = SSEClient(WIKI_STREAM_URL)
    # Chúng ta dùng 'requests' để kết nối trước, sau đó đưa cho SSEClient
    
    print("Đang kết nối đến luồng Wikipedia SSE...")
    response = requests.get(WIKI_STREAM_URL, stream=True)
    response.raise_for_status() # Sẽ báo lỗi ngay nếu kết nối hỏng (404, 500, v.v.)
    client = SSEClient(response)
    print("Kết nối thành công. Bắt đầu lắng nghe sự kiện...")
    # --- KẾT THÚC PHẦN SỬA ---

    for event in client:
        # 1. Dừng khi hết thời gian
        if time.time() - start_time > args.timeout:
            print(f"Hết {args.timeout} giây. Dừng thu thập.")
            break
            
        # 2. Chỉ xử lý các tin nhắn 'message'
        if event.event == 'message':
            try:
                data = json.loads(event.data)
                
                # 3. Chỉ lấy các sự kiện 'edit'
                if data.get('type') == 'edit':
                    
                    # 4. Tính toán kích thước sửa đổi
                    old_len = data.get('length', {}).get('old', 0) or 0
                    new_len = data.get('length', {}).get('new', 0) or 0
                    edit_size_bytes = new_len - old_len
                    
                    # 5. Chuyển đổi timestamp (Unix) sang (ISO 8601)
                    ts_int = data.get("timestamp")
                    ts_iso = datetime.utcfromtimestamp(ts_int).isoformat()
                    
                    # 6. Xây dựng đối tượng JSON sạch khớp với Schema
                    clean_event = {
                        "timestamp": ts_iso,
                        "title": data.get("title"),
                        "user": data.get("user"),
                        "is_bot": data.get("bot", False),
                        "server_name": data.get("server_name"),
                        "edit_size_bytes": edit_size_bytes,
                        "country_code": None # Tạm thời để Null
                    }
                    events_batch.append(clean_event)
                    
            except json.JSONDecodeError:
                pass # Bỏ qua các tin nhắn không phải JSON

except Exception as e:
    # Lỗi bây giờ sẽ rõ ràng hơn, ví dụ như "404 Client Error"
    print(f"Lỗi trong khi xử lý luồng: {e}")

# 7. Gửi dữ liệu (batch) đến Tinybird
if events_batch:
    print(f"Gửi {len(events_batch)} sự kiện đến Tinybird...")
    
    # Chuyển đổi list of dicts thành định dạng NDJSON (mỗi dòng 1 JSON)
    payload = "\n".join(json.dumps(event) for event in events_batch)
    
    response = requests.post(
        TINYBIRD_URL,
        headers={"Authorization": f"Bearer {TINYBIRD_TOKEN}"},
        data=payload
    )
    
    # Kiểm tra kết quả
    if response.status_code == 200 or response.status_code == 202:
        print(f"Tinybird phản hồi: {response.status_code} (Gửi thành công!)")
    else:
        print(f"Lỗi: Tinybird phản hồi: {response.status_code}")
        print(response.text) # In lỗi nếu có
else:
    print("Không có sự kiện nào được thu thập để gửi.")
