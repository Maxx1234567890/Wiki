# producer.py

import requests
import json
import time
import os
import argparse
from sseclient import SSEClient
from datetime import datetime

# URL của luồng Wikipedia
WIKI_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

# URL Ingest Endpoint của Tinybird
TINYBIRD_URL = "https://api.tinybird.co/v0/events?name=wiki_events"

# Lấy Token từ env
TINYBIRD_TOKEN = os.environ.get("TINYBIRD_TOKEN")

# Thiết lập thời gian chạy
parser = argparse.ArgumentParser()
parser.add_argument('--timeout', type=int, default=600, help='Thời gian chạy (giây)')
args = parser.parse_args()

print(f"--- Kịch bản sẽ chạy trong {args.timeout} giây ---")
start_time = time.time()

if not TINYBIRD_TOKEN:
    print("❌ Lỗi: TINYBIRD_TOKEN chưa được thiết lập.")
    exit(1)

# Batch buffer
events_batch = []
BATCH_SIZE = 100  # Gửi mỗi 100 sự kiện

try:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    })
    print("🔗 Đang kết nối đến luồng Wikipedia SSE...")

    # Gửi request theo session
    response = session.get(WIKI_STREAM_URL, stream=True, timeout=30)

    # Tạo SSEClient từ response
    client = SSEClient(response)

    print("✅ Kết nối thành công. Bắt đầu lắng nghe...")

    # Quan trọng: dùng .events() thay vì for client
    for event in client.events():
        # Dừng khi hết thời gian
        if time.time() - start_time > args.timeout:
            print(f"⏱ Hết {args.timeout} giây. Dừng thu thập.")
            break

        if event.event == 'message':
            try:
                data = json.loads(event.data)

                if data.get("type") == "edit":
                    old_len = data.get("length", {}).get("old", 0) or 0
                    new_len = data.get("length", {}).get("new", 0) or 0
                    edit_size_bytes = new_len - old_len

                    ts_iso = datetime.utcfromtimestamp(data["timestamp"]).isoformat()

                    clean_event = {
                        "timestamp": ts_iso,
                        "title": data.get("title"),
                        "user": data.get("user"),
                        "is_bot": data.get("bot", False),
                        "server_name": data.get("server_name"),
                        "edit_size_bytes": edit_size_bytes,
                        "country_code": None
                    }
                    events_batch.append(clean_event)

                    if len(events_batch) >= BATCH_SIZE:
                        print(f"📤 Gửi batch {len(events_batch)} sự kiện...")
                        payload = "\n".join(json.dumps(e) for e in events_batch)
                        r = requests.post(
                            TINYBIRD_URL,
                            headers={"Authorization": f"Bearer {TINYBIRD_TOKEN}"},
                            data=payload
                        )
                        print(f"➡️ Tinybird phản hồi: {r.status_code}")
                        events_batch = []

            except json.JSONDecodeError:
                pass

except Exception as e:
    print(f"❌ Lỗi trong khi xử lý luồng: {e}")

# Gửi nốt phần còn lại
if events_batch:
    print(f"📤 Gửi nốt {len(events_batch)} sự kiện còn lại...")
    payload = "\n".join(json.dumps(e) for e in events_batch)
    r = requests.post(
        TINYBIRD_URL,
        headers={"Authorization": f"Bearer {TINYBIRD_TOKEN}"},
        data=payload
    )
    print(f"➡️ Tinybird phản hồi: {r.status_code}")

print("✅ Hoàn tất.")
