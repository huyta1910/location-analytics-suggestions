import time
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from pymongo import MongoClient
from urllib.parse import quote_plus


keywords = [
  # Nhóm: Quán nhậu, bia, rượu
  "quán nhậu", "quán bia", "nhà hàng bia", "quán rượu", "beer club", "pub",
  "quán nướng bia", "quán nhậu bình dân", "nhà hàng có phục vụ bia",
  "quán ăn có bia", "quán nhậu ngoài trời", "quán nhậu hải sản", "lẩu bia",
  "nhậu bình dân", "quán nhậu sân vườn", "quán nhậu giá rẻ",
  "quán nhậu sinh viên", "quán nhậu mở khuya", "quán nhậu vỉa hè",
  "quán nhậu đặc sản",

  # Nhóm: Karaoke - Giải trí
  "karaoke", "karaoke ktv", "karaoke gia đình", "karaoke vip",
  "karaoke bình dân", "karaoke có phục vụ đồ ăn", "karaoke phòng lớn",

  # Nhóm: Quán nướng - Lẩu - Nhà hàng
  "quán nướng", "quán lẩu nướng", "lẩu nướng ngoài trời", "nhà hàng hải sản",
  "quán nướng Hàn Quốc", "BBQ", "buffet nướng", "buffet lẩu",
  "quán ăn gia đình", "quán ăn đêm", "quán ăn ngon",

  # Nhóm: Nhà hàng tiệc cưới - Sự kiện
  "nhà hàng tiệc cưới", "nhà hàng tiệc cưới bình dân", "nhà hàng tiệc",
  "trung tâm tiệc cưới", "nhà hàng tổ chức sự kiện", "tiệc cưới ngoài trời",
  "nhà hàng sang trọng", "dịch vụ cưới hỏi",

  # Nhóm: Bar - Rượu - Bia đặc biệt
  "bar", "lounge", "quán bia hơi", "quán bia tươi", "quán bia Đức",
  "craft beer", "wine bar", "quán rượu vang"
]

#config
MONGO_URI = ""
DB_NAME = "crawl_restaurant_drink_beer"
LOCATION_COLLECTION_NAME = "location"
RESTAURANT_COLLECTION_NAME = "restaurant_drink_beer"

# Hàm để lấy dữ liệu vị trí từ MongoDB
def fetch_data_from_mongodb():
    """
    Kết nối đến MongoDB và lấy danh sách các vị trí.
    Nếu chưa có cơ sở dữ liệu, hàm này sẽ trả về một danh sách mẫu.
    """
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[LOCATION_COLLECTION_NAME]
        data = list(collection.find({}))
        client.close()
        if not data:
            print("Không tìm thấy dữ liệu vị trí trong MongoDB. Sử dụng dữ liệu mẫu.")
            # Dữ liệu mẫu nếu bạn chưa có MongoDB
            return [
                {"ward": "Phường Bến Nghé", "district": "Quận 1", "city": "Hồ Chí Minh"},
                {"ward": "Phường Thảo Điền", "district": "Quận 2", "city": "Hồ Chí Minh"}
            ]
        return data
    except Exception as e:
        print(f"Lỗi khi kết nối MongoDB: {e}. Sử dụng dữ liệu mẫu.")
        # Dữ liệu mẫu nếu bạn chưa có MongoDB
        return [
            {"ward": "Phường Bến Nghé", "district": "Quận 1", "city": "Hồ Chí Minh"},
            {"ward": "Phường Thảo Điền", "district": "Quận 2", "city": "Hồ Chí Minh"}
        ]

# Hàm để xóa một vị trí sau khi đã cào dữ liệu xong
def delete_location(location_id):
    """Xóa một tài liệu vị trí khỏi MongoDB bằng ID của nó."""
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[LOCATION_COLLECTION_NAME]
        result = collection.delete_one({"_id": location_id})
        client.close()
        return result
    except Exception as e:
        print(f"Lỗi khi xóa vị trí: {e}")
        return None

# Hàm chính để cào dữ liệu
def crawl_restaurant_drink_beer(key, loc):
    """
    Cào dữ liệu các nhà hàng từ Google Maps cho một từ khóa và vị trí cụ thể.
    """
    # Thiết lập trình duyệt Chrome
    service = Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless")  # Bỏ comment dòng này để chạy ở chế độ không giao diện
    driver = webdriver.Chrome(service=service, options=options)

    query = f"{key} {loc.get('ward', '')} {loc.get('district', '')}"
    url = f"https://www.google.com/maps/search/{quote_plus(query)}"
    
    driver.get(url)

    try:
        # Đợi cho đến khi vùng chứa kết quả được tải
        wait = WebDriverWait(driver, 10)
        feed_div = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="feed"]')))
        
        print("🗺️ Đang tải danh sách địa điểm...")
        time.sleep(2)

        # Cuộn xuống để tải thêm kết quả
        for i in range(10):
            print(f"🔄 Cuộn lần {i+1}...")
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", feed_div)
            time.sleep(2)

        print("🔍 Đang thu thập dữ liệu...")
        divs = driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")
        
        results = []
        for div in divs:
            try:
                a_tag = div.find_element(By.CSS_SELECTOR, "a.hfpxzc")
                name = a_tag.get_attribute("aria-label")
                link = a_tag.get_attribute("href")

                # Lấy thông tin chi tiết khác
                details_elements = div.find_elements(By.CSS_SELECTOR, ".W4Efsd span")
                details_text = [elem.text for elem in details_elements if elem.text]
                
                # Xử lý các thông tin trích xuất
                rating_reviews = details_text[0] if details_text else ""
                rating = rating_reviews.split(" ")[0] if rating_reviews else None
                
                category = details_text[1] if len(details_text) > 1 else None
                address = details_text[2] if len(details_text) > 2 else None
                opening_hours = details_text[3] if len(details_text) > 3 else None

                img = div.find_element(By.TAG_NAME, "img").get_attribute("src") if div.find_elements(By.TAG_NAME, "img") else None

                results.append({
                    "name": name,
                    "link": link,
                    "rating": rating,
                    "category": category,
                    "address": address,
                    "openingHours": opening_hours,
                    "img": img,
                    "ward": loc.get('ward'),
                    "district": loc.get('district'),
                    "city": loc.get('city')
                })
            except Exception as e:
                print(f"Lỗi khi trích xuất một địa điểm: {e}")

        # Lưu vào MongoDB
        if results:
            try:
                client = MongoClient(MONGO_URI)
                db = client[DB_NAME]
                collection = db[RESTAURANT_COLLECTION_NAME]
                collection.insert_many(results)
                print(
                    f"✅ Đã lưu {len(results)} địa điểm cho từ khóa \"{key}\" tại {loc.get('ward', '')}, {loc.get('district', '')}"
                )
            except Exception as e:
                print(f"Lỗi khi lưu dữ liệu vào MongoDB: {e}")
            finally:
                client.close()
        
        # Tùy chọn: Lưu vào file JSON nếu không có DB
        with open(f"results_{key}_{loc.get('district', '')}.json", "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=4)

    except Exception as e:
        print(f"Đã xảy ra lỗi trong quá trình cào dữ liệu: {e}")
    finally:
        driver.quit()

def main():
    """
    Hàm chính để điều phối quá trình cào dữ liệu.
    """
    locations = fetch_data_from_mongodb()
    if not locations:
        print("Không có dữ liệu vị trí để xử lý.")
        return

    for loc in locations:
        for key in keywords:
            print(
                f"🚀 Bắt đầu cào dữ liệu cho: {key} tại {loc.get('ward', '')}, {loc.get('district', '')}"
            )
            try:
                crawl_restaurant_drink_beer(key, loc)
            except Exception as e:
                print(f"Lỗi khi cào dữ liệu cho: {key} tại {loc.get('ward', '')}, {loc.get('district', '')}")
                print(e)
 
        # if loc.get('_id'):
        #     delete_location(loc['_id'])
        #     print(f" Đã xóa địa điểm: {loc.get('ward')}")

if __name__ == "__main__":
    main()