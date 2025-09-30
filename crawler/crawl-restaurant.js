const keywords = [
  // Nhóm: Quán nhậu, bia, rượu
  "quán nhậu",
  "quán bia",
  "nhà hàng bia",
  "quán rượu",
  "beer club",
  "pub",
  "quán nướng bia",
  "quán nhậu bình dân",
  "nhà hàng có phục vụ bia",
  "quán ăn có bia",
  "quán nhậu ngoài trời",
  "quán nhậu hải sản",
  "lẩu bia",
  "nhậu bình dân",
  "quán nhậu sân vườn",
  "quán nhậu giá rẻ",
  "quán nhậu sinh viên",
  "quán nhậu mở khuya",
  "quán nhậu vỉa hè",
  "quán nhậu đặc sản",

  // Nhóm: Karaoke - Giải trí
  "karaoke",
  "karaoke ktv",
  "karaoke gia đình",
  "karaoke vip",
  "karaoke bình dân",
  "karaoke có phục vụ đồ ăn",
  "karaoke phòng lớn",

  // Nhóm: Quán nướng - Lẩu - Nhà hàng
  "quán nướng",
  "quán lẩu nướng",
  "lẩu nướng ngoài trời",
  "nhà hàng hải sản",
  "quán nướng Hàn Quốc",
  "BBQ",
  "buffet nướng",
  "buffet lẩu",
  "quán ăn gia đình",
  "quán ăn đêm",
  "quán ăn ngon",

  // Nhóm: Nhà hàng tiệc cưới - Sự kiện
  "nhà hàng tiệc cưới",
  "nhà hàng tiệc cưới bình dân",
  "nhà hàng tiêc",  // lỗi chính tả vẫn giữ nguyên như mẫu gốc
  "trung tâm tiệc cưới",
  "nhà hàng tổ chức sự kiện",
  "tiệc cưới ngoài trời",
  "nhà hàng sang trọng",
  "dịch vụ cưới hỏi",

  // Nhóm: Bar - Rượu - Bia đặc biệt
  "bar",
  "lounge",
  "quán bia hơi",
  "quán bia tươi",
  "quán bia Đức",
  "craft beer",
  "wine bar",
  "quán rượu vang"
];


const conStr = "mongodb://127.0.0.1:27017/";
const dbName = "location";
const collectionName = "places";
const restaurantCollectionName = "restaurant_drink_beer";

// import necessary modules
const mongodb = require("mongodb");
const puppeteer = require("puppeteer");

//fetch data from MongoDB
async function fetchDataFromMongoDB() {
  const client = new mongodb.MongoClient(conStr);
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const data = await collection.find({}).toArray();
    return data;
  } catch (error) {
    console.error("Error fetching data from MongoDB:", error);
  } finally {
    await client.close();
  }
}

// delete location
async function deleteLocation(locationId) {
  const client = new mongodb.MongoClient(conStr);
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const data = await collection.deleteOne({ _id: locationId })
    return data;
  } catch (error) {
    console.error("Error fetching data from MongoDB:", error);
  } finally {
    await client.close();
  }
}

// sleep await func
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const crawl_restaurant_drink_beer = async (key, loc) => {
  const browser = await puppeteer.launch({
    headless: false,
    defaultViewport: null,
  });

  const page = await browser.newPage();

  const query = `${key} ${loc?.ward} ${loc?.district}`;

  const url = `https://www.google.com/maps/search/${encodeURIComponent(query)}`;

  await page.goto(url, { waitUntil: "networkidle2" });

  await page.waitForSelector('div[role="feed"]');
  console.log("Loading the list of locations...");

  await sleep(2000);

  const scrollContainer = await page.$('div[role="feed"]');
  for (let i = 1; i <= 10; i++) {
    console.log(`Scrolling ${i}...`);
    await page.evaluate((sc) => sc.scrollBy(0, 2000), scrollContainer);
    await sleep(2000);
  }

  console.log("Collecting...");
  const data = await page.$$eval("div.Nv2PK", (divs) => {
    function extractLatLng(href) {
      const latMatch = href.match(/!3d([0-9.-]+)/);
      const lngMatch = href.match(/!4d([0-9.-]+)/);
      return latMatch && lngMatch
        ? { lat: parseFloat(latMatch[1]), lng: parseFloat(lngMatch[1]) }
        : null;
    }

    return divs.map((div) => {
      const aTag = div.querySelector("a.hfpxzc");
      const name = aTag?.getAttribute("aria-label") || null;
      const link = aTag?.href || null;

      const latlng = link ? extractLatLng(link) : null;

      // lấy toàn bộ W4Efsd
      const w4Elements = div.querySelectorAll(".W4Efsd");
      let rating = null;
      let reviews = null;
      let priceText = null;
      let category = null;
      let address = null;
      let openingHours = null;
      let description = null;

      if (w4Elements.length >= 1) {
        const firstText = w4Elements[0].innerText.trim(); // VD: 4.8(54) · ₫100–200K
        const ratingMatch = firstText.match(/^([0-9.]+)\(?(\d*)\)?/);
        const priceMatch = firstText.includes("·")
          ? firstText.split("·")[1].trim()
          : null;
        description = firstText
        rating = ratingMatch ? parseFloat(ratingMatch[1]) : null;
        reviews =
          ratingMatch && ratingMatch[2] ? parseInt(ratingMatch[2]) : null;
        priceText = priceMatch;
      }

      category = w4Elements[1]?.innerText || null;
      address = w4Elements[2]?.innerText || null;
      openingHours = w4Elements[3]?.innerText || null;

      // Các dịch vụ và review snippet
      const serviceBlocks = div.querySelectorAll(".W6VQef");
      const services = Array.from(serviceBlocks)
        .map((e) => e.innerText.trim())
        .filter(Boolean);

      const reviewSnippets = Array.from(div.querySelectorAll(".ah5Ghc"))
        .map((e) => e.innerText.trim())
        .filter(Boolean);

      const img = div.querySelector("img")?.src || null;
     
      return {
        name,
        link,
        ...latlng,
        rating,
        reviews,
        priceText,
        category,
        description,
        address,
        openingHours,
        reviewSnippets,
        services,
        img,
      };
    });
  });

  // save to mongoDB
  const client = new mongodb.MongoClient(conStr);
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(restaurantCollectionName);

    const dataFormatted = data.map((item) => ({
      ...item,
      ward: loc.ward,
      district: loc.district,
      city: loc.city,
    }))

    await collection.insertMany(dataFormatted);
    console.log(
      `Saved ${data.length} location for keyword "${key}" tại ${loc.ward}, ${loc.district}`
    );
  } catch (error) {
    console.error("Error saving data to MongoDB:", error);
  } finally {
    await client.close();
    await browser.close();
  }
};

const main = async () => {
  // Fetch location data from MongoDB
  const locations = await fetchDataFromMongoDB();
  if (!locations || locations.length === 0) {
    console.error("No location data found in MongoDB.");
    return;
  }

    for (const loc of locations) {
      for (const key of keywords) {
        console.log(
          `Starting crawl data for: ${key} tại ${loc.ward}, ${loc.district}`
        );
       try {
         await crawl_restaurant_drink_beer(key, loc);
       } catch (error) {
        console.log(`error to crawl for: ${key} tại ${loc.ward}, ${loc.district}`);
        console.error(error);
       }
      }

      await deleteLocation(loc._id)
      console.log(`Deleted location! : ${loc.ward}`)
    }
  
};

main();
