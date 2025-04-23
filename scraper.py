import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from tqdm import tqdm

# Base URL
url = "https://nwfc.pmd.gov.pk/new/rainfall.php"

# Start session
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0"
})

# Step 1: Fetch station options
response = session.get(url)
soup = BeautifulSoup(response.text, 'html.parser')
stations = soup.select("select[name='station'] option")
station_list = [(opt['value'], opt.text.strip()) for opt in stations if opt['value'].isdigit()]

# Step 2: Prepare to store scraped data
rainfall_data = []

# Step 3: Scrape each station using tqdm
print("\n📊 Scraping Rainfall Data...\n")
for station_id, station_name in tqdm(station_list, desc="🔍 Scraping", unit="station"):
    form_data = {
        'station': station_id,
        'filter': 'station'
    }

    try:
        res = session.post(url, data=form_data, timeout=10)
        page = BeautifulSoup(res.text, 'html.parser')
        table = page.find("table", class_="table table-bordered")

        if table:
            rows = table.find_all("tr")[1:]  # skip headers
            for row in rows:
                cols = row.find_all("td")
                if len(cols) == 4:
                    province = cols[0].text.strip()
                    reported_station = cols[1].text.strip()
                    rainfall = cols[2].text.strip()
                    date = cols[3].text.strip()

                    entry = {
                        'Station ID': station_id,
                        'Station Name': station_name,
                        'Province': province,
                        'Reported Station': reported_station,
                        'Rainfall (mm)': rainfall,
                        'Date': date
                    }

                    rainfall_data.append(entry)

                    # 👇 Live output row by row
                    print(f"{entry['Station ID']}, {entry['Station Name']}, {entry['Province']}, "
                          f"{entry['Reported Station']}, {entry['Rainfall (mm)']}, {entry['Date']}")

    except Exception as e:
        print(f"❌ Error on {station_name}: {e}")

    time.sleep(1)  # Avoid overloading server

# Step 4: Save to CSV
df = pd.DataFrame(rainfall_data)
df.to_csv("pakistan_rainfall_data.csv", index=False)

print("\n✅ Done! Data saved to 'pakistan_rainfall_data.csv'")
