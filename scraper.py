import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from tqdm import tqdm
import os
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Base URL
url = "https://nwfc.pmd.gov.pk/new/rainfall.php"
csv_file = "testRainfall.csv"

# Start session
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
})

# Step 1: Fetch station list
try:
    response = session.get(url, timeout=20, verify=False)  # Bypass SSL verification
    response.raise_for_status()
except requests.exceptions.RequestException as e:
    print(f"❌ Error fetching station list: {e}")
    exit()

soup = BeautifulSoup(response.text, 'html.parser')
stations = soup.select("select[name='station'] option")
station_list = [(opt['value'], opt.text.strip()) for opt in stations if opt['value'].isdigit()]

# Step 2: Scrape rainfall data
rainfall_data = []
print("\n📊 Scraping Rainfall Data...\n")
for station_id, station_name in tqdm(station_list, desc="🔍 Scraping", unit="station"):
    form_data = {
        'station': station_id,
        'filter': 'station'
    }

    try:
        res = session.post(url, data=form_data, timeout=10, verify=False)  # Bypass SSL verification
        res.raise_for_status()
        page = BeautifulSoup(res.text, 'html.parser')
        table = page.find("table", class_="table table-bordered")

        if table:
            rows = table.find_all("tr")[1:]
            for row in rows:
                cols = row.find_all("td")
                if len(cols) == 4:
                    date_str = cols[3].text.strip()
                    parsed_date = pd.to_datetime(date_str, format='%d %b, %Y', dayfirst=True, errors='coerce')
                    if pd.isna(parsed_date):
                        continue

                    entry = {
                        'Station ID': str(station_id),
                        'Station Name': station_name,
                        'Province': cols[0].text.strip(),
                        'Reported Station': cols[1].text.strip(),
                        'Rainfall (mm)': cols[2].text.strip(),
                        'Date': parsed_date
                    }
                    rainfall_data.append(entry)

    except Exception as e:
        print(f"❌ Error at {station_name} (ID: {station_id}): {e}")

    time.sleep(0.25)

# Step 3: Convert to DataFrame
new_df = pd.DataFrame(rainfall_data)
if new_df.empty:
    print("⚠️ No new rainfall data found. Exiting.")
    exit()

new_df['Station ID'] = new_df['Station ID'].astype(str)
new_df['Date'] = pd.to_datetime(new_df['Date'], errors='coerce')
new_df.dropna(subset=['Date'], inplace=True)

# Step 4: Load existing CSV and merge
if os.path.exists(csv_file):
    try:
        print(f"\n📂 Reading existing data from '{csv_file}'...")
        existing_df = pd.read_csv(csv_file, encoding='utf-8-sig', dtype={'Station ID': str})
        existing_df['Date'] = pd.to_datetime(existing_df['Date'], format='%d %b, %Y', errors='coerce')
        existing_df.dropna(subset=['Date'], inplace=True)
        print(f"✅ Existing rows: {len(existing_df)}")
    except Exception as e:
        print(f"⚠️ Failed to load existing data: {e}")
        existing_df = pd.DataFrame()
else:
    existing_df = pd.DataFrame()

# Step 5: Merge, deduplicate, and filter by date
combined_df = pd.concat([existing_df, new_df], ignore_index=True)
before_dedup = len(combined_df)
combined_df.drop_duplicates(subset=['Station ID', 'Date', 'Reported Station'], keep='last', inplace=True)
after_dedup = len(combined_df)
print(f"🧹 Removed {before_dedup - after_dedup} duplicates. Final rows: {after_dedup}")

# Filter from 1 April 2025 onwards
combined_df = combined_df[combined_df['Date'] >= pd.to_datetime('2025-04-01')]

# Step 6: Sort and Save
combined_df.sort_values(by=['Date', 'Station Name'], ascending=[False, True], inplace=True)
combined_df['Date'] = combined_df['Date'].dt.strftime('%d %b, %Y')

try:
    combined_df.to_csv(csv_file, index=False, encoding='utf-8-sig')
    print(f"\n✅ Rainfall data saved to '{csv_file}' with {len(combined_df)} rows (from 1 Apr 2025 onwards).")
except Exception as e:
    print(f"❌ Failed to save data: {e}")
