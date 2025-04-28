import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os

# ScraperAPI URL with your API key
SCRAPERAPI_URL = "http://api.scraperapi.com"
API_KEY = "c0b71f525b1c7e7272d0a1b9968d6f98"

# Base URL
url = "https://nwfc.pmd.gov.pk/new/rainfall.php"

# Start session
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0"
})

# Retry configuration
MAX_RETRIES = 5  # Number of retry attempts
RETRY_DELAY = 5  # Seconds delay before retrying

# Step 1: Fetch station options with retries
retries = 0
response = None

while retries < MAX_RETRIES:
    try:
        # Use ScraperAPI for fetching the page
        response = session.get(f"{SCRAPERAPI_URL}?api_key={API_KEY}&url={url}")
        response.raise_for_status()  # Raise error for unsuccessful responses (4xx, 5xx)
        break  # If successful, break out of the loop
    except requests.exceptions.RequestException as e:
        retries += 1
        print(f"❌ Attempt {retries}/{MAX_RETRIES} failed. Retrying in {RETRY_DELAY} seconds...")
        time.sleep(RETRY_DELAY)  # Wait before retrying

if not response:
    print("❌ All attempts failed. Exiting.")
    exit(1)  # Exit if all retries fail

# Step 2: Parse the page and get stations
soup = BeautifulSoup(response.text, 'html.parser')
stations = soup.select("select[name='station'] option")
station_list = [(opt['value'], opt.text.strip()) for opt in stations if opt['value'].isdigit()]

# Step 3: Prepare to store scraped data
rainfall_data = []

# Step 4: Function to scrape each station
def scrape_station(station_id, station_name):
    form_data = {
        'station': station_id,
        'filter': 'station'
    }

    try:
        res = session.post(f"{SCRAPERAPI_URL}?api_key={API_KEY}&url={url}", data=form_data)
        page = BeautifulSoup(res.text, 'html.parser')
        table = page.find("table", class_="table table-bordered")

        if table:
            rows = table.find_all("tr")[1:]  # skip headers
            for row in rows:
                cols = row.find_all("td")
                if len(cols) == 4:
                    date = cols[3].text.strip()

                    # Skip if date is missing
                    if not date:
                        continue

                    entry = {
                        'Station ID': station_id,
                        'Station Name': station_name,
                        'Province': cols[0].text.strip(),
                        'Reported Station': cols[1].text.strip(),
                        'Rainfall (mm)': cols[2].text.strip(),
                        'Date': date
                    }

                    rainfall_data.append(entry)

                    # 👇 Live output row by row
                    print(f"{entry['Station ID']}, {entry['Station Name']}, {entry['Province']}, "
                          f"{entry['Reported Station']}, {entry['Rainfall (mm)']}, {entry['Date']}")

    except Exception as e:
        print(f"❌ Error on {station_name}: {e}")

# Step 5: Use ThreadPoolExecutor for parallel scraping
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(scrape_station, station_id, station_name) for station_id, station_name in station_list]
    
    # Wait for all futures to complete
    for future in as_completed(futures):
        future.result()  # This will raise exceptions if any occurred

# Step 6: Convert to DataFrame
new_df = pd.DataFrame(rainfall_data)

# Step 7: Remove rows with blank or NaT in date before proceeding
new_df = new_df[new_df['Date'].str.strip() != ""]

# Step 8: Load existing CSV if exists, then merge
csv_file = "pakistan_rainfall_data.csv"
if os.path.exists(csv_file):
    existing_df = pd.read_csv(csv_file)
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
else:
    combined_df = new_df

# Step 9: Convert Date column to datetime and clean
combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce', dayfirst=True)
combined_df = combined_df.dropna(subset=['Date'])  # remove rows where Date couldn't be parsed
combined_df = combined_df.drop_duplicates(subset=['Station ID', 'Date', 'Reported Station'])

# Step 10: Sort and save
combined_df = combined_df.sort_values(by='Date', ascending=False)
combined_df.to_csv(csv_file, index=False)

print("\n✅ Scraping completed successfully!")
print(f"📁 Updated data saved to: {csv_file}\n")
