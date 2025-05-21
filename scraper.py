import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from tqdm import tqdm
import os

# Base URL
url = "https://nwfc.pmd.gov.pk/new/rainfall.php"

# Start session
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
})

# Step 1: Fetch station options
try:
    response = session.get(url, timeout=20) # Increased timeout for initial fetch
    response.raise_for_status() # Check for HTTP errors
except requests.exceptions.RequestException as e:
    print(f"❌ Error fetching station list: {e}")
    exit()

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
        res.raise_for_status() # Check for HTTP errors
        page = BeautifulSoup(res.text, 'html.parser')
        table = page.find("table", class_="table table-bordered")

        if table:
            rows = table.find_all("tr")[1:]  # skip headers
            for row in rows:
                cols = row.find_all("td")
                if len(cols) == 4:
                    date_str = cols[3].text.strip()

                    if not date_str: # Skip if date string is empty
                        # print(f"ℹ️ Empty date for {station_name}, row: {[c.text.strip() for c in cols]}. Skipping.")
                        continue
                    
                    # Parse date string to datetime object
                    parsed_date = pd.to_datetime(date_str, errors='coerce') 
                    # Expected format from site "28 May, 2024" which pd.to_datetime handles well.
                    
                    if pd.isna(parsed_date): # Skip if date could not be parsed
                        # print(f"⚠️ Warning: Could not parse date '{date_str}' for {station_name}. Skipping row.")
                        continue

                    entry = {
                        'Station ID': station_id,
                        'Station Name': station_name,
                        'Province': cols[0].text.strip(),
                        'Reported Station': cols[1].text.strip(),
                        'Rainfall (mm)': cols[2].text.strip(),
                        'Date': parsed_date # Store as datetime object
                    }
                    rainfall_data.append(entry)

                    # Live output row by row (format date for printing)
                    print(f"{entry['Station ID']}, {entry['Station Name']}, {entry['Province']}, "
                          f"{entry['Reported Station']}, {entry['Rainfall (mm)']}, {entry['Date'].strftime('%d %b, %Y')}")

    except requests.exceptions.Timeout:
        print(f"❌ Timeout occurred for {station_name}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Request Error on {station_name}: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred on {station_name}: {e}")

    time.sleep(0.5)  # Respect server

# Step 4: Convert to DataFrame
# 'Date' column will be of datetime64 type
new_df = pd.DataFrame(rainfall_data)

if new_df.empty:
    print("\n⚠️ No new data was scraped. Exiting.")
    # Potentially save existing CSV again if needed, or just exit
    if os.path.exists("testRainfall.csv"):
         print(f"📁 Existing data remains in: testRainfall.csv\n")
    exit()


# Step 5: (Original Step 5 for filtering blanks is handled by parse checks now)
# Ensure 'Date' column is clean in new_df (already done by checks during scraping)
new_df = new_df.dropna(subset=['Date'])


# Step 6: Load existing CSV if exists, then merge
csv_file = "testRainfall.csv"
if os.path.exists(csv_file):
    try:
        existing_df = pd.read_csv(csv_file)
        # Convert 'Date' column in existing_df to datetime objects
        # This handles various formats it might have been saved in previously.
        existing_df['Date'] = pd.to_datetime(existing_df['Date'], errors='coerce')
        existing_df = existing_df.dropna(subset=['Date']) # Remove rows where date couldn't be parsed
        
        # Concatenate old and new data
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    except pd.errors.EmptyDataError:
        print(f"⚠️ Existing CSV '{csv_file}' is empty. Will use new data only.")
        combined_df = new_df
    except Exception as e:
        print(f"⚠️ Error reading existing CSV '{csv_file}': {e}. Will use new data only.")
        combined_df = new_df
else:
    combined_df = new_df

# Step 7: Ensure 'Date' column is datetime type across the combined DataFrame
# (This should already be true if above steps worked, but as a safeguard)
combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
combined_df = combined_df.dropna(subset=['Date']) # Final cleaning of any NaT dates

# Remove duplicates:
# Keep='last' ensures that if new_df has entries that are "duplicates"
# (same Station ID, Date, Reported Station) of existing_df entries,
# the ones from new_df (the latest scrape) are kept.
combined_df = combined_df.drop_duplicates(subset=['Station ID', 'Date', 'Reported Station'], keep='last')

# Step 8: Sort and save
combined_df = combined_df.sort_values(by=['Date', 'Station Name'], ascending=[False, True])

# OPTIONAL: Convert 'Date' column to desired string format '%d %b, %Y' before saving
# If you prefer dates in YYYY-MM-DD format in CSV, you can skip this line.
combined_df['Date'] = combined_df['Date'].dt.strftime('%d %b, %Y')

# Save the CSV with proper encoding
combined_df.to_csv(csv_file, index=False, encoding='utf-8-sig')

print("\n✅ Scraping and processing completed successfully!")
print(f"📁 Updated data saved to: {csv_file}\n")
