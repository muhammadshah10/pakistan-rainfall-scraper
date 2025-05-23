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
        'station': station_id, # station_id is already a string from opt['value']
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
                        continue
                    
                    parsed_date = pd.to_datetime(date_str, errors='coerce') 
                    
                    if pd.isna(parsed_date): # Skip if date could not be parsed
                        continue

                    entry = {
                        'Station ID': str(station_id), # Ensure station_id is stored as string
                        'Station Name': station_name,
                        'Province': cols[0].text.strip(),
                        'Reported Station': cols[1].text.strip(),
                        'Rainfall (mm)': cols[2].text.strip(),
                        'Date': parsed_date # Store as datetime object
                    }
                    rainfall_data.append(entry)

                    # Optional: Live output row by row
                    # print(f"{entry['Station ID']}, {entry['Station Name']}, {entry['Province']}, "
                    #       f"{entry['Reported Station']}, {entry['Rainfall (mm)']}, {entry['Date'].strftime('%d %b, %Y')}")

    except requests.exceptions.Timeout:
        print(f"❌ Timeout occurred for {station_name} (ID: {station_id})")
    except requests.exceptions.RequestException as e:
        print(f"❌ Request Error on {station_name} (ID: {station_id}): {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred on {station_name} (ID: {station_id}): {e}")

    time.sleep(0.25) # Slightly reduced sleep, adjust if needed

# Step 4: Convert scraped data to DataFrame
new_df = pd.DataFrame(rainfall_data)

if new_df.empty:
    print("\n⚠️ No new data was scraped. Exiting.")
    if os.path.exists("testRainfall.csv"):
         print(f"📁 Existing data remains in: testRainfall.csv\n")
    exit()
else:
    # Ensure 'Station ID' in new_df is string type. It should be from scraping logic, but explicit is safer.
    if 'Station ID' in new_df.columns:
        new_df['Station ID'] = new_df['Station ID'].astype(str)
    else:
        print("⚠️ Critical Error: 'Station ID' column is missing in newly scraped data. Cannot proceed.")
        exit()
    # 'Date' column in new_df is already datetime64 type due to pd.to_datetime during scraping.

# Step 5: (Original Step 5 is implicitly handled by date parsing during scraping and NaT checks)
# new_df should already have clean 'Date' column.

# Step 6: Load existing CSV if exists, then merge
csv_file = "testRainfall.csv"
combined_df = new_df.copy() # Start with new_df. Will be updated if existing data is loaded.

if os.path.exists(csv_file):
    print(f"\nℹ️ Existing CSV file '{csv_file}' found. Attempting to load and merge.")
    try:
        # Read 'Station ID' as string to ensure type consistency for merging.
        existing_df = pd.read_csv(csv_file, dtype={'Station ID': str})
        
        if existing_df.empty:
            print(f"ℹ️ Existing CSV '{csv_file}' was loaded but is empty. Will use new data only.")
            # combined_df is already set to new_df
        else:
            # Ensure 'Station ID' from existing_df is string. (Should be handled by dtype, but for safety)
            if 'Station ID' in existing_df.columns:
                existing_df['Station ID'] = existing_df['Station ID'].astype(str)
            else:
                print(f"⚠️ Warning: 'Station ID' column not found in existing CSV '{csv_file}'. Cannot reliably merge. Using new data only.")
                # combined_df remains new_df; skip to next stage with new_df only.
                existing_df = pd.DataFrame() # Empty it to prevent further processing

            if not existing_df.empty and 'Date' in existing_df.columns:
                existing_df['Date'] = pd.to_datetime(existing_df['Date'], errors='coerce')
                existing_df.dropna(subset=['Date'], inplace=True) # Remove rows where date couldn't be parsed
                
                if existing_df.empty:
                    print(f"ℹ️ Existing CSV '{csv_file}' became empty after date parsing/cleaning. Will use new data only.")
                    # combined_df is already set to new_df
                else:
                    print(f"✅ Successfully loaded and processed {len(existing_df)} rows from existing data in '{csv_file}'. Merging with {len(new_df)} new rows.")
                    # Concatenate old and new data. new_df is guaranteed non-empty here.
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    print(f"ℹ️ Combined DataFrame has {len(combined_df)} rows before deduplication.")
            elif not existing_df.empty: # existing_df not empty, but 'Date' column missing
                 print(f"⚠️ Warning: 'Date' column not found in existing CSV '{csv_file}'. Cannot reliably merge. Using new data only.")
                 # combined_df is already set to new_df
                 
    except pd.errors.EmptyDataError:
        print(f"⚠️ Existing CSV '{csv_file}' is empty (caught EmptyDataError). Will use new data only.")
        # combined_df is already set to new_df
    except Exception as e:
        print(f"⚠️ Error reading or processing existing CSV '{csv_file}': {e}. Will use new data only.")
        # combined_df is already set to new_df
else:
    print(f"ℹ️ No existing CSV file found at '{csv_file}'. Starting with new data only.")
    # combined_df is already set to new_df


# Step 7: Final processing on combined_df
if not combined_df.empty:
    # Ensure 'Date' column is datetime type (especially if combined_df is just new_df or if concat introduced mixed types)
    if 'Date' in combined_df.columns:
        combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
        combined_df.dropna(subset=['Date'], inplace=True) 
    else:
        print("⚠️ Warning: 'Date' column missing in combined_df. Cannot perform date-based operations or save correctly.")
        # Consider exiting or handling this error more robustly if 'Date' is critical

    # Ensure 'Station ID' is string type before deduplication (should be, but as a final check)
    if 'Station ID' in combined_df.columns:
        combined_df['Station ID'] = combined_df['Station ID'].astype(str)
    else:
        print("⚠️ Warning: 'Station ID' column missing in combined_df. Cannot perform deduplication correctly.")

    # Remove duplicates if all necessary columns exist
    dedup_cols = ['Station ID', 'Date', 'Reported Station']
    if all(col in combined_df.columns for col in dedup_cols) and not combined_df.empty:
        initial_rows = len(combined_df)
        combined_df.drop_duplicates(subset=dedup_cols, keep='last', inplace=True)
        print(f"ℹ️ Deduplication removed {initial_rows - len(combined_df)} rows. Combined DataFrame now has {len(combined_df)} rows.")
    elif not combined_df.empty:
        print(f"⚠️ Skipping deduplication because one or more key columns ({dedup_cols}) are missing or DataFrame is empty.")

else:
    print("\n⚠️ Combined DataFrame is empty before final processing. Nothing to save.")


# Step 8: Sort and save
if not combined_df.empty and 'Date' in combined_df.columns: # Ensure Date column exists for sorting and formatting
    # Sort values
    sort_by_cols = ['Date', 'Station Name']
    if all(col in combined_df.columns for col in sort_by_cols):
        combined_df.sort_values(by=sort_by_cols, ascending=[False, True], inplace=True)
    elif 'Date' in combined_df.columns: # Fallback to sort by Date only if Station Name is missing
        print("ℹ️ 'Station Name' column missing, sorting by 'Date' only.")
        combined_df.sort_values(by=['Date'], ascending=False, inplace=True)
    
    # Convert 'Date' column to desired string format '%d %b, %Y' before saving
    combined_df['Date'] = combined_df['Date'].dt.strftime('%d %b, %Y')

    try:
        combined_df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        print(f"\n✅ Scraping and processing completed successfully!")
        print(f"📁 Updated data saved to: {csv_file} ({len(combined_df)} rows)\n")
    except Exception as e:
        print(f"❌ Error saving data to CSV '{csv_file}': {e}")

elif combined_df.empty:
    print("\n⚠️ Combined data is empty after all processing. Nothing was saved.")
else: # Not empty, but 'Date' column missing, problematic for standard save.
    print(f"\n⚠️ Combined data is not empty ({len(combined_df)} rows) but essential 'Date' column is missing. Cannot save in standard format.")
    # Optionally, try saving without date formatting or handle differently
    # combined_df.to_csv(csv_file, index=False, encoding='utf-8-sig')
    # print(f"📁 Data (potentially without proper date formatting) saved to: {csv_file}\n")
