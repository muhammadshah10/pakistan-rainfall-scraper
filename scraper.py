import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from tqdm import tqdm
from colorama import Fore, Style

# Base URL
url = "https://nwfc.pmd.gov.pk/new/rainfall.php"

# Start a session
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0"
})

# Get the main page to fetch station options
response = session.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

# Extract station options
stations = soup.select("select[name='station'] option")
station_list = [(opt['value'], opt.text.strip()) for opt in stations if opt['value'].isdigit()]

# Storage for all rainfall results
rainfall_data = []

# Title
print(Fore.CYAN + Style.BRIGHT + "\n📊 Starting Rainfall Data Scraping for Pakistan\n" + Style.RESET_ALL)

# Loop with styled tqdm
for station_id, station_name in tqdm(station_list, desc="⏳ Scraping Station Data", unit="station", ncols=100, colour="green"):
    form_data = {
        'station': station_id,
        'filter': 'station'
    }

    resp = session.post(url, data=form_data)
    page = BeautifulSoup(resp.text, 'html.parser')

    table = page.find("table", class_="table table-bordered")
    if table:
        rows = table.find_all("tr")[1:]  # Skip header
        for row in rows:
            cols = row.find_all("td")
            if len(cols) == 4:
                data_row = {
                    'Station ID': station_id,
                    'Station Name': station_name,
                    'Province': cols[0].text.strip(),
                    'Reported Station': cols[1].text.strip(),
                    'Rainfall (mm)': cols[2].text.strip(),
                    'Date': cols[3].text.strip()
                }
                rainfall_data.append(data_row)

                # Print each data row as it's scraped
                print(
                    Fore.GREEN + f"📍 {data_row['Station Name']} ({data_row['Province']}) - "
                    f"{data_row['Reported Station']}: {data_row['Rainfall (mm)']} mm on {data_row['Date']}"
                    + Style.RESET_ALL
                )

    time.sleep(0.5)  # Respect the server

# Create DataFrame
df = pd.DataFrame(rainfall_data)

# Convert Date column to datetime and sort it
df['Date'] = pd.to_datetime(df['Date'], errors='coerce', dayfirst=True)
df = df.sort_values(by='Date', ascending=False)

# Save to CSV
df.to_csv("DateWiseRainfallData.csv", index=False)

# Completion message
print(Fore.GREEN + Style.BRIGHT + "\n✅ Scraping completed successfully!")
print(f"📁 Data saved to: {Style.RESET_ALL}DateWiseRainfallData.csv\n")
