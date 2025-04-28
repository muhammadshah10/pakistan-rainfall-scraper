import requests
from bs4 import BeautifulSoup
import time
from requests.exceptions import RequestException

# Base URL
url = "https://nwfc.pmd.gov.pk/new/rainfall.php"

# Start session
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0"
})

# Retry configuration
MAX_RETRIES = 5
RETRY_DELAY = 5  # Seconds delay before retrying

# Step 1: Fetch station options with retries
retries = 0
response = None

while retries < MAX_RETRIES:
    try:
        response = session.get(url)
        response.raise_for_status()  # Check for successful response
        break  # If successful, break out of the loop
    except RequestException as e:
        retries += 1
        print(f"❌ Attempt {retries}/{MAX_RETRIES} failed. Retrying in {RETRY_DELAY} seconds...")
        time.sleep(RETRY_DELAY)  # Wait before retrying

if not response:
    print("❌ All attempts failed. Exiting.")
    exit(1)  # Exit if all retries fail

# Proceed with scraping if successful
soup = BeautifulSoup(response.text, 'html.parser')
stations = soup.select("select[name='station'] option")
station_list = [(opt['value'], opt.text.strip()) for opt in stations if opt['value'].isdigit()]

# Step 2: Prepare to store scraped data
rainfall_data = []

# Continue scraping as usual...
