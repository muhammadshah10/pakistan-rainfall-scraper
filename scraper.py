import os
import time
import requests
import pandas as pd

from bs4 import BeautifulSoup
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================
# Configuration
# =========================
BASE_URL = "https://nwfc.pmd.gov.pk/"
RAINFALL_URL = "https://nwfc.pmd.gov.pk/new/rainfall.php"
CSV_FILE = "testRainfall.csv"
DATE_FILTER_FROM = "2025-04-01"


# =========================
# Create session
# =========================
def create_session():
    session = requests.Session()

    retry_strategy = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.5,
        status_forcelist=[403, 429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": BASE_URL,
    })

    return session


# =========================
# Fetch station list
# =========================
def fetch_station_list(session):
    print("🌐 Warming up session with homepage...")
    home_response = session.get(BASE_URL, timeout=20)
    print(f"🏠 Homepage status: {home_response.status_code}")
    home_response.raise_for_status()

    print("🌧️ Fetching rainfall page...")
    response = session.get(RAINFALL_URL, timeout=25)
    print(f"📄 Rainfall page status: {response.status_code}")

    if response.status_code == 403:
        print("❌ Access denied (403) while fetching rainfall page.")
        print("This usually means the server is blocking the GitHub Actions runner/IP.")
        response.raise_for_status()

    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    stations = soup.select("select[name='station'] option")

    station_list = []
    for opt in stations:
        value = opt.get("value", "").strip()
        label = opt.text.strip()
        if value.isdigit():
            station_list.append((value, label))

    print(f"✅ Total stations found: {len(station_list)}")
    return station_list


# =========================
# Scrape rainfall data for all stations
# =========================
def scrape_rainfall_data(session, station_list):
    rainfall_data = []

    print("\n📊 Scraping Rainfall Data...\n")

    for station_id, station_name in tqdm(station_list, desc="🔍 Scraping", unit="station"):
        form_data = {
            "station": station_id,
            "filter": "station"
        }

        try:
            res = session.post(
                RAINFALL_URL,
                data=form_data,
                timeout=20,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": BASE_URL.rstrip("/"),
                    "Referer": RAINFALL_URL,
                    "User-Agent": session.headers["User-Agent"],
                    "Accept": session.headers["Accept"],
                    "Accept-Language": session.headers["Accept-Language"],
                }
            )

            if res.status_code == 403:
                print(f"❌ 403 blocked at station {station_name} (ID: {station_id})")
                continue

            res.raise_for_status()

            page = BeautifulSoup(res.text, "html.parser")
            table = page.find("table", class_="table table-bordered")

            if table:
                rows = table.find_all("tr")[1:]

                for row in rows:
                    cols = row.find_all("td")
                    if len(cols) == 4:
                        province = cols[0].text.strip()
                        reported_station = cols[1].text.strip()
                        rainfall_mm = cols[2].text.strip()
                        date_str = cols[3].text.strip()

                        parsed_date = pd.to_datetime(
                            date_str,
                            format="%d %b, %Y",
                            dayfirst=True,
                            errors="coerce"
                        )

                        if pd.isna(parsed_date):
                            continue

                        entry = {
                            "Station ID": str(station_id),
                            "Station Name": station_name,
                            "Province": province,
                            "Reported Station": reported_station,
                            "Rainfall (mm)": rainfall_mm,
                            "Date": parsed_date
                        }
                        rainfall_data.append(entry)

        except requests.exceptions.RequestException as e:
            print(f"❌ Request error at {station_name} (ID: {station_id}): {e}")
        except Exception as e:
            print(f"❌ General error at {station_name} (ID: {station_id}): {e}")

        time.sleep(0.4)

    return rainfall_data


# =========================
# Load existing CSV
# =========================
def load_existing_data(csv_file):
    if os.path.exists(csv_file):
        try:
            print(f"\n📂 Reading existing data from '{csv_file}'...")
            existing_df = pd.read_csv(
                csv_file,
                encoding="utf-8-sig",
                dtype={"Station ID": str}
            )

            if "Date" in existing_df.columns:
                existing_df["Date"] = pd.to_datetime(
                    existing_df["Date"],
                    format="%d %b, %Y",
                    errors="coerce"
                )
                existing_df.dropna(subset=["Date"], inplace=True)

            print(f"✅ Existing rows: {len(existing_df)}")
            return existing_df

        except Exception as e:
            print(f"⚠️ Failed to load existing data: {e}")
            return pd.DataFrame()

    return pd.DataFrame()


# =========================
# Save merged data
# =========================
def process_and_save_data(rainfall_data, csv_file):
    new_df = pd.DataFrame(rainfall_data)

    if new_df.empty:
        print("⚠️ No new rainfall data found. Exiting.")
        return

    new_df["Station ID"] = new_df["Station ID"].astype(str)
    new_df["Date"] = pd.to_datetime(new_df["Date"], errors="coerce")
    new_df.dropna(subset=["Date"], inplace=True)

    existing_df = load_existing_data(csv_file)

    combined_df = pd.concat([existing_df, new_df], ignore_index=True)

    before_dedup = len(combined_df)
    combined_df.drop_duplicates(
        subset=["Station ID", "Date", "Reported Station"],
        keep="last",
        inplace=True
    )
    after_dedup = len(combined_df)

    print(f"🧹 Removed {before_dedup - after_dedup} duplicates. Final rows: {after_dedup}")

    combined_df = combined_df[combined_df["Date"] >= pd.to_datetime(DATE_FILTER_FROM)]

    combined_df.sort_values(
        by=["Date", "Station Name"],
        ascending=[False, True],
        inplace=True
    )

    combined_df["Date"] = combined_df["Date"].dt.strftime("%d %b, %Y")

    try:
        combined_df.to_csv(csv_file, index=False, encoding="utf-8-sig")
        print(
            f"\n✅ Rainfall data saved to '{csv_file}' "
            f"with {len(combined_df)} rows (from 1 Apr 2025 onwards)."
        )
    except Exception as e:
        print(f"❌ Failed to save data: {e}")


# =========================
# Main
# =========================
def main():
    session = create_session()

    try:
        station_list = fetch_station_list(session)
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching station list: {e}")
        return
    except Exception as e:
        print(f"❌ Unexpected error while fetching station list: {e}")
        return

    if not station_list:
        print("⚠️ No stations found. Exiting.")
        return

    rainfall_data = scrape_rainfall_data(session, station_list)
    process_and_save_data(rainfall_data, CSV_FILE)


if __name__ == "__main__":
    main()
