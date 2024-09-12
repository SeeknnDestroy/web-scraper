import aiohttp
import asyncio
import pandas as pd
from bs4 import BeautifulSoup
import aiofiles
import urllib.parse
import time
import random
import logging

# Configure logging
logging.basicConfig(filename='scraper.log', level=logging.INFO)

# Load the initial dataset
input_file = 'horses.csv'
output_file = 'horse_details_new.csv'

# Read the initial dataset
df_initial = pd.read_csv(input_file)

# Define a pool of user-agents and cookies
user_agents = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.48",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:87.0) Gecko/20100101 Firefox/87.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:86.0) Gecko/20100101 Firefox/86.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:85.0) Gecko/20100101 Firefox/85.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:84.0) Gecko/20100101 Firefox/84.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:83.0) Gecko/20100101 Firefox/83.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:82.0) Gecko/20100101 Firefox/82.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:81.0) Gecko/20100101 Firefox/81.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:80.0) Gecko/20100101 Firefox/80.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.0.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.0.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:79.0) Gecko/20100101 Firefox/79.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.0.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.75 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/10.1.1 Safari/603.3.8",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/10.1 Safari/603.3.8",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/10.0.3 Safari/603.3.8",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/10.0.2 Safari/603.3.8",
]

cookies_list = [
    "_ga=GA1.1.2073242724.1724175667; _ga_MGLMKZ4BFD=GS1.1.1724435017.11.1.1724435114.60.0.0; _gid=GA1.2.1715885817.1724265937",
    "_ga=GA1.1.1575847294.1724466728; _ga_MGLMKZ4BFD=GS1.1.1724466728.2.1.1724466753.60.0.0; _gid=GA1.2.1715885817.1724265937",
    "_ga=GA1.1.1234567890.1724567890; _ga_MGLMKZ4BFD=GS1.1.1724567890.3.1.1724567891.60.0.0; _gid=GA1.2.9876543210.1724567890",
    "_ga=GA1.1.0987654321.1724667890; _ga_MGLMKZ4BFD=GS1.1.1724667890.4.1.1724667891.60.0.0; _gid=GA1.2.1234567890.1724667890",
    "_ga=GA1.1.1122334455.1724767890; _ga_MGLMKZ4BFD=GS1.1.1724767890.5.1.1724767891.60.0.0; _gid=GA1.2.5544332211.1724767890",
    "_ga=GA1.1.2233445566.1724867890; _ga_MGLMKZ4BFD=GS1.1.1724867890.6.1.1724867891.60.0.0; _gid=GA1.2.6655443322.1724867890",
    "_ga=GA1.1.3344556677.1724967890; _ga_MGLMKZ4BFD=GS1.1.1724967890.7.1.1724967891.60.0.0; _gid=GA1.2.7766554433.1724967890",
    "_ga=GA1.1.4455667788.1725067890; _ga_MGLMKZ4BFD=GS1.1.1725067890.8.1.1725067891.60.0.0; _gid=GA1.2.8877665544.1725067890",
    "_ga=GA1.1.5566778899.1725167890; _ga_MGLMKZ4BFD=GS1.1.1725167890.9.1.1725167891.60.0.0; _gid=GA1.2.9988776655.1725167890",
    "_ga=GA1.1.6677889900.1725267890; _ga_MGLMKZ4BFD=GS1.1.1725267890.10.1.1725267891.60.0.0; _gid=GA1.2.1100998877.1725267890",
]


# Asynchronous function to fetch and parse the horse details
async def fetch_race_details(row, session, semaphore, queue, retries=5, retry_delay=5):
    details_url = row['details']
    encoded_url = urllib.parse.quote(details_url, safe=':/&=?')

    async with semaphore:
        for attempt in range(retries):
            try:
                headers = {
                    "User-Agent": random.choice(user_agents),
                    "Accept-Language": "en-US,en;q=0.9",
                }
                # Add a random delay to mimic human behavior
                await asyncio.sleep(random.uniform(1, 3))

                async with session.get(encoded_url, headers=headers) as response:
                    if response.status == 404:
                        logging.warning(f"404 error at URL: {encoded_url}. Attempt {attempt + 1} of {retries}")
                        if attempt < retries - 1:
                            await asyncio.sleep(retry_delay)
                            continue
                        else:
                            logging.error(f"404 error at URL: {encoded_url}. Skipping after {retries} attempts.")
                            return
                    elif response.status in [429, 503]:
                        logging.warning(f"Server busy at URL: {encoded_url}. Attempt {attempt + 1} of {retries}")
                        retry_delay += 5
                        await asyncio.sleep(retry_delay)
                        continue

                    response.raise_for_status()
                    html_content = await response.text()
                    soup = BeautifulSoup(html_content, 'html.parser')

                    # Extract horse details
                    horse_details_div = soup.find('div', class_='grid_8 alpha omega kunye')
                    dogum_tarihi = None
                    handikap_puani = None

                    if horse_details_div:
                        dob_span = horse_details_div.find('span', string='Doğ. Trh')
                        handicap_span = horse_details_div.find('span', string='Handikap P.')

                        dogum_tarihi = dob_span.find_next('span', class_='value').get_text(strip=True) if dob_span else None
                        handikap_puani = handicap_span.find_next('span', class_='value').get_text(strip=True) if handicap_span else None

                    # Extract race data
                    race_data = []
                    table_rows = soup.select('tbody.ajaxtbody tr')
                    for race_row in table_rows:
                        columns = race_row.find_all('td')
                        if len(columns) > 13:
                            tarih = columns[0].get_text(strip=True)
                            sehir = columns[1].get_text(strip=True)
                            mesafe = columns[2].get_text(strip=True)
                            pist = columns[3].get_text(strip=True)
                            derece = columns[5].get_text(strip=True)
                            siklet = columns[6].get_text(strip=True)
                            takı = columns[7].get_text(strip=True)
                            jokey = columns[8].get_text(strip=True)
                            kosu_cinsi = columns[13].get_text(strip=True)

                            race_data.append({
                                "At İsmi": row["At İsmi"],
                                "Irk": row["Irk"],
                                "Cinsiyet": row["Cinsiyet"],
                                "Yaş": row["Yaş"],
                                "Doğ. Trh": dogum_tarihi,
                                "Handikap P.": handikap_puani,
                                "Tarih": tarih,
                                "Şehir": sehir,
                                "Mesafe": mesafe,
                                "Pist": pist,
                                "Derece": derece,
                                "Siklet": siklet,
                                "Takı": takı,
                                "Jokey": jokey,
                                "Kcins": kosu_cinsi,
                                "details": details_url
                            })

                    # If no race data is found, still return horse details
                    if not race_data:
                        race_data = [{
                            "At İsmi": row["At İsmi"],
                            "Irk": row["Irk"],
                            "Cinsiyet": row["Cinsiyet"],
                            "Yaş": row["Yaş"],
                            "Doğ. Trh": dogum_tarihi,
                            "Handikap P.": handikap_puani,
                            "Tarih": None,
                            "Şehir": None,
                            "Mesafe": None,
                            "Pist": None,
                            "Derece": None,
                            "Siklet": None,
                            "Takı": None,
                            "Jokey": None,
                            "Kcins": None,
                            "details": details_url
                        }]

                    await queue.put(race_data)
                    return

            except Exception as e:
                logging.error(f"Error at URL {encoded_url}: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(retry_delay)
                else:
                    # Log and skip after retries
                    logging.error(f"Failed to fetch {encoded_url} after {retries} attempts.")
                    return

async def save_results(queue):
    async with aiofiles.open(output_file, 'a', encoding='utf-8-sig') as f:
        while True:
            result = await queue.get()
            if result is None:
                break
            df_enriched = pd.DataFrame(result)
            await f.write(df_enriched.to_csv(header=False, index=False, encoding='utf-8-sig'))
            queue.task_done()

async def main():
    # Check if output file exists and initialize headers if it doesn't
    try:
        with open(output_file, 'r', encoding='utf-8-sig') as f:
            pass
    except FileNotFoundError:
        with open(output_file, 'w', encoding='utf-8-sig') as f:
            df = pd.DataFrame(columns=[
                "At İsmi", "Irk", "Cinsiyet", "Yaş", "Doğ. Trh", "Handikap P.",
                "Tarih", "Şehir", "Mesafe", "Pist", "Derece", "Siklet",
                "Takı", "Jokey", "Kcins", "details"
            ])
            df.to_csv(f, index=False, encoding='utf-8-sig')

    queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(500)

    async with aiohttp.ClientSession() as session:
        writer_task = asyncio.create_task(save_results(queue))

        batch_size = 250
        retries = 15
        retry_delay = 3

        for i in range(0, len(df_initial), batch_size):
            batch_df = df_initial.iloc[i:i+batch_size]
            tasks = [
                asyncio.create_task(
                    fetch_race_details(row, session, semaphore, queue, retries, retry_delay)
                )
                for _, row in batch_df.iterrows()
            ]
            await asyncio.gather(*tasks)
            await asyncio.sleep(1)  # Delay between batches

        await queue.put(None)
        await writer_task

# Run the asynchronous main function
if __name__ == "__main__":
    asyncio.run(main())