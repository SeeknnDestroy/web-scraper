import aiohttp
import asyncio
import pandas as pd
from bs4 import BeautifulSoup
import aiofiles
import urllib.parse
import time
import random

# Load the initial dataset
input_file = 'scraped_data_async.csv'
output_file = 'enriched_data.csv'

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

# Define the headers to mimic a real browser request
headers = {
    "cookie": "_ga=GA1.1.2073242724.1724175667; _ga_MGLMKZ4BFD=GS1.1.1724435017.11.1.1724435114.60.0.0; _gid=GA1.2.1715885817.1724265937",
    "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
}

# Asynchronous function to fetch and parse the race data for each horse with retries and session reset
async def fetch_race_details(row, retries=10, retry_delay=4):
    details_url = row['details']
    
    # Ensure the URL is properly encoded
    encoded_url = urllib.parse.quote(details_url, safe=':/&=?')

    for attempt in range(retries):
        try:
            headers = {
                "cookie": random.choice(cookies_list),
                "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
                "sec-ch-ua-platform": '"Linux"',
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "user-agent": random.choice(user_agents),
            }
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(encoded_url) as response:
                    if response.status == 404:
                        print(f"404 error at URL: {encoded_url}. Attempt {attempt + 1} of {retries}")
                        if attempt < retries - 1:
                            await asyncio.sleep(retry_delay)  # wait before retrying
                            continue
                        else:
                            raise aiohttp.ClientResponseError(response.request_info, (), status=404)
                    response.raise_for_status()
                    html_content = await response.text()
                    soup = BeautifulSoup(html_content, 'html.parser')

                    # Check if race data is present
                    no_data_div = soup.find(string="Aradığınız kriterlere uygun veri bulunmamaktadır")
                    if no_data_div:
                        # No race data available
                        return [{
                            "At İsmi": row["At İsmi"],
                            "Irk": row["Irk"],
                            "Cinsiyet": row["Cinsiyet"],
                            "Yaş": row["Yaş"],
                            "Şehir": None,
                            "Mesafe": None,
                            "Derece": None,
                            "details": details_url
                        }]
                    
                    # Extract race data
                    race_data = []
                    table_rows = soup.select('tbody.ajaxtbody tr')
                    for race_row in table_rows:
                        columns = race_row.find_all('td')
                        if len(columns) > 5:  # Check that there are enough columns
                            tarih = columns[0].get_text(strip=True) if len(columns) > 0 else None
                            sehir = columns[1].get_text(strip=True) if len(columns) > 1 else None
                            mesafe = columns[2].get_text(strip=True) if len(columns) > 2 else None
                            derece = columns[5].get_text(strip=True) if len(columns) > 5 else None

                            race_data.append({
                                "At İsmi": row["At İsmi"],
                                "Irk": row["Irk"],
                                "Cinsiyet": row["Cinsiyet"],
                                "Yaş": row["Yaş"],
                                "Şehir": sehir,
                                "Mesafe": mesafe,
                                "Derece": derece,
                                "details": details_url
                            })

                    return race_data
        
        except (aiohttp.ClientResponseError, aiohttp.ClientConnectionError) as e:
            print(f"Error {e} at URL: {encoded_url}. Attempt {attempt + 1} of {retries}")
            if attempt < retries - 1:
                await asyncio.sleep(retry_delay)  # wait before retrying
            else:
                # If it fails after all retries, we record the failure with None values
                return [{
                    "At İsmi": row["At İsmi"],
                    "Irk": row["Irk"],
                    "Cinsiyet": row["Cinsiyet"],
                    "Yaş": row["Yaş"],
                    "Şehir": None,
                    "Mesafe": None,
                    "Derece": "NONE",
                    "details": details_url
                }]

# Asynchronous function to scrape all the race details
async def scrape_all_race_details(df, max_concurrent_requests=50):
    tasks = []
    for index, row in df.iterrows():
        task = asyncio.ensure_future(fetch_race_details(row))
        tasks.append(task)

        # Control the rate of requests
        if len(tasks) >= max_concurrent_requests:
            await asyncio.sleep(1)  # Introduce a small delay between batches
            results = await asyncio.gather(*tasks)
            tasks = []
            await save_results(results)
            print(f"Saved results for batch {index + 1}")

    if tasks:
        results = await asyncio.gather(*tasks)
        await save_results(results)

# Function to save results incrementally
async def save_results(results):
    flat_results = [item for sublist in results for item in sublist]
    df_enriched = pd.DataFrame(flat_results)
    async with aiofiles.open(output_file, 'a', encoding='utf-8-sig') as f:
        await f.write(df_enriched.to_csv(header=False, index=False, encoding='utf-8-sig'))

# Main function to run the scraper and save the data
async def main():
    # Initialize the output file with headers if it doesn't exist
    if not pd.io.common.file_exists(output_file):
        with open(output_file, 'w', encoding='utf-8-sig') as f:
            df = pd.DataFrame(columns=["At İsmi", "Irk", "Cinsiyet", "Yaş", "Şehir", "Mesafe", "Derece", "details"])
            df.to_csv(f, index=False, encoding='utf-8-sig')

    # Scrape race details
    await scrape_all_race_details(df_initial)

# Run the asynchronous main function
asyncio.run(main())
