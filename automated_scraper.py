import aiohttp
import asyncio
import pandas as pd
from bs4 import BeautifulSoup
import aiofiles

# Base URL identified from the network request
url = "https://www.tjk.org/TR/YarisSever/Query/DataRows/Atlar"
details_base_url = "https://www.tjk.org/TR/YarisSever/Query/"

# Headers to mimic the browser request
headers = {
    "authority": "www.tjk.org",
    "method": "GET",
    "scheme": "https",
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "cookie": "_gid=GA1.2.1715885817.1724265937; _ga=GA1.1.2073242724.1724175667; _ga_MGLMKZ4BFD=GS1.1.1724348880.5.1.1724352490.60.0.0",
    "priority": "u=1, i",
    "referer": "https://www.tjk.org/TR/YarisSever/Query/Page/Atlar?QueryParameter_OLDUFLG=off",
    "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest",
}

# Output file
output_file = 'scraped_data_async.csv'

# Initialize the CSV file (with headers) if it doesn't exist
with open(output_file, 'w', encoding='utf-8-sig') as f:
    df = pd.DataFrame(columns=["At İsmi", "Irk", "Cinsiyet", "Yaş", "details"])
    df.to_csv(f, index=False, encoding='utf-8-sig')

async def fetch_page(session, page, max_retries=5, retry_delay=5):
    params = {
        "QueryParameter_OLDUFLG": "off",
        "PageNumber": str(page),
        "Sort": "AtIsmi",
    }
    retries = 0
    while retries < max_retries:
        try:
            async with session.get(url, headers=headers, params=params) as response:
                response.raise_for_status()  # Check for HTTP errors
                response = await response.text()
                print(f"Fetched page {page}")
                return response
        except aiohttp.ClientResponseError as e:
            if retries < max_retries:
                print(f"Error fetching page {page}: {e}. Retrying in {retry_delay} seconds...")
                retries += 1
                await asyncio.sleep(retry_delay)
            else:
                print(f"Failed to fetch page {page} after {max_retries} attempts.")
                raise

def parse_data(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    table_rows = soup.find_all('tr')  # Adjust based on the actual structure

    data = []
    for row in table_rows:
        columns = row.find_all('td')
        if len(columns) >= 4:  # Ensure there are enough columns
            horse_name = columns[0].get_text(strip=True)
            breed = columns[1].get_text(strip=True)
            gender = columns[2].get_text(strip=True)
            age = columns[3].get_text(strip=True)

            # Extract the "details" link
            details_link = columns[0].find('a')['href'] if columns[0].find('a') else None
            if details_link and 'Query/' in details_link:
                details_link = details_link.split('Query/')[-1]
                details_link = details_base_url + details_link

            data.append({
                "At İsmi": horse_name,
                "Irk": breed,
                "Cinsiyet": gender,
                "Yaş": age,
                "details": details_link
            })
    return data

async def save_data(data):
    df = pd.DataFrame(data)
    async with aiofiles.open(output_file, 'a', encoding='utf-8-sig') as f:
        await f.write(df.to_csv(header=False, index=False, encoding='utf-8-sig'))

async def scrape_all_pages(start_page=0, max_concurrent_requests=50, max_retries=10, retry_delay=2):
    async with aiohttp.ClientSession() as session:
        page = start_page
        while True:
            tasks = [fetch_page(session, page + i, max_retries, retry_delay) for i in range(max_concurrent_requests)]
            try:
                results = await asyncio.gather(*tasks)
            except aiohttp.ClientResponseError as e:
                print(f"Terminating scraping due to persistent errors: {e}")
                break

            all_data = []
            for result in results:
                data = parse_data(result)
                all_data.extend(data)
            await save_data(all_data)
            print(f"Scraped and saved data for pages {page} to {page + max_concurrent_requests - 1}")
            page += max_concurrent_requests

# Run the async scraper
asyncio.run(scrape_all_pages())
