import os
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import zipfile
from io import BytesIO
from openpyxl import Workbook

BASE_URL = "https://www.scrapethissite.com/pages/forms/?page={}"

async def fetch_html(session, url):
    async with session.get(url) as response:
        return await response.text()

async def scrape_data():
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_html(session, BASE_URL.format(i)) for i in range(1, 25)]
        pages = await asyncio.gather(*tasks)
        return pages

def parse_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    rows = []
    for row in soup.select('tr.team'):
        year_cell = row.select_one('td[data-stat="year"]')
        team_name_cell = row.select_one('td[data-stat="team_name"]')
        wins_cell = row.select_one('td[data-stat="wins"]')
        losses_cell = row.select_one('td[data-stat="losses"]')
        
        if not (year_cell and team_name_cell and wins_cell and losses_cell):
            print("Warning: Missing data in row:", row)
            continue
        
        year = year_cell.text.strip()
        team_name = team_name_cell.text.strip()
        wins = int(wins_cell.text.strip())
        losses = int(losses_cell.text.strip())
        rows.append((year, team_name, wins, losses))
    
    return rows

def summarize_stats(data):
    summary = {}
    for year, team, wins, _ in data:
        if year not in summary:
            summary[year] = {"winner": (team, wins), "loser": (team, wins)}
        else:
            if wins > summary[year]["winner"][1]:
                summary[year]["winner"] = (team, wins)
            if wins < summary[year]["loser"][1]:
                summary[year]["loser"] = (team, wins)
    
    return [(year, s['winner'][0], s['winner'][1], s['loser'][0], s['loser'][1]) for year, s in summary.items()]

async def main():
    # Step 1: Scrape data
    pages = await scrape_data()
    
    # Step 2: Parse the data
    all_rows = []
    for page in pages:
        all_rows.extend(parse_html(page))
    
    # Step 3: Create the ZIP file with HTML content
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zf:
        for i, page in enumerate(pages, 1):
            zf.writestr(f'{i}.html', page)
    
    # Save the ZIP file
    with open("hockey_pages.zip", "wb") as f:
        f.write(zip_buffer.getvalue())
    
    # Step 4: Create the Excel file
    wb = Workbook()
    
    # Sheet 1: NHL Stats 1990-2011
    ws1 = wb.active
    ws1.title = "NHL Stats 1990-2011"
    ws1.append(["Year", "Team", "Wins", "Losses"])
    for row in all_rows:
        ws1.append(row)
    
    # Sheet 2: Winner and Loser per Year
    ws2 = wb.create_sheet(title="Winner and Loser per Year")
    ws2.append(["Year", "Winner", "Winner Num. of Wins", "Loser", "Loser Num. of Wins"])
    summary_data = summarize_stats(all_rows)
    for row in summary_data:
        ws2.append(row)
    
    # Save the Excel file
    wb.save("hockey_stats.xlsx")

# Run the main function
asyncio.run(main())
