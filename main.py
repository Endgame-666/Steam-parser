import asyncio
from aiohttp import ClientSession
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import json
import aiosqlite

BASE_URL = "https://store.steampowered.com/search/"
SEARCH_TERMS = ["souls_like", "action", "strategy"]
MAX_PAGES = 2
DELAY = 1

class SQLiteDatabase:
    """Создает базу данных results.db"""
    def __init__(self, db_name="results.db"):
        self.db_name = db_name

    async def __aenter__(self):
        """Создает таблицу games"""
        self.connection = await aiosqlite.connect(self.db_name)
        await self.connection.execute(''' 
            CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                price TEXT,
                reviews TEXT,
                developer TEXT,
                genres TEXT,
                release_date TEXT
            )
        ''')
        await self.connection.commit()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.connection.close()

    async def insert_game(self, game_data):
        """Вставляет данные об игре в таблицу"""
        await self.connection.execute('''
            INSERT INTO games (title, price, reviews, developer, genres, release_date)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            game_data["title"],
            game_data["price"],
            game_data["reviews"],
            game_data["developer"],
            ", ".join(game_data["genres"]),
            game_data["release_date"]
        ))
        await self.connection.commit()

    async def export_to_json(self, json_file="results.json"):
        """Экспортирует данные из базы в JSON файл"""
        cursor = await self.connection.execute('SELECT title, price, reviews, developer, genres, release_date FROM games')
        rows = await cursor.fetchall()
        games = [
            {
                "title": row[0],
                "price": row[1],
                "reviews": row[2],
                "developer": row[3],
                "genres": row[4].split(", "),
                "release_date": row[5]
            }
            for row in rows
        ]

        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(games, f, indent=4, ensure_ascii=False)

        print(f"Данные успешно экспортированы в файл {json_file}, мой Повелитель!")



def build_url(query, page):
    """Делает url для поиска в steam"""
    query_params = {
        "term": query,
        "page": page,
        "ignore_preferences": 1
    }
    return f"{BASE_URL}?{urlencode(query_params)}"


def parse_page(html, term):
    """Парсит страницу steam"""
    soup = BeautifulSoup(html, 'html.parser')
    games = []
    rows = soup.select('#search_resultsRows > a')

    for row in rows:
        title = row.select_one('.title').get_text(strip=True) if row.select_one('.title') else "Unknown"
        release_date = row.select_one('.search_released').get_text(strip=True) if row.select_one(
            '.search_released') else "2024"
        price = row.select_one('.discount_final_price').get_text(strip=True) if row.select_one(
            '.discount_final_price') else "Free"
        reviews = row.select_one('.search_review_summary')
        reviews = reviews.get('data-tooltip-html', '').split('<br>')[0] if reviews else "No reviews"
        game_url = row['href'] if 'href' in row.attrs else None

        games.append({
            "title": title,
            "price": price,
            "reviews": reviews,
            "developer": f"Bundle of {term} games",
            "genres": [term],
            "release_date": release_date,
            "url": game_url
        })

    return games


async def parse_game_details(session, game_url, term):
    """Переходит на страницу каждой игры и забирает название автора и жанры (если не бандл с играми)"""
    async with session.get(game_url) as response:
        html = await response.text()
        soup = BeautifulSoup(html, 'html.parser')

        age_verification_form = soup.select_one('select[name="ageYear"]')
        if age_verification_form:
            payload = {
                "ageYear": "2000",
                "ageMonth": "1",
                "ageDay": "1",
            }
            async with session.post(game_url, data=payload) as post_response:
                html = await post_response.text()
                soup = BeautifulSoup(html, 'html.parser')

            product_page_button = soup.select_one("#view_product_page_btn")
            if product_page_button:
                product_page_url = product_page_button["href"]
                if not product_page_url.startswith('http'):
                    product_page_url = "https://store.steampowered.com" + product_page_url
                async with session.get(product_page_url) as product_page_response:
                    html = await product_page_response.text()
                    soup = BeautifulSoup(html, 'html.parser')
        else:
            soup = BeautifulSoup(html, 'html.parser')

        genre_tags = soup.select('#genresAndManufacturer span a')
        genres = [genre.get_text(strip=True) for genre in genre_tags]
        genres.append(term)
        developer = f"Bundle of {term} games"
        developer_tag = soup.select_one('.dev_row b + a')
        if developer_tag:
            developer = developer_tag.get_text(strip=True)

        return genres, developer


async def scrape_page(session, url, term):
    """Основной скрейпинг"""
    async with session.get(url) as response:
        html = await response.text()
        return parse_page(html, term)


async def scrape_game_details(session, game, term):
    """Уточняет детали игры"""
    if game['url']:
        game_url = "https://store.steampowered.com" + game['url'] if not game['url'].startswith('http') else game['url']
        genres, developer = await parse_game_details(session, game_url, term)
        game['genres'] = genres
        game['developer'] = developer
    return game


async def main():
    async with ClientSession(headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}) as session:
        async with SQLiteDatabase() as db:
            for term in SEARCH_TERMS:
                page = 1
                while page <= MAX_PAGES:
                    url = build_url(term, page)
                    games = await scrape_page(session, url, term)

                    if not games:  # Если нет игр на странице то прекращаем
                        break

                    for game in games:
                        game = await scrape_game_details(session, game, term)
                        await db.insert_game(game)

                    page += 1
                    await asyncio.sleep(DELAY)



if __name__ == "__main__":
    asyncio.run(main())
    # Экспорт результатов в JSON
    async def export_results():
        async with SQLiteDatabase() as db:
            await db.export_to_json()

    asyncio.run(export_results())
    print("Работа выполнена, мой Повелитель!")

