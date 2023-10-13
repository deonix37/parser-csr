import asyncio
import re
from asyncio.exceptions import TimeoutError
from datetime import datetime

import aiosqlite
import json
from aiohttp import ClientError, ClientSession, CookieJar, TCPConnector
from aiohttp_retry import ExponentialRetry, RetryClient
from bs4 import BeautifulSoup

service_centers = {}
testimonials = {}
services = {}
service_categories = {}


async def main():
    await init()
    await parse_data()
    await insert_data()


async def init():
    async with aiosqlite.connect('database.sqlite3') as db:
        await create_schema(db)
        await set_service_centers_with_data(db)


async def set_service_centers_with_data(db):
    global sc_with_testimonials, sc_with_services

    async with db.execute(
        '''SELECT DISTINCT zoon_service_center_id FROM zoon_testimonials'''
    ) as cursor:
        sc_with_testimonials = {row[0] for row in await cursor.fetchall()}

    async with db.execute(
        '''SELECT DISTINCT zoon_service_center_id FROM zoon_services'''
    ) as cursor:
        sc_with_services = {row[0] for row in await cursor.fetchall()}


async def parse_data():
    global session

    session = ClientSession(
        'https://spb.zoon.ru',
        headers={'User-Agent': 'Mozilla/5.0'},
        cookie_jar=CookieJar(),
        connector=TCPConnector(limit=3)
    )
    retry_options = ExponentialRetry(
        attempts=50,
        statuses=(212, 302, 512),
        exceptions=(ClientError, TimeoutError),
    )

    async with RetryClient(
        session,
        retry_options=retry_options,
        raise_for_status=True
    ) as session:
        await set_api_token()
        await parse_main_page()


async def set_api_token():
    global api_token

    async with session.get('/repair/') as resp:
        text = await resp.text()

    api_token = re.search(r"'apiToken', '(.+?)'", text)[1]
    session_id = re.search(r'"session_id":"(.+?)"', text)[1]

    session._client.cookie_jar.update_cookies({'sid': session_id})


async def parse_main_page():
    await asyncio.gather(*(
        parse_service_centers_page(page)
        for page in range(1, 20)
    ))


async def parse_service_centers_page(page):
    data = await apiPost(
        '/repair/',
        params={
            'action': 'listJson',
            'type': 'service',
        },
        data={
            'page': page,
            'need[]': 'items',
            'search_query_form': 1,
            'bounds[]': [
                30.21261142152425,
                59.76708012960721,
                30.50443576234457,
                60.07457381811325,
            ],
        }
    )

    if not data['success']:
        return

    html = BeautifulSoup(data['html'], 'html.parser')

    await asyncio.gather(*(
        parse_service_center(sc_card)
        for sc_card in html.select('.minicard-item')
    ))


async def parse_service_center(sc_card):
    sc, added = add_service_center(sc_card)

    if not added:
        return

    sc_html = await get_sc_html(sc)

    await parse_service_categories(sc_html)

    initial_testimonials = sc_html.select('.js-comment-list li')
    initial_services = sc_html.select('.price-dish')

    if sc['id'] not in sc_with_testimonials and initial_testimonials:
        await parse_testimonials(sc)

    if sc['id'] not in sc_with_services and initial_services:
        await parse_services(sc)


async def get_sc_html(sc):
    async with session.get(sc['link']) as resp:
        return BeautifulSoup(await resp.text(), 'html.parser')


async def parse_testimonials(sc):
    data = await apiPost('/js.php', data={
        'area': 'service',
        'action': 'CommentList',
        'owner[]': ['organization', 'prof'],
        'organization': sc['id'],
        'sort': 'default',
        'limit': 100,
        'skip': 0,
    })

    html = BeautifulSoup(data['list'], 'html.parser')

    for testimonial_tag in html.select(':not(.subcomments) > .js-comment'):
        try:
            add_testimonial(sc, testimonial_tag)
        except AttributeError:
            pass


async def parse_service_categories(sc_html):
    if (data_match := re.search(r'initialCategories: (.+),\n', str(sc_html))):
        categories = json.loads(data_match[1])

        for category in categories.values():
            children = category['children'] or {}

            for category in [category, *children.values()]:
                service_categories[category['id']] = {
                    'id': category['id'],
                    'title': category['title'],
                }

    return await asyncio.sleep(1)


async def parse_services(sc):
    data = await apiPost('/json-rpc/v1/', json=[{
        'id': 0,
        'jsonrpc': '2.0',
        'method': 'MenuDish.List',
        'params': {
            'limit': 1000,
            'offset': 0,
            'owner_id': sc['id'],
            'owner_type': 'organization',
        },
    }])

    if data is None:
        return await asyncio.sleep(1)

    for service_data in data[0]['result']['items']:
        add_service(sc, service_data)


def add_service_center(sc_card):
    sc_id = sc_card['data-id']
    title = sc_card.select_one('.title-link').get_text(strip=True)
    link = sc_card.select_one('.title-link')['href']

    if sc_id in service_centers:
        return service_centers[sc_id], False

    service_centers[sc_id] = {
        'id': sc_id,
        'title': title,
        'link': link.removeprefix('https://spb.zoon.ru'),
        'slug': link.removeprefix('https://spb.zoon.ru'),
    }

    return service_centers[sc_id], True


def add_testimonial(sc, testimonial_tag):
    testimonial_id = testimonial_tag['data-id']

    if (author := testimonial_tag.select_one('.name')):
        author = author.get_text(strip=True)

    if (rating := testimonial_tag.select_one('.stars-rating-text')):
        rating = int(rating.get_text(strip=True)[0])

    if (advantages := testimonial_tag.find(
        class_='comment-text-subtitle',
        string=re.compile('Достоинства')
    )):
        advantages = advantages.find_next(
            class_='js-comment-content'
        ).get_text()

    if (disadvantages := testimonial_tag.find(
        class_='comment-text-subtitle',
        string=re.compile('Недостатки')
    )):
        disadvantages = disadvantages.find_next(
            class_='js-comment-content'
        ).get_text()

    if (comment := testimonial_tag.find(
        class_='comment-text-subtitle',
        string=re.compile('Комментарий')
    )):
        comment = comment.find_next(
            class_='js-comment-content'
        ).get_text()

    if (published_at := testimonial_tag.select_one(
        '[itemprop="datePublished"]'
    )):
        published_at = datetime.fromisoformat(published_at['content'])
        published_at = published_at.strftime('%Y-%m-%d %H:%M:%S')

    testimonials[testimonial_id] = {
        'id': testimonial_id,
        'zoon_service_center_id': sc['id'],
        'author': author,
        'rating': rating,
        'advantages': advantages,
        'disadvantages': disadvantages,
        'comment': comment,
        'published_at': published_at,
    }


def add_service(sc, service_data):
    service_id = service_data['id']

    services[service_id] = {
        'id': service_id,
        'zoon_service_center_id': sc['id'],
        'zoon_service_category_id': service_data['parent_id'],
        'title': service_data['title'],
        'price': service_data['cost'],
    }


async def apiPost(url, **kwargs):
    headers = kwargs.pop('headers', {})
    headers['Authorization'] = f'Bearer {api_token}'

    async with session.post(
        url,
        headers=headers,
        allow_redirects=False,
        **kwargs
    ) as resp:
        return await resp.json(content_type=None)


async def insert_data():
    async with aiosqlite.connect('database.sqlite3') as db:
        db.row_factory = aiosqlite.Row

        await insert_service_centers(db)
        await insert_service_categories(db)
        await db.commit()

        await insert_testimonials(db)
        await insert_services(db)
        await db.commit()


async def create_schema(db):
    await db.execute(
        '''CREATE TABLE IF NOT EXISTS zoon_service_centers (
            id VARCHAR(255) PRIMARY KEY,
            slug VARCHAR(255) NOT NULL UNIQUE,
            title VARCHAR(255) NOT NULL
        )'''
    )
    await db.execute(
        '''CREATE TABLE IF NOT EXISTS zoon_service_categories (
            id VARCHAR(255) PRIMARY KEY,
            title VARCHAR(255) NOT NULL
        )'''
    )
    await db.execute(
        '''CREATE TABLE IF NOT EXISTS zoon_testimonials (
            id VARCHAR(255) PRIMARY KEY,
            zoon_service_center_id VARCHAR(255) NOT NULL,
            author VARCHAR(255) NOT NULL,
            rating INTEGER NOT NULL,
            advantages TEXT,
            disadvantages TEXT,
            comment TEXT NOT NULL,
            published_at DATETIME NOT NULL
        )'''
    )
    await db.execute(
        '''CREATE TABLE IF NOT EXISTS zoon_services (
            id VARCHAR(255) PRIMARY KEY,
            zoon_service_center_id VARCHAR(255) NOT NULL,
            zoon_service_category_id VARCHAR(255) NOT NULL,
            title VARCHAR(255) NOT NULL,
            price VARCHAR(255) NOT NULL
        )'''
    )


async def insert_service_categories(db):
    await db.executemany(
        get_insert_query('zoon_service_categories', ('id', 'title')),
        service_categories.values()
    )


async def insert_service_centers(db):
    await db.executemany(
        get_insert_query('zoon_service_centers', ('id', 'slug', 'title')),
        service_centers.values()
    )


async def insert_testimonials(db):
    await db.executemany(
        get_insert_query('zoon_testimonials', (
            'id',
            'zoon_service_center_id',
            'author',
            'rating',
            'advantages',
            'disadvantages',
            'comment',
            'published_at',
        )),
        testimonials.values()
    )


async def insert_services(db):
    await db.executemany(
        get_insert_query('zoon_services', (
            'id',
            'zoon_service_center_id',
            'zoon_service_category_id',
            'title',
            'price',
        )),
        services.values()
    )


def get_insert_query(table, fields):
    return f'''INSERT OR IGNORE INTO {table} (
        {', '.join(fields)}
    ) VALUES (
        {', '.join([f':{field}' for field in fields])}
    )'''


if __name__ == '__main__':
    asyncio.run(main())
