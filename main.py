import asyncio
import re
from asyncio.exceptions import TimeoutError
from collections import defaultdict

import aiosqlite
from aiohttp import ClientError, ClientSession, TCPConnector
from aiohttp_retry import ExponentialRetry, RetryClient
from aiopath import AsyncPath
from bs4 import BeautifulSoup
from slugify import slugify

advantages = {}
brands = {}
devices = {}
features = {}
metros = {}
service_centers = {}


async def main():
    await parse_data()
    await insert_data()


async def parse_data():
    global session

    session = ClientSession(
        'https://centr-servisov.ru',
        headers={'User-Agent': 'Mozilla/5.0'},
        connector=TCPConnector(limit=7)
    )
    retry_options = ExponentialRetry(
        attempts=40,
        exceptions=(ClientError, TimeoutError),
    )

    async with RetryClient(session, retry_options=retry_options) as session:
        await parse_main_page()


async def parse_main_page():
    async with session.get('/spb.htm') as resp:
        html = BeautifulSoup(await resp.text(), 'html.parser')

    devices_block, brands_block = html.select('.job-stats')

    await asyncio.gather(*(
        parse_category(device_tag, 'devices')
        for device_tag in devices_block.select('a')
    ))

    for brand_tag in brands_block.select('a'):
        add_popular_brand(brand_tag)


async def parse_category(category_tag, category_type):
    category = add_category(category_type, category_tag)

    async with session.get(category_tag['href']) as resp:
        html = BeautifulSoup(await resp.text(), 'html.parser')

    if category['type'] == 'devices':
        await asyncio.gather(*(
            parse_device_brand(category, brand_tag)
            for brand_tag in html.select('.single-job .job-stats a[href]')
        ))

        for service_tag in html.select('.table_price tr'):
            add_service(category, service_tag)

    if category['thumbnail_url']:
        await download_file(category['thumbnail_url'])

    page_links = (
        page_tag['href']
        if page_tag['href'].startswith('/')
        else f'{category_tag["href"]}{page_tag["href"]}'
        for page_tag in html.select('.pagination li a')[1:-1]
    )

    await asyncio.gather(*(
        parse_category_page(page_link, category)
        for page_link in page_links
    ))

    return category


async def parse_category_page(page_link, category):
    async with session.get(page_link) as resp:
        html = BeautifulSoup(await resp.text(), 'html.parser')

    sc_cards = ({
        'title': card.select_one('.namesc').get_text(strip=True),
        'link': card.select_one('a[href*="/sc_"]')['href'],
        'metros_tags': card.select('.services > .container .bliz span'),
        'hours_tags': card.select('.services > .container .time ul li'),
        'address_tag': getattr(card.select_one(
            '.fa-map-marker'
        ), 'parent', None),
    } for card in html.select('.contacty'))

    await asyncio.gather(*(
        parse_service_center(sc_card, category)
        for sc_card in sc_cards
    ))


async def parse_device_brand(device, brand_tag):
    brand = await parse_category(brand_tag, 'brands')
    device['brands'].add(brand['slug'])


async def parse_service_center(sc_card, category):
    sc, added = add_service_center(sc_card, category)

    if not added:
        return

    for metro_tag in sc_card['metros_tags']:
        add_metro(sc, metro_tag)

    for hour_tag in sc_card['hours_tags']:
        add_opening_hour(sc, hour_tag)

    if sc_card['address_tag']:
        sc['primary_address'] = (
            sc_card['address_tag']
                .get_text(strip=True)
                .removeprefix('Адрес: ')
        )

    async with session.get(sc_card['link']) as resp:
        html = BeautifulSoup(await resp.text(), 'html.parser')

    for location_tag in html.select('.address h4'):
        add_location(sc, location_tag, html.head)

    for feature_tag in html.select('.job-info .sidebar-tags a'):
        add_feature(sc, feature_tag)

    if (advantages_title := html.find('h3', string=' Преимущества сервиса')):
        for advantage_tag in advantages_title.parent.select('li'):
            add_advantage(sc, advantage_tag)

    if (title := html.select_one('.main h1.title')):
        sc['title'] = (
            title.get_text(strip=True).removeprefix('Сервисный центр ')
        )

    if (phone := html.select_one('.main h2.title a[href]')):
        sc['phone'] = phone['href'].removeprefix('tel:+')

    if (slogan := html.select_one('.main .description.light')):
        sc['slogan'] = slogan.get_text(strip=True)

    if (site_link := html.select_one('.address .btn-default[href]')):
        sc['site_url'] = re.search(r'url=(.+)%', site_link['href'])[1]

        if '//' not in sc['site_url']:
            sc['site_url'] = 'http://' + sc['site_url']

    if (description := html.select_one('.culture p')):
        sc['description'] = description.get_text(strip=True)

    if (logo_img := html.select_one('.culture img[src]')):
        sc['logo'] = await download_file(logo_img['src'])

    await parse_gallery(sc, html)


async def parse_gallery(sc, sc_html):
    sc['gallery'] = await asyncio.gather(*(
        download_file(img['src'])
        for img in sc_html.select('[data-fancybox="gallery"] img')
    ))


async def download_file(url):
    path = AsyncPath(f'./downloads{url}')

    if not await path.exists():
        await path.parent.mkdir(parents=True, exist_ok=True)

        async with session.get(url) as resp:
            await path.write_bytes(await resp.read())

    return f'{path.parent.name}/{path.name}'


def add_popular_brand(brand_tag):
    target_slug = slugify(get_category_title(brand_tag))

    for brand_slug, brand in brands.items():
        if brand_slug == target_slug:
            brand['is_popular'] = True


def add_category(category_type, category_tag):
    title = get_category_title(category_tag)

    category = {
        'title': title,
        'slug': slugify(title),
        'type': category_type,
        'thumbnail_url': None,
        'is_popular': False,
    }
    global_category = {
        'brands': brands,
        'devices': devices,
    }[category_type]

    if (thumbnail := category_tag.find('img')):
        if (thumbnail_url := thumbnail.get('src')):
            category['thumbnail_url'] = thumbnail_url
        elif (thumbnail_url := thumbnail.get('data-src')):
            category['thumbnail_url'] = thumbnail_url

    if category_type == 'devices':
        category.update({
            'brands': set(),
            'services': [],
        })

    if category['slug'] not in global_category:
        global_category[category['slug']] = category

    return category


def get_category_title(category_tag):
    if (image := category_tag.find('img')):
        if image['alt'].startswith('Логотип'):
            return image['alt'].removeprefix('Логотип ')

    return category_tag.get_text(strip=True)


def add_service_center(sc_card, category):
    slug = slugify(sc_card['title'])

    if slug in service_centers:
        sc = service_centers[slug]
        created = False
    else:
        sc = {
            'slug': slug,
            'advantages': set(),
            'brands': set(),
            'devices': set(),
            'features': set(),
            'metros': set(),
            'gallery': [],
            'locations': [],
            'opening_hours': [],
            'primary_address': None,
        }
        service_centers[slug] = sc
        created = True

    sc[category['type']].add(category['slug'])

    return sc, created


def add_service(category, service_tag):
    title_tag = service_tag.select_one('.col-price-1')
    price_tag = service_tag.select_one('.col-price-2')

    category['services'].append({
        'title': title_tag.get_text(strip=True),
        'price': price_tag.get_text(strip=True).removesuffix(' руб.'),
    })


def add_advantage(sc, advantage_tag):
    title = advantage_tag.get_text(strip=True)

    advantages[title] = {
        'title': title,
    }

    sc['advantages'].add(title)


def add_feature(sc, feature_tag):
    title = feature_tag.get_text(strip=True)

    features[title] = {
        'title': title,
    }

    sc['features'].add(title)


def add_metro(sc, metro_tag):
    title = metro_tag.get_text(strip=True).removeprefix('метро ')

    if title == 'Новокрестовская':
        title = 'Зенит'

    metros[title] = {
        'title': title,
    }

    sc['metros'].add(title)


def add_location(sc, location_tag, head_tag):
    metro, address = location_tag.get_text(strip=True).split('-', 1)

    location = {
        'coords': None,
        'metro': metro.removeprefix('метро '),
        'address': address,
        'is_primary': False,
    }

    if sc['primary_address']:
        location['is_primary'] = sc['primary_address'] in address

    coords_addresses = re.findall(
        r'Placemark\(\[(.+?)\].+?balloonContentBody: "(.+?)"',
        str(head_tag)
    )

    for coords, address in coords_addresses:
        if location['address'] in address:
            location['coords'] = coords

    sc['locations'].append(location)


def add_opening_hour(sc, hour_tag):
    weekdays_names = ('ПН', 'ВТ', 'СР', 'ЧТ', 'ПТ', 'СБ', 'ВС')

    text = hour_tag.get_text(strip=True)

    weekdays, times = text.rsplit(' - ', 1)
    weekdays, times = weekdays.split('-'), times.split()

    weekdays = [weekdays_names.index(day) + 1 for day in weekdays]

    if 'Круглосуточно' in times:
        times = ['00:00', '00:00']
    elif 'Выходной' in times:
        times = [None, None]
    else:
        times = [time for time in times if ':' in time]

    sc['opening_hours'].append({
        'weekday_from': weekdays[0],
        'weekday_to': weekdays[-1],
        'time_from': times[0],
        'time_to': times[-1],
    })


async def insert_data():
    async with aiosqlite.connect('database.sqlite3') as db:
        db.row_factory = aiosqlite.Row

        await create_schema(db)

        await insert_brands(db)
        await insert_devices(db)
        await insert_metros(db)
        await insert_advantages(db)
        await insert_features(db)
        await db.commit()

        await insert_service_centers(db)
        await db.commit()

        sc_ids_by_slugs = await get_items_ids_by_field(
            db,
            'service_centers',
            'slug'
        )
        metro_ids_by_titles = await get_items_ids_by_field(
            db,
            'metros',
            'title'
        )
        brand_ids_by_slugs = await get_items_ids_by_field(
            db,
            'brands',
            'slug'
        )
        device_ids_by_slugs = await get_items_ids_by_field(
            db,
            'devices',
            'slug'
        )
        advantage_ids_by_titles = await get_items_ids_by_field(
            db,
            'advantages',
            'title'
        )
        feature_ids_by_titles = await get_items_ids_by_field(
            db,
            'features',
            'title'
        )

        await insert_opening_hours(db, sc_ids_by_slugs)
        await insert_gallery_images(db, sc_ids_by_slugs)
        await insert_services(db, device_ids_by_slugs)
        await insert_locations(
            db,
            sc_ids_by_slugs,
            metro_ids_by_titles
        )
        await insert_service_centers_brands(
            db,
            sc_ids_by_slugs,
            brand_ids_by_slugs
        )
        await insert_service_centers_devices(
            db,
            sc_ids_by_slugs,
            device_ids_by_slugs
        )
        await insert_service_centers_metros(
            db,
            sc_ids_by_slugs,
            metro_ids_by_titles
        )
        await insert_devices_brands(
            db,
            brand_ids_by_slugs,
            device_ids_by_slugs
        )
        await insert_service_centers_advantages(
            db,
            sc_ids_by_slugs,
            advantage_ids_by_titles
        )
        await insert_service_centers_features(
            db,
            sc_ids_by_slugs,
            feature_ids_by_titles
        )
        await db.commit()


async def create_schema(db):
    await db.execute(
        '''CREATE TABLE IF NOT EXISTS service_centers (
            id INTEGER PRIMARY KEY,
            slug VARCHAR(255) NOT NULL UNIQUE,
            title VARCHAR(255) NOT NULL,
            phone VARCHAR(255) NOT NULL,
            slogan VARCHAR(255),
            logo VARCHAR(255),
            site_url VARCHAR(255),
            description TEXT
        )'''
    )
    await db.execute(
        '''CREATE TABLE IF NOT EXISTS brands (
            id INTEGER PRIMARY KEY,
            slug VARCHAR(255) NOT NULL UNIQUE,
            title VARCHAR(255) NOT NULL,
            is_popular BOOLEAN NOT NULL
        )'''
    )
    await db.execute(
        '''CREATE TABLE IF NOT EXISTS devices (
            id INTEGER PRIMARY KEY,
            slug VARCHAR(255) NOT NULL UNIQUE,
            title VARCHAR(255) NOT NULL
        )'''
    )
    await db.execute(
        '''CREATE TABLE IF NOT EXISTS metros (
            id INTEGER PRIMARY KEY,
            title VARCHAR(255) NOT NULL UNIQUE
        )'''
    )
    await db.execute(
        '''CREATE TABLE IF NOT EXISTS locations (
            id INTEGER PRIMARY KEY,
            service_center_id INTEGER NOT NULL,
            metro_id INTEGER NOT NULL,
            address VARCHAR(255) NOT NULL,
            coords VARCHAR(255),
            is_primary BOOLEAN NOT NULL,
            UNIQUE (service_center_id, metro_id, address)
        )'''
    )
    await db.execute(
        '''CREATE TABLE IF NOT EXISTS advantages (
            id INTEGER PRIMARY KEY,
            title VARCHAR(255) NOT NULL UNIQUE
        )'''
    )
    await db.execute(
        '''CREATE TABLE IF NOT EXISTS features (
            id INTEGER PRIMARY KEY,
            title VARCHAR(255) NOT NULL UNIQUE
        )'''
    )
    await db.execute(
        '''CREATE TABLE IF NOT EXISTS opening_hours (
            id INTEGER PRIMARY KEY,
            service_center_id INTEGER NOT NULL,
            weekday_from TINYINT NOT NULL,
            weekday_to TINYINT NOT NULL,
            time_from VARCHAR(255),
            time_to VARCHAR(255),
            UNIQUE (service_center_id, weekday_from, weekday_to)
        )'''
    )
    await db.execute(
        '''CREATE TABLE IF NOT EXISTS gallery_images (
            id INTEGER PRIMARY KEY,
            service_center_id INTEGER NOT NULL,
            image VARCHAR(255),
            UNIQUE (service_center_id, image)
        )'''
    )
    await db.execute(
        '''CREATE TABLE IF NOT EXISTS services (
            id INTEGER PRIMARY KEY,
            device_id INTEGER NOT NULL,
            title VARCHAR(255) NOT NULL,
            price VARCHAR(255) NOT NULL,
            UNIQUE (device_id, title)
        )'''
    )
    await db.execute(
        '''CREATE TABLE IF NOT EXISTS advantage_service_center (
            advantage_id INTEGER,
            service_center_id INTEGER,
            PRIMARY KEY (advantage_id, service_center_id)
        )'''
    )
    await db.execute(
        '''CREATE TABLE IF NOT EXISTS brand_service_center (
            brand_id INTEGER,
            service_center_id INTEGER,
            PRIMARY KEY (brand_id, service_center_id)
        )'''
    )
    await db.execute(
        '''CREATE TABLE IF NOT EXISTS device_service_center (
            device_id INTEGER,
            service_center_id INTEGER,
            PRIMARY KEY (device_id, service_center_id)
        )'''
    )
    await db.execute(
        '''CREATE TABLE IF NOT EXISTS metro_service_center (
            metro_id INTEGER,
            service_center_id INTEGER,
            PRIMARY KEY (metro_id, service_center_id)
        )'''
    )
    await db.execute(
        '''CREATE TABLE IF NOT EXISTS feature_service_center (
            feature_id INTEGER,
            service_center_id INTEGER,
            PRIMARY KEY (feature_id, service_center_id)
        )'''
    )
    await db.execute(
        '''CREATE TABLE IF NOT EXISTS brand_device (
            brand_id INTEGER,
            device_id INTEGER,
            PRIMARY KEY (brand_id, device_id)
        )'''
    )


async def insert_brands(db):
    await db.executemany(
        get_insert_query('brands', ('slug', 'title', 'is_popular')),
        brands.values()
    )


async def insert_devices(db):
    await db.executemany(
        get_insert_query('devices', ('slug', 'title')),
        devices.values()
    )


async def insert_metros(db):
    await db.executemany(
        get_insert_query('metros', ('title',)),
        metros.values()
    )


async def insert_advantages(db):
    await db.executemany(
        get_insert_query('advantages', ('title',)),
        advantages.values()
    )


async def insert_features(db):
    await db.executemany(
        get_insert_query('features', ('title',)),
        features.values()
    )


async def insert_service_centers(db):
    await db.executemany(
        get_insert_query('service_centers', (
            'slug',
            'title',
            'phone',
            'slogan',
            'logo',
            'site_url',
            'description',
        )),
        [
            defaultdict(lambda: None, sc)
            for sc in service_centers.values()
        ]
    )


async def insert_locations(db, sc_ids_by_slugs, metro_ids_by_titles):
    await db.executemany(
        get_insert_query('locations', (
            'service_center_id',
            'metro_id',
            'address',
            'coords',
            'is_primary',
        )),
        [
            {
                'service_center_id': sc_ids_by_slugs[sc_slug],
                'metro_id': metro_ids_by_titles.get(location['metro']),
                'address': location['address'],
                'coords': location['coords'],
                'is_primary': location['is_primary'],
            }
            for sc_slug, sc in service_centers.items()
            for location in sc['locations']
        ]
    )


async def insert_opening_hours(db, sc_ids_by_slugs):
    await db.executemany(
        get_insert_query('opening_hours', (
            'service_center_id',
            'weekday_from',
            'weekday_to',
            'time_from',
            'time_to',
        )),
        [
            {'service_center_id': sc_ids_by_slugs[sc_slug], **opening_hour}
            for sc_slug, sc in service_centers.items()
            for opening_hour in sc['opening_hours']
        ]
    )


async def insert_services(db, device_ids_by_slugs):
    await db.executemany(
        get_insert_query('services', ('device_id', 'title', 'price')),
        [
            {'device_id': device_ids_by_slugs[device_slug], **service}
            for device_slug, device in devices.items()
            for service in device['services']
        ]
    )


async def insert_service_centers_advantages(
    db,
    sc_ids_by_slugs,
    advantage_ids_by_title
):

    await db.executemany(
        get_insert_query('advantage_service_center', (
            'service_center_id',
            'advantage_id',
        )),
        [
            {
                'service_center_id': sc_ids_by_slugs[sc_slug],
                'advantage_id': advantage_ids_by_title[advantage_title],
            }
            for sc_slug, sc in service_centers.items()
            for advantage_title in sc['advantages']
        ]
    )


async def insert_service_centers_features(
    db,
    sc_ids_by_slugs,
    feature_ids_by_title
):

    await db.executemany(
        get_insert_query('feature_service_center', (
            'service_center_id',
            'feature_id',
        )),
        [
            {
                'service_center_id': sc_ids_by_slugs[sc_slug],
                'feature_id': feature_ids_by_title[feature_title],
            }
            for sc_slug, sc in service_centers.items()
            for feature_title in sc['features']
        ]
    )


async def insert_service_centers_brands(
    db,
    sc_ids_by_slugs,
    brand_ids_by_slugs
):
    await db.executemany(
        get_insert_query('brand_service_center', (
            'service_center_id',
            'brand_id',
        )),
        [
            {
                'service_center_id': sc_ids_by_slugs[sc_slug],
                'brand_id': brand_ids_by_slugs[brand_slug],
            }
            for sc_slug, sc in service_centers.items()
            for brand_slug in sc['brands']
        ]
    )


async def insert_service_centers_devices(
    db,
    sc_ids_by_slugs,
    device_ids_by_slugs
):
    await db.executemany(
        get_insert_query('device_service_center', (
            'service_center_id',
            'device_id',
        )),
        [
            {
                'service_center_id': sc_ids_by_slugs[sc_slug],
                'device_id': device_ids_by_slugs[device_slug],
            }
            for sc_slug, sc in service_centers.items()
            for device_slug in sc['devices']
        ]
    )


async def insert_service_centers_metros(
    db,
    sc_ids_by_slugs,
    metro_ids_by_titles
):
    await db.executemany(
        get_insert_query('metro_service_center', (
            'service_center_id',
            'metro_id',
        )),
        [
            {
                'service_center_id': sc_ids_by_slugs[sc_slug],
                'metro_id': metro_ids_by_titles[metro_title],
            }
            for sc_slug, sc in service_centers.items()
            for metro_title in sc['metros']
        ]
    )


async def insert_gallery_images(db, sc_ids_by_slugs):
    await db.executemany(
        get_insert_query('gallery_images', (
            'service_center_id',
            'image',
        )),
        [
            {
                'service_center_id': sc_ids_by_slugs[sc_slug],
                'image': image,
            }
            for sc_slug, sc in service_centers.items()
            for image in sc['gallery']
        ]
    )


async def insert_devices_brands(db, brand_ids_by_slugs, device_ids_by_slugs):
    await db.executemany(
        get_insert_query('brand_device', ('brand_id', 'device_id')),
        [
            {
                'brand_id': brand_ids_by_slugs[brand_slug],
                'device_id': device_ids_by_slugs[device_slug],
            }
            for device_slug, device in devices.items()
            for brand_slug in device['brands']
        ]
    )


async def get_items_ids_by_field(db, table, field):
    async with db.execute(f'SELECT id, {field} FROM {table}') as cursor:
        return {
            item[field]: item['id']
            for item in await cursor.fetchall()
        }


def get_insert_query(table, fields):
    return f'''INSERT OR IGNORE INTO {table} (
        {', '.join(fields)}
    ) VALUES (
        {', '.join([f':{field}' for field in fields])}
    )'''


if __name__ == '__main__':
    asyncio.run(main())
