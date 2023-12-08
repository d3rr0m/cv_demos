import aiohttp
import asyncio
import httpx
import argparse
from collections import namedtuple
from urllib.parse import urlunparse, urlencode
from csv import DictWriter


def init(filename):
    init_fieldnames_url = 'http://IP:4000/api/v1/structure'
    init_max_page_url = 'http://IP:4000/api/v1/logs'

    response = httpx.get(init_fieldnames_url)
    response.raise_for_status()
    fieldnames = [item['field_name'] for item in response.json()['items']]
    with open(filename, 'w') as csvfile:
        writer = DictWriter(
            csvfile,
            fieldnames=fieldnames,
            delimiter='\t',
            lineterminator='\n',
            )
        writer.writeheader()

    response = httpx.get(init_max_page_url, params={'page': 1})
    response.raise_for_status()
    totalEntries = response.json()['totalEntries']
    per_page = response.json()['per_page']
    max_page = round(totalEntries/per_page)+1

    return fieldnames, max_page


def write_to_csv(rows, fieldnames, filename):
    with open(filename, 'a') as csvfile:
        writer = DictWriter(
            csvfile,
            fieldnames=fieldnames,
            delimiter='\t',
            lineterminator='\n'
            )
        writer.writerows(rows['items'])


async def download_page(name, queue, fieldnames, filename):
    async with aiohttp.ClientSession() as session:
        while not queue.empty():
            url = await queue.get()

            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        rows = await response.json()
                        write_to_csv(rows, fieldnames, filename)
                    elif response.status == 429:
                        await asyncio.sleep(10)
                        await queue.put(url)
                    elif response.status == 404:
                        print(f'''The page {url} doesn't exist.''')
                    else:
                        print('f{url} {response.status}')
            except aiohttp.client_exceptions.ClientConnectorError as e:
                print('Ошибка подключения.', str(e))
            except asyncio.TimeoutError:
                print('Превышен таймаут ожидания при загрузке {url}')


def create_url(page):
    Components = namedtuple(
        typename='Components',
        field_names=['scheme', 'netloc', 'url', 'path', 'query', 'fragment']
        )
    params = {
        'page': f'{page}',
    }
    url = urlunparse(
        Components(
            scheme='http',
            netloc='5.159.103.105:4000',
            query=urlencode(params),
            path='',
            url='/api/v1/logs',
            fragment='',
        )
    )
    return url


async def main():
    parser = argparse.ArgumentParser(
        description='''Программа получает данные из сервиса API
         http://5.159.103.105:4000/ и сохраняет полученный датасет в файл в
         формате csv.'''
    )
    parser.add_argument(
        '-s',
        '--start_page',
        default=1,
        type=int,
        help='''Номер стартовой страницы с которой начнется выборка данных.
            Значение по умолчанию - 1.''',
        )
    parser.add_argument(
        '-e',
        '--end_page',
        default=None,
        type=int,
        help='''Номер последней страницы на которой выборка данных закончится.
            Значение по умолчанию - масимально доступное кол-во страниц.''',
        )
    parser.add_argument(
        '-t',
        '--tasks',
        default=5,
        type=int,
        help='''Количевство одновременно работающих asyncio.task.
            Рекомендуемое значение - не более 7. Значение по умолчанию - 5.''',
        ),
    parser.add_argument(
        '-f',
        '--filename',
        default='customs_data.csv',
        type=str,
        help='''Имя файла, куда будет сохранен датасет.
            Значение по умолчанию - customs_data.csv''',
        )
    args = parser.parse_args()
    fieldnames, max_page = init(args.filename)
    args.end_page = max_page+1 if args.end_page is None else args.end_page+1

    queue = asyncio.Queue()
    urls = [create_url(page) for page in range(args.start_page, args.end_page)]
    for url in urls:
        await queue.put(url)

    tasks = (asyncio.create_task(download_page(
        f'{worker}',
        queue,
        fieldnames,
        args.filename,
        )) for worker in range(args.tasks))

    await asyncio.gather(*tasks)


def start():
    asyncio.run(main())


if __name__ == '__main__':
    start()
