import aiohttp
import asyncio
import json
import operator

from pyquery import PyQuery as pq

headers = {
    'content-type': "application/x-www-form-urlencoded",
    'accept': "application/json, text/javascript, */*; q=0.01",
    'accept-encoding': "gzip, deflate, br",
    'accept-language': "en-US,en;q=0.8,bg;q=0.6,ru;q=0.4",
    'connection': "keep-alive",
    'dnt': "1",
    'host': "7777.bg",
    'origin': "https://7777.bg",
    'referer': "https://7777.bg/loto_games/big_jackpot/",
    'x-requested-with': "XMLHttpRequest",
    'cache-control': "no-cache"
}

stats = {}
dates = {}


def add_statistic(row):
    if row[0] in dates:
        return  # Avoid redundant data in statistics
    else:
        dates[row[0]] = 1

    # TODO: extract timestamp from row[0] and write all data to DB
    for num in row[1]:
        stats[num] += 1


def __handle_data(d, k, v):
    heading = d(".inner-heading", v)
    balls = d(".draw-balls", v)

    if heading and balls:
        row_stat = ([heading.text(), balls.text().split(" ")])
        add_statistic(row_stat)


def handle_page_data(data):
    d = pq(data)
    d("#numbers div.grid-2.grid-box").children().each(
        lambda k, v:
        d(v).each(
            lambda k, v:
            d(v).children().each(
                lambda k, v:
                __handle_data(d, k, v)
            )
        )
    )


async def get_page_data(page):
    """

    :param page:
    :return:
    """
    payload = "filter_date=&draws_page=" + str(
        page) + "&filter_game_type=6_to_47&show_only=draws&page_limit=200&skip_ajax=0&skip_new_window=1&ajax_request=1"

    async with aiohttp.ClientSession() as session:
        async with session.post('https://7777.bg/loto_games/', data=payload, headers=headers) as resp:
            if 200 == resp.status:
                data = json.loads(await resp.text())
                handle_page_data(data['html'])


if __name__ == "__main__":
    # init empty stats
    for i in range(1, 48):
        stats[str(i)] = 0

    # get event loop
    loop = asyncio.get_event_loop()
    # ensure 5 tasks (5 * 200 records per page <~~ 1000 draws since 2015 till now)
    tasks = [asyncio.ensure_future(get_page_data(i)) for i in range(5)]
    # run tasks
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()

    sorted_stats = sorted(stats.items(), key=operator.itemgetter(1), reverse=True)

    print("Raw statistics --> " + str(stats))
    print("Sorted statistics --> " + str(sorted_stats))
    print("Most common 6 --> " + str([int(i[0]) for i in sorted_stats[:6]]))
    print("Most rare 6 --> " + str([int(i[0]) for i in sorted_stats[-6:]]))
    print("Dates: " + str(dates))
