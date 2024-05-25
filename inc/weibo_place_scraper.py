"""
此脚本使用 Python 的异步 I/O 库 `asyncio` 和 HTTP 客户端库 `aiohttp` 实现对指定城市（如上海）微博地点数据的抓取，并将抓取结果存储为 CSV 文件。

所需库：
- asyncio: 提供异步编程支持
- aiohttp: 异步 HTTP 客户端库，用于发送网络请求
- pandas: 用于数据处理与 CSV 文件生成

主要功能：
1. 异步抓取指定城市多页地点数据。
2. 实时显示抓取进度（已完成页面数/总页面数，已抓取地名数量）。
3. 将所有抓取到的地点信息整合为一个列表，并转换为 DataFrame。
4. 将 DataFrame 写入 CSV 文件。

全局变量：
- headers: 请求头，包含 Cookies 和 User-Agent 等信息，模拟浏览器请求以避免被服务器识别为爬虫。

函数说明：

调用示例:
import asyncio

# 导入 weibo_place_scraper 模块
import weibo_place_scraper

async def run_weibo_scraper(city, num_pages):
    # 调用 weibo_place_scraper 模块中的 main 函数
    await weibo_place_scraper.main(city, num_pages)

# 示例：抓取“北京”的地点数据，共抓取 100 页
asyncio.run(run_weibo_scraper("北京", 100))
"""

import asyncio
import time

import aiohttp
import pandas as pd


def headers():
    """
    定义请求头，包含必要的 Cookies 和 User-Agent 信息。

    返回:
        dict: 请求头字典
    """
    return {
        "Cookie": "SINAGLOBAL=8599286144449.927.1714293457090; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WF1M-aqdj0KVVdyH6PIFP_.5JpX5K-hUgL.Fo-0SKqf1Kz0S0B2dJLoIEBLxKqL1KnLB-qLxK-L12-L1-qLxK-L1K2LBoeLxK.LBonLB-Bt; ALF=1718509699; SUB=_2A25LQqPTDeRhGeNN7lQU-SzPzDiIHXVoIbkbrDV8PUJbkNB-LWbNkW1NSW3YpJqvZi7ea2MsVXOmyKunbVKTp3-y; XSRF-TOKEN=_5TT9hKL8qCqlj5q1_QNvZNs; WBPSESS=aCn8rBMzDHJZTQD4UbM_CfmGFvBxuYCXcDVDZZtuJ8dWQx3MmqM_VVCkdw7PpRz2VuVDxpgMGzDUDdfOS4eWbtLBXcJUXCG1DEi98qfD7rBcY38DSgFrVcOLZWvg8HusKjChUpzHb5YQp4giZ_r0QQ==; PC_TOKEN=fe56dcd49b; _s_tentry=-; Apache=3542630535877.036.1716281974275; ULV=1716281974278:6:4:2:3542630535877.036.1716281974275:1716180088558; wb_view_log_5356592374=1920*10801; webim_unReadCount=%7B%22time%22%3A1716282020066%2C%22dm_pub_total%22%3A0%2C%22chat_group_client%22%3A0%2C%22chat_group_notice%22%3A0%2C%22allcountNum%22%3A33%2C%22msgbox%22%3A0%7D",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    }


async def fetch_page(session, url,semaphore):
    """
    异步抓取指定 URL 的微博地点数据。

    参数:
        session (aiohttp.ClientSession): 用于发送请求的 aiohttp 客户端会话实例。
        url (str): 微博地点数据 API URL。

    返回:
        tuple[list[dict], int]: 包含两个元素的元组，第一个元素是抓取到的地点信息列表（每个元素为包含 poiid、title、lat、lon 字段的字典），
                               第二个元素是该页面抓取到的地点数量。
    """
    async with semaphore:
        async with session.get(url, headers=headers()) as response:
            data = await response.json()
            try:
                pois_list = []
                num_pois = 0

                if data["data"]['pois']:
                    for item in data["data"]['pois']:
                        poi_info = {
                            "poiid": item["poiid"],
                            "title": item["title"],
                            "lat": item["lat"],
                            "lon": item["lon"]
                        }
                        pois_list.append(poi_info)
                        num_pois += 1

                else:
                    print('已抓完')

                return pois_list, num_pois
            except Exception as e:
                print(f"抓取异常: {e}")
                return [],0


async def main(site_name, num_pages,semaphore):
    """
    主函数，负责协调异步任务并管理抓取流程。

    参数:
        stie_name (str): 指定要抓取的城市名（如“上海”）。
        num_pages (int): 指定要抓取的页面数。

    返回:
        None
    """
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_page(session, f'https://weibo.com/ajax/statuses/place?q={site_name}&page={i}', semaphore)
                for i in range(1, num_pages+1)]

        total_num_pois = 0
        completed_pages = 0
        all_pois = []

        for task_result in asyncio.as_completed(tasks):
            # if tasks_result=[]:
                time.sleep(1)
                try:
                    pois_list, num_pois = await task_result
                    if pois_list is not None:  # 确保只有当抓取成功时才累加和扩展数据
                        total_num_pois += num_pois
                        completed_pages += 1
                        print(f'已完成抓取 {site_name}{completed_pages}/{num_pages} 页，已抓取地名 {total_num_pois} 个')
                        all_pois.extend(pois_list)
                    else:  # 明确处理None情况，这里选择直接跳过
                        print(f"第 {completed_pages}/{num_pages} 页抓取失败，已跳过")
                except Exception as e:
                    print(f"在处理任务时遇到异常: {e}, 已跳过")

    df = pd.DataFrame(all_pois)
    df.to_csv(f'place_all/{site_name}_place.csv', index=False)
    print(f'抓到 {total_num_pois} 个地名')


async def fetch_data(max_concurrent=5):
    semaphore = asyncio.Semaphore(max_concurrent)  # 限制最大并发数
    place_name = '上海'
    all_places = [
        '黄浦区', '徐汇区', '长宁区', '静安区', '普陀区',
        '虹口区', '杨浦区', '浦东新区', '闵行区', '宝山区',
        '嘉定区', '金山区', '松江区', '青浦区', '奉贤区', '崇明区'
    ]

    # 首先处理整个上海
    await main(place_name, 140, semaphore)

    # 然后处理每个区
    tasks = [main(f"{place_name}{i}", 140, semaphore) for i in all_places]
    results = await asyncio.gather(*tasks, return_exceptions=True)  # 收集所有并发任务的结果，包括异常

if __name__ == '__main__':
    asyncio.run(fetch_data())
