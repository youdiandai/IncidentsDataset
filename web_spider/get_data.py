import asyncio
import json
import os
import sys

import aiohttp


# def down_image(image_name):
#     print('start')
#     m, n = image_name.split('/')
#     print(m)
#     mkdir_if_not_exist(os.path.join(IMAGE_DIR, m))
#     with requests.get(dataset[image_name]['url']) as response:
#         m, n = image_name.split('/')
#         if response.status_code == 200:
#             with open(os.path.join(IMAGE_DIR, m, n), 'wb') as f:
#                 f.write(response.content)
#
#
# async def async_down(url):
#     print("craw url:", url)
#     async with aiohttp.ClientSession() as session:
#         async with session.get(url) as resp:
#             result = await resp.text()
#             print(f"download image url:{url},{len(result)}")

def mkdir_if_not_exist(path):
    if not os.path.exists(path):
        os.mkdir(path)


def create_hashset(file_path):
    file_name = os.path.join(file_path, 'downloaded.txt')
    if os.path.exists(file_name):
        with open(file_name, 'r') as f:
            hashset = set()
            images = f.readlines()
            for image in images:
                hashset.add(image)
            return hashset
    else:
        with open(file_name, 'w') as f:
            hashset = set()
            return hashset


sem = asyncio.Semaphore(100)


async def async_down(url, filepath, hashset: set):
    # print(f"down image :{url}")
    async with sem:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                result = await response.read()
                mkdir_if_not_exist(os.path.dirname(os.path.join(IMAGE_DIR, filepath)))
                with open(os.path.join(IMAGE_DIR, filepath), 'wb') as f:
                    f.write(result)
                hashset.add(filepath)
                with open(os.path.join(IMAGE_DIR, 'downloaded.txt'), "a") as f:
                    f.write(filepath+"\n")
                return str(url)


if __name__ == "__main__":
    IMAGE_DIR = '/Users/changxin/Desktop/image_data'
    mkdir_if_not_exist(IMAGE_DIR)
    new_dir = sys.argv[1]
    if new_dir:
        IMAGE_DIR = new_dir
    print(f"下载文件到{IMAGE_DIR}目录")
    DOWNLOADED = create_hashset(IMAGE_DIR)

    # 读取数据库列表
    with open("../data/eccv_train.json", "r") as fp:
        dataset = json.load(fp)

    dict_key_list = list(dataset.keys())
    image_count = len(dict_key_list)
    downloaded = len(DOWNLOADED)
    print("剩余未下载图像共%d" % (image_count - downloaded))
    dict_key_list = filter(lambda x: x not in DOWNLOADED, dict_key_list)
    urls_filepath = [(dataset[x]['url'], x) for x in dict_key_list]

    loop = asyncio.get_event_loop()
    tasks = [loop.create_task(async_down(url_file[0], url_file[1], DOWNLOADED)) for url_file in urls_filepath]
    loop.run_until_complete(asyncio.wait(tasks))
    print("本次启动共下载图像%d张，剩余未下载图像共%d张" % (len(DOWNLOADED) - downloaded, image_count - len(DOWNLOADED)))
