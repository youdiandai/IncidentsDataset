import asyncio
import json
import os
import sys
from PIL import Image
import aiohttp
import ssl

ssl._create_default_https_context = ssl._create_unverified_context


def mkdir_if_not_exist(path):
    # 如果文件夹不存在就创建一个
    if not os.path.exists(path):
        os.mkdir(path)


def create_hashset(file_path, filename):
    # 如果存在就打开文件，并从文件重建一个哈希表，如果不存在就创建一个文件
    file_name = os.path.join(file_path, filename)
    if os.path.exists(file_name):
        with open(file_name, 'r') as f:
            hashset = set()
            images = f.readlines()
            for image in images:
                hashset.add(image.strip())
            return hashset
    else:
        with open(file_name, 'w') as f:
            hashset = set()
            return hashset


sem = asyncio.Semaphore(100)


async def async_down(url, filepath):
    # 异步下载文件
    async with sem:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    result = await response.read()
                    mkdir_if_not_exist(os.path.dirname(os.path.join(IMAGE_DIR, filepath)))
                    with open(os.path.join(IMAGE_DIR, filepath), 'wb') as f:
                        f.write(result)
                    with open(os.path.join(IMAGE_DIR, 'downloaded.txt'), "a") as f:
                        f.write(filepath + "\n")
                    return str(url)
            except:
                with open(os.path.join(IMAGE_DIR, 'error.txt'), "a") as f:
                    f.write(filepath + "\n")
                # hashset2.add(filepath)


def count_images(path):
    # 递归计算目录下的文件数
    if os.path.isfile(path):
        return 1
    else:
        return sum([count_images(os.path.join(path, x)) for x in os.listdir(path)])


def count_images2(path):
    # 递归计算目录下的文件数
    if os.path.isfile(path):
        try:
            with Image.open(path) as i:
                pass
            return 1
        except:
            return 0
    else:
        return sum([count_images2(os.path.join(path, x)) for x in os.listdir(path)])


if __name__ == "__main__":
    # 创建存储文件的文件夹
    IMAGE_DIR = '/Users/changxin/Desktop/image_data'
    new_dir = sys.argv[1]
    if new_dir:
        IMAGE_DIR = new_dir
    mkdir_if_not_exist(IMAGE_DIR)
    print(f"下载文件到{IMAGE_DIR}目录")

    # 两个用于确认文件已经下载的哈希表
    DOWNLOADED = create_hashset(IMAGE_DIR, 'downloaded.txt')
    ERRORDOWNED = create_hashset(IMAGE_DIR, 'error.txt')

    # 读取数据库
    with open("../data/eccv_train.json", "r") as fp:
        dataset = json.load(fp)
    print("已成功下载图像:%d" % (count_images(IMAGE_DIR) - 2))
    print("剩余未下载图像共%d" % (len(dataset.keys()) - len(DOWNLOADED) - len(ERRORDOWNED)))

    # 生成一个 (url,文件相对路径)构成的列表
    dict_key_list = filter(lambda x: x not in DOWNLOADED and x not in ERRORDOWNED, dataset.keys())
    urls_filepath = [(dataset[x]['url'], x) for x in dict_key_list]

    # 协程下载所有图像
    loop = asyncio.get_event_loop()
    tasks = [loop.create_task(async_down(url_file[0], url_file[1])) for url_file in urls_filepath]
    loop.run_until_complete(asyncio.wait(tasks))
