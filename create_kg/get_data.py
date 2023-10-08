import json
import os
import re  # 正则表达式，用于文字匹配
import time
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup  # 用于网页解析，对HTML进行拆分，方便获取数据
import pickle as pkl
from selenium import webdriver
import selenium
from selenium.webdriver.chrome.service import Service
from urllib.request import urlretrieve
import ssl
from collections import defaultdict

option = webdriver.ChromeOptions()
option.add_argument('headless')  # 设置option 不显示浏览器
option.add_argument('blink-settings=imagesEnabled=false')  # 设置option 不加载图片
chrome_driver = webdriver.Chrome(options=option, service=Service('driver/chromedriver.exe'))
ssl._create_default_https_context = ssl._create_unverified_context

def processUrl(baseUrl, filePath):
    """
    将filePath文件中的电影名称拼接到baseUrl后，针对WiKi的三种电影检索方式进行了处理。
    :param baseUrl: WiKi基础链接
    :param filePath: 存有电影名称的文件
    :return: 返回moveId和三种URL的列表。
    """
    movieUrlList = []  # 存放所有电影的wiki链接
    # 特殊URL有75个，人工收集的有76个。刚开始先爬取特殊URL，然后爬取正常URL，最后有40个不需要爬取（因为内容和之前的一样，只是换了名字）
    with open(filePath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            data = json.loads(line)
            movie = dict()
            movie['movieId'] = dict(data).get('movieId')

            movieName = dict(data).get('movieName')
            movie['movieName'] = movieName.strip()
            if 'special_url' in dict(data).keys():
                # 如果有特殊URL
                movie['url1'] = baseUrl + dict(data).get('special_url')
                movie['url2'] = 'no_url'
                movie['url3'] = 'no_url'
                movie['url4'] = 'no_url'
                movie['url5'] = 'no_url'
            elif 'alternativeMovieId' in dict(data).keys():
                movie['alternativeName'] = dict(data).get('alternativeName')
                movie['alternativeMovieId'] = dict(data).get('alternativeMovieId')
            else:
                # 判断电影名末尾是否含有时间
                time_flag = re.match(r'.*\(\d\d\d\d\)$', movieName)
                if time_flag:
                    # 有时间
                    index = str(movieName).rfind('(')  # 获取到 '('字符的位置
                    movie_name = '_'.join(movieName[:index].strip().split(' '))  # 电影名格式 xxx_xxx
                    movie_time = movieName[index:]  # 时间格式 (xxxx)

                    movie['url1'] = baseUrl + movie_name + '_' + movie_time[:-1] + '_film)'  # 电影名_(时间_film)
                    movie['url2'] = baseUrl + movie_name + '_(film)'  # 电影名_(film)
                    movie['url3'] = baseUrl + movie_name + '_' + movie_time  # 电影名_(时间)
                    movie['url4'] = baseUrl + movie_name  # 电影名
                    movie['url5'] = baseUrl + movie_name + '_' + movie_time[
                                                                 :-1] + '_American_film)'  # 电影名_(时间_American_film)
                else:
                    # 无时间
                    movie_name = '_'.join(movieName.strip().split(' '))  # 电影名格式 xxx_xxx
                    movie['url1'] = baseUrl + movie_name + '_(film)'  # 电影名_(film)
                    movie['url2'] = baseUrl + movie_name  # 电影名
                    movie['url3'] = baseUrl + movie_name + '_(American_film)'  # 电影名_(American_film)
                    movie['url4'] = 'no_url'
                    movie['url5'] = 'no_url'
            movieUrlList.append(movie)
    return movieUrlList


def askURL(url):
    """
    根据URL返回网页HTML源码
    :param url: 网页链接
    :return:
    """
    msgtimeout = True
    try:
        chrome_driver.get(url)
        time.sleep(0.25)
        html = chrome_driver.page_source
        msgtimeout = False
        return html
    except selenium.common.exceptions.TimeoutException as e:
        print('获取HTML超时：{}'.format(url))
        chrome_driver.delete_all_cookies()
        msgtimeout = False
        return chrome_driver.get(url)
    finally:
        if msgtimeout:
            return chrome_driver.get(url)


def getData(urlList):
    """
    爬取指定URL的Wiki页面信息
    :param urlList: 每部电影的URL列表
    :return: 电影信息列表，爬取失败的列表
    """
    # 特殊URL有75个，人工收集的有75个。刚开始先爬取特殊URL，然后爬取正常URL，最后有40个不需要爬取（因为内容和之前的一样，只是换了名字）
    print("--------------------- 开始爬取网页 ---------------------")
    movieInfoList = []
    movie_failed = []
    successful_flag = False

    # 爬取所有网页并获取需要的信息
    for movie in tqdm(urlList):

        movieInfo = dict()
        movieInfo['movieId'] = movie['movieId']
        movieInfo['movieName'] = '_'.join(str(movie['movieName']).strip().split(' '))

        # 如果当前电影有别称，则直接将别称电影的信息复制，无需再爬虫（有别称的电影是最后40个）
        if 'alternativeMovieId' in dict(movie).keys():
            for temp in movieInfoList:
                if temp['movieId'] == movie['alternativeMovieId']:
                    movieInfo['img'] = temp['img']
                    movieInfo['Directed_by'] = temp['Directed_by']
                    movieInfo['Produced_by'] = temp['Produced_by']
                    movieInfo['Starring'] = temp['Starring']
                    movieInfo['Language'] = temp['Language']
                    movieInfo['Screenplay_by'] = temp['Screenplay_by']
                    movieInfoList.append(movieInfo)
                    break
            continue

        for url in ['url1', 'url2', 'url3', 'url4', 'url5']:
            # 当前电影的链接已经全都找完
            if movie[url] == 'no_url':
                print('no_url--没有搜索到：' + movie['movieName'])
                movie_failed.append(movie['movieName'])
                break

            html = askURL(movie[url])  # 获取HTML源码

            # 对页面源码逐一解析
            soup = BeautifulSoup(html, "html.parser")  # 设置解析器
            if soup.find(attrs={"class": re.compile(r'noarticletext')}) is not None:
                # 当前电影的链接已经全都找完
                if url == 'url5':
                    print('url5--没有搜索到：' + movie['movieName'] + ' URL1：' + movie['url1'] + ' URL2：' + movie[
                        'url2'] + ' URL3：' + movie['url3'] + ' URL4：' + movie['url4'] + ' URL5：' + movie['url4'])
                    movie_failed.append(movie['movieName'])
                    break
                continue

            # 查找 class为infobox vevent的class标签 (是一个table)
            table = soup.find(name='table', attrs={"class": "infobox vevent"})
            if table is None:
                if url == 'url5':
                    print('没有table：' + movie['movieName'])
                    movie_failed.append(movie['movieName'])
                continue
            # 遍历table的所有tr标签
            for tr in table.find_all(name='tr'):
                # 判断当前tr是否为图片链接
                td_image = tr.find('td', class_='infobox-image')
                if td_image is not None:
                    if td_image.find('img') is not None:
                        movieInfo['img'] = 'https:' + td_image.find('img')['src']
                    continue
                # 判断当前tr是否有 label
                label = tr.find('th', class_='infobox-label')
                if label is not None:
                    key = '_'.join(str(label.get_text()).strip().split(' '))
                    # 只需要以下label项
                    if key == 'Directed_by' or key == 'Screenplay_by' or key == 'Produced_by' or key == 'Starring' or key == 'Language':
                        # 至少得进来一次才算成功搜索
                        successful_flag = True

                        # 获取当前label的item
                        # 当前label可能有多项item，例如制片人、主演，获取所有项，那么都会放在li中
                        lis = tr.find_all('li')
                        li_list = []
                        if lis is not None and len(lis) > 0:
                            for li in lis:
                                # 如果内容里包含 [1]这种引用，则去掉
                                content = li.get_text()
                                if re.match(r'.*[\d]', content):
                                    content = content[:str(content).rfind('[')]
                                # 将空格都换成 _
                                li_list.append('_'.join(str(content).strip().split(' ')))
                            movieInfo[key] = li_list
                            continue
                        # 当前label只有一项item，那么只会放在 class=infobox-data的td中
                        td = tr.find('td', class_='infobox-data')
                        if td is not None:
                            # 此时有两种情况：要么里面是多个<a>，要么真的就只有一项

                            alist = td.find_all('a')
                            if alist is not None:
                                # 情况1：有多个<a>标签，获取标签里的内容
                                items = []
                                for a in alist:
                                    content = a.get_text()
                                    # 如果内容里包含 [1]这种引用，则去掉
                                    if re.match(r'.*[\d]', content):
                                        content = content[:str(content).rfind('[')]
                                    items.append('_'.join(str(content).strip().split(' ')))
                                if len(items) == 0:
                                    # 如果没有内容，就跳过
                                    continue
                                movieInfo[key] = items
                            else:
                                # 情况2：真的就只有一个内容，此时直接获取text就行
                                content = td.get_text()
                                # 如果内容里包含 [1]这种引用，则去掉
                                if re.match(r'.*\[\d\]', content):
                                    content = content[:str(content).rfind('[')]
                                movieInfo[key] = '_'.join(str(content).strip().split(' '))

            # 只要url1、url2、url3中有一个能成功，就可以结束当前循环
            if 'img' not in movieInfo.keys():
                movieInfo['img'] = 'no_img'  # "img": [""] "img": ["no_img"]
            if 'Directed_by' not in movieInfo.keys():
                movieInfo['Directed_by'] = ['no_director']  # "Directed_by": [""] "Directed_by": ["no_director"]
            if 'Starring' not in movieInfo.keys():
                movieInfo['Starring'] = ['no_starring']  # "Starring": [""] "Starring": ["no_starring"]
            if 'Screenplay_by' not in movieInfo.keys():
                movieInfo['Writer_by'] = ['no_writer']  # "Screenplay_by": [""] "Screenplay_by": ["no_screenplay"]
            if 'Produced_by' not in movieInfo.keys():
                movieInfo['Produced_by'] = ['no_producer']  # "Produced_by": [""] "Produced_by": ["no_producer"]
            if 'Language' not in movieInfo.keys():
                movieInfo['Language'] = ['no_language']  # "Language": [""] "Language": ["no_language"]
            movieInfoList.append(movieInfo)

            if not successful_flag:
                temp = dict()
                temp['movieName'] = movie['movieName']
                temp['url1'] = movie['url1']
                temp['url2'] = movie['url2']
                temp['url3'] = movie['url3']
                temp['url4'] = movie['url4']
                movie_failed.append(temp)
            break
        #  开始找下一个电影的信息
    # 返回解析好的数据
    print("--------------------- 网页爬取结束 ---------------------")

    # 去重 搜索失败的电影
    movie_failed = sorted(set(movie_failed), key=movie_failed.index)
    return movieInfoList, movie_failed


def saveData(dataList, movie_failed, save_path):
    """
    保存数据
    :param dataList: 被成功搜索的电影的信息
    :param movie_failed: 搜索失败的电影
    :param save_path: 保存路径
    :return:
    """
    print("--------------------- 开始保存数据 ---------------------")
    print("成功搜索的电影：")
    with open(save_path + 'movie_info_2.jsonl', 'w', encoding='utf-8') as f:
        for data in tqdm(dataList):
            f.write(json.dumps(data, ensure_ascii=False) + '\n')
    print("搜索失败的电影：")
    with open(save_path + 'movie_failed_2.jsonl', 'w', encoding='utf-8') as f:
        for data in tqdm(movie_failed):
            f.write(json.dumps(data, ensure_ascii=False) + '\n')
    print("--------------------- 数据保存结束 ---------------------")
    return True


def extract_genre(info_path, genre_path):
    """
    共有16974部IMDb电影重复。共有5794部电影含有类别。
    抽取将IMDb信息文件中的类别，融入的WiKi信息文件中。
    :param info_path: WiKi信息文件路径
    :param genre_path: IMDb信息文件路径
    :return:
    """

    count = 0

    # 加载WiKi信息文件
    info = dict()
    with open(info_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            data = json.loads(line)
            info[data['movieName']] = data

    # 加载IMDb信息文件
    genre = dict()
    imdb = pd.read_csv(genre_path, delimiter="\t", dtype={'tconst': str, 'titleType': str, 'primaryTitle': str,
                                                          'originalTitle': str, 'isAdult': str, 'startYear': str,
                                                          'endYear': str, 'runtimeMinutes': str, 'genres': str})
    count_repeat = 0
    for index, row in tqdm(imdb.iterrows()):
        time = ''
        name_time = ''
        name = '_'.join(str(row['originalTitle']).split(' '))
        temp = dict()

        # 判断类别是否存在
        if '\\N' == row['titleType']:
            temp['type'] = False
        else:
            temp['type'] = row['titleType'] == 'movie'

        # 判断类别是否存在
        if '\\N' == row['genres']:
            temp['genre'] = ['no_genre']
        else:
            temp['genre'] = str(row['genres']).split(',')

        # 判断时间是否存在
        if '\\N' != row['startYear']:
            time = row['startYear']
            name_time = name + '_(' + time + ')'
            temp['time'] = time
        else:
            temp['time'] = ''

        # 判断是否已经有 同名电影_时间 或者 同名电影 被添加过
        if name_time in genre.keys():
            if genre[name_time]['time'] != '' and time != '' and int(genre[name_time]['time']) < int(time):
                genre[name_time] = temp
                genre[name] = temp
                count_repeat += 1
            elif genre[name_time]['type']:
                genre[name_time] = temp
                genre[name] = temp
                count_repeat += 1
        elif time != '':
            genre[name_time] = temp
            genre[name] = temp
        else:
            genre[name] = temp

    for movieName in tqdm(info.keys()):
        if 'Genre' in info[movieName].keys():
            continue
        alternativeName = movieName
        # 如果当前电影有替代名字，则用替代名电影的类型，但info的key始终是movieName。
        if 'alternativeName' in info[movieName].keys():
            alternativeName = info[movieName]['alternativeName']

        if alternativeName in genre.keys():
            count += 1
            info[movieName]['Genre'] = genre[alternativeName]['Genre']
        else:
            info[movieName]['Genre'] = ['no_genre']

    print("共有{}部IMDb电影重复".format(count_repeat))
    print("共有{}部电影含有类别".format(count))

    with open('data/inspired/spider_result/movie_info_genre_reins2.jsonl', 'w', encoding='utf-8') as f:
        for k, v in info.items():
            f.write(json.dumps(v, ensure_ascii=False) + '\n')

    # with open('data/redial/spider_result/movie_genre.jsonl', 'w', encoding='utf-8') as f:
    #     for k, v in info.items():
    #         temp = dict()
    #         temp['movieId'] = v['movieId']
    #         temp['movieName'] = v['movieName']
    #         temp['Genre'] = v['Genre']
    #         f.write(json.dumps(temp, ensure_ascii=False) + '\n')


def downloadFile(url, filename):
    """
    图片下载
    :param url: 图片的链接
    :param filename: 图片名称（以电影ID命名）
    :return:
    """
    urlretrieve(url, './data/inspired/image/' + filename)


def getImage():
    """
    下载movie_info_genre.json文件中所有电影海报
    :return:
    """
    info = dict()
    with open('data/redial/spider_result/movie_info.jsonl', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            data = json.loads(line)
            temp = dict()
            temp['movieName'] = data['movieName']
            temp['img'] = data['img']
            info[data['movieId']] = temp

    exist_ids = os.listdir('data/redial/image')
    exist_ids = [id.split('.')[0] for id in exist_ids]
    # 99703
    for k, v in tqdm(info.items()):
        if v['img'] != 'no_img' and k not in exist_ids:
            downloadFile(v['img'], str(k) + '.jpg')
            time.sleep(0.5)


if __name__ == '__main__':
    baseUrl = "https://en.wikipedia.org/wiki/"
    filePath = "./data/redial/spider_result/movie_id_name_fre.jsonl"
    movieUrlList = processUrl(baseUrl, filePath)
    dataList, movie_failed = getData(movieUrlList)
    save_path = "./data/redial/"
    saveData(dataList, movie_failed, save_path)
    extract_genre('./data/redial/movie_info.jsonl', './data/imdb/IMDB_data.tsv')
    getImage()