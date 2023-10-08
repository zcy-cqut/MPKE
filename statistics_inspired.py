import json
import os
from tqdm import tqdm
import pickle as pkl
import pandas as pd


def all_movie():
    """
    统计电影名以及频率
    :return:
    """
    #  读取原始文件（train、valid、test）
    redial_movie = dict()
    for file_name in ['train', 'valid', 'test']:
        with open("./data/redial/datasets/" + file_name + "_data_dbpedia_raw.jsonl", 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                data = json.loads(line)
                if len(data['movieMentions']) == 0:
                    continue
                for k, v in data['movieMentions'].items():
                    if k not in redial_movie:
                        redial_movie[k] = dict()
                        redial_movie[k]['movieName'] = v
                        redial_movie[k]['frequency'] = 0
                    else:
                        redial_movie[k]['frequency'] = redial_movie[k]['frequency'] + 1

    # 将所有电影名和次数写入到文件
    # 真实的电影数量是 movie_id_name_fre_backup文件
    with open("data/inspired/spider_result/movie_id_name_fre.jsonl", 'w', encoding='utf-8') as f:
        for k, v in redial_movie.items():
            temp = dict()
            temp['movieId'] = k
            temp['movieName'] = v['movieName']
            temp['frequency'] = v['frequency']
            f.write(json.dumps(temp) + "\n")
    return redial_movie


def all_genre():
    """
    统计电影类别
    :return:
    """
    info = dict()
    with open('data/inspired/spider_result/movie_info_genre_reins2.jsonl', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            data = json.loads(line)
            temp = dict()
            temp['movieName'] = data['movieName']
            temp['img'] = data['img']
            temp['Directed_by'] = data['Directed_by']
            temp['Produced_by'] = data['Produced_by']
            temp['Starring'] = data['Starring']
            temp['Language'] = data['Language']
            temp['Writer_by'] = data['Writer_by']
            temp['Genre'] = data['Genre']
            info[data['movieId']] = temp

    movie_count = 0
    has_genre = 0
    not_genre = 0
    genre_statistic = dict()
    for k, v in info.items():
        movie_count += 1
        if v['Genre'][0] == 'no_genre':
            not_genre += 1
        else:
            has_genre += 1
        for genre in v['Genre']:
            if genre not in genre_statistic:
                genre_statistic[genre] = 1
            else:
                genre_statistic[genre] += 1

    with open('./data/inspired/statistic/movie_info_genre_reins2.jsonl', 'w', encoding='utf-8') as f:
        f.write("共有 {} 部电影\n".format(movie_count))
        f.write("有 {} 部电影含有类别属性\n".format(has_genre))
        f.write("有 {} 部电影不含有类别属性\n".format(not_genre))
        f.write("有 {} 种类别\n".format(len(genre_statistic.keys())))
        f.write("------具体的电影类别------\n")
        genre_statistic = sorted(genre_statistic.items(), key=lambda x: x[1], reverse=True)
        for item in genre_statistic:
            f.write("类别：{}，电影数量：{}\n".format(item[0], item[1]))

    # xx 的电影超过1 才算有效xx
    with open('data/inspired/statistic/valid_tails.txt', 'a', encoding='utf-8') as f:
        for item in genre_statistic:
            if item[0] == 'no_genre':
                continue
            if item[1] > 1:
                f.write(item[0] + '\n')

    # xx 的电影超过1 才算有效xx
    with open('data/inspired/statistic/valid_genre.txt', 'w', encoding='utf-8') as f:
        for item in genre_statistic:
            if item[0] == 'no_genre':
                continue
            if item[1] > 1:
                f.write(item[0] + '\n')


def all_language():
    """
    统计语言
    :return:
    """
    info = dict()
    with open('data/inspired/spider_result/movie_info_genre_reins2.jsonl', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            data = json.loads(line)
            temp = dict()
            temp['movieName'] = data['movieName']
            temp['img'] = data['img']
            temp['Directed_by'] = data['Directed_by']
            temp['Produced_by'] = data['Produced_by']
            temp['Starring'] = data['Starring']
            temp['Language'] = data['Language']
            temp['Writer_by'] = data['Writer_by']
            temp['Genre'] = data['Genre']
            info[data['movieId']] = temp

    movie_count = 0
    has = 0
    hasnot = 0
    overhas = 0
    statistic = dict()
    average = []
    for k, v in info.items():
        movie_count += 1
        if v['Language'][0] == 'no_language':
            hasnot += 1
        else:
            has += 1
            average.append(len(v['Language']))
            if len(v['Language']) > 1:
                overhas += 1

        for item in v['Language']:
            if item not in statistic:
                statistic[item] = 1
            else:
                statistic[item] += 1

    with open('./data/inspired/statistic/movie_statistic_language.jsonl', 'w', encoding='utf-8') as f:
        f.write("共有 {} 部电影\n".format(movie_count))
        f.write("有语言的电影数量：{} \n".format(has))
        f.write("无语言的电影数量：{}\n".format(hasnot))
        f.write("共有 {} 种语言\n".format(len(statistic.keys())))
        f.write("拥有一部电影以上的有 {} 种语言\n".format(overhas))
        f.write("平均每部电影含 {} 种语言\n".format(float(sum(average)) / len(average)))
        f.write("------每种语言拥有的电影情况------\n")
        statistic = sorted(statistic.items(), key=lambda x: x[1], reverse=True)
        for item in statistic:
            f.write("语言：{}，电影数量：{}\n".format(item[0], item[1]))

    # xx 的电影超过1 才算有效xx
    with open('data/inspired/statistic/valid_tails.txt', 'a', encoding='utf-8') as f:
        for item in statistic:
            if item[0] == 'no_language':
                continue
            if item[1] > 1:
                f.write(item[0] + '\n')

    # xx 的电影超过1 才算有效xx
    with open('data/inspired/statistic/valid_language.txt', 'w', encoding='utf-8') as f:
        for item in statistic:
            if item[0] == 'no_language':
                continue
            if item[1] > 1:
                f.write(item[0] + '\n')


def all_director():
    """
    统计导演
    :return:
    """
    info = dict()
    with open('data/inspired/spider_result/movie_info_genre_reins2.jsonl', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            data = json.loads(line)
            temp = dict()
            temp['movieName'] = data['movieName']
            temp['img'] = data['img']
            temp['Directed_by'] = data['Directed_by']
            temp['Produced_by'] = data['Produced_by']
            temp['Starring'] = data['Starring']
            temp['Language'] = data['Language']
            temp['Writer_by'] = data['Writer_by']
            temp['Genre'] = data['Genre']
            info[data['movieId']] = temp

    movie_count = 0
    has = 0
    hasnot = 0
    overhas = 0
    statistic = dict()
    average = []
    for k, v in info.items():
        movie_count += 1
        if v['Directed_by'][0] == 'no_director':
            hasnot += 1
        else:
            has += 1
            average.append(len(v['Directed_by']))
            if len(v['Directed_by']) > 1:
                overhas += 1

        for item in v['Directed_by']:
            if item not in statistic:
                statistic[item] = 1
            else:
                statistic[item] += 1

    with open('./data/inspired/statistic/movie_statistic_director.jsonl', 'w', encoding='utf-8') as f:
        f.write("共有 {} 部电影\n".format(movie_count))
        f.write("有导演的电影数量：{} \n".format(has))
        f.write("无导演的电影数量：{}\n".format(hasnot))
        f.write("共有 {} 位导演\n".format(len(statistic.keys())))
        f.write("拥有一部电影以上的有 {} 位导演\n".format(overhas))
        f.write("平均每部电影含 {} 位导演\n".format(float(sum(average)) / len(average)))
        f.write("------每位导演拥有的电影情况------\n")
        statistic = sorted(statistic.items(), key=lambda x: x[1], reverse=True)
        for item in statistic:
            f.write("导演：{}，电影数量：{}\n".format(item[0], item[1]))

    # xx 的电影超过1 才算有效xx
    with open('data/inspired/statistic/valid_tails.txt', 'a', encoding='utf-8') as f:
        for item in statistic:
            if item[1] > 1:
                if item[0] == 'no_director':
                    continue
                f.write(item[0] + '\n')

    # xx 的电影超过1 才算有效xx
    with open('data/inspired/statistic/valid_director.txt', 'w', encoding='utf-8') as f:
        for item in statistic:
            if item[1] > 1:
                if item[0] == 'no_director':
                    continue
                f.write(item[0] + '\n')


def all_starring():
    """
    统计主演
    :return:
    """
    info = dict()
    with open('data/inspired/spider_result/movie_info_genre_reins2.jsonl', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            data = json.loads(line)
            temp = dict()
            temp['movieName'] = data['movieName']
            temp['img'] = data['img']
            temp['Directed_by'] = data['Directed_by']
            temp['Produced_by'] = data['Produced_by']
            temp['Starring'] = data['Starring']
            temp['Language'] = data['Language']
            temp['Writer_by'] = data['Writer_by']
            temp['Genre'] = data['Genre']
            info[data['movieId']] = temp

    movie_count = 0
    has = 0
    hasnot = 0
    overhas = 0
    statistic = dict()
    average = []
    for k, v in info.items():
        movie_count += 1
        if v['Starring'][0] == 'no_starring':
            hasnot += 1
        else:
            has += 1
            average.append(len(v['Starring']))
            if len(v['Starring']) > 1:
                overhas += 1

        for item in v['Starring']:
            if item not in statistic:
                statistic[item] = 1
            else:
                statistic[item] += 1

    with open('./data/inspired/statistic/movie_statistic_starring.jsonl', 'w', encoding='utf-8') as f:
        f.write("共有 {} 部电影\n".format(movie_count))
        f.write("有主演的电影数量：{} \n".format(has))
        f.write("无主演的电影数量：{}\n".format(hasnot))
        f.write("共有 {} 位主演\n".format(len(statistic.keys())))
        f.write("拥有一部电影以上的有 {} 位演员\n".format(overhas))
        f.write("平均每部电影含 {} 位演员\n".format(float(sum(average)) / len(average)))
        f.write("------每位演员拥有的电影情况------\n")
        statistic = sorted(statistic.items(), key=lambda x: x[1], reverse=True)
        for item in statistic:
            f.write("演员：{}，电影数量：{}\n".format(item[0], item[1]))

    # xx 的电影超过1 才算有效xx
    with open('data/inspired/statistic/valid_tails.txt', 'a', encoding='utf-8') as f:
        for item in statistic:
            if item[1] > 1:
                if item[0] == 'no_starring':
                    continue
                f.write(item[0] + '\n')

    # xx 的电影超过1 才算有效xx
    with open('data/inspired/statistic/valid_starring.txt', 'w', encoding='utf-8') as f:
        for item in statistic:
            if item[1] > 1:
                if item[0] == 'no_starring':
                    continue
                f.write(item[0] + '\n')


def all_producer():
    """
    统计制片人
    :return:
    """
    info = dict()
    with open('data/inspired/spider_result/movie_info_genre_reins2.jsonl', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            data = json.loads(line)
            temp = dict()
            temp['movieName'] = data['movieName']
            temp['img'] = data['img']
            temp['Directed_by'] = data['Directed_by']
            temp['Produced_by'] = data['Produced_by']
            temp['Starring'] = data['Starring']
            temp['Language'] = data['Language']
            temp['Writer_by'] = data['Writer_by']
            temp['Genre'] = data['Genre']
            info[data['movieId']] = temp

    movie_count = 0
    has = 0
    hasnot = 0
    overhas = 0
    statistic = dict()
    average = []
    for k, v in info.items():
        movie_count += 1
        if v['Produced_by'][0] == 'no_producer':
            hasnot += 1
        else:
            has += 1
            average.append(len(v['Produced_by']))
            if len(v['Produced_by']) > 1:
                overhas += 1

        for item in v['Produced_by']:
            if item not in statistic:
                statistic[item] = 1
            else:
                statistic[item] += 1

    with open('./data/inspired/statistic/movie_statistic_producer.jsonl', 'w', encoding='utf-8') as f:
        f.write("共有 {} 部电影\n".format(movie_count))
        f.write("有制片的电影数量：{} \n".format(has))
        f.write("无制片的电影数量：{}\n".format(hasnot))
        f.write("共有 {} 位制片\n".format(len(statistic.keys())))
        f.write("拥有一部电影以上的有 {} 位制片\n".format(overhas))
        f.write("平均每部电影含 {} 位制片\n".format(float(sum(average)) / len(average)))
        f.write("------每位制片拥有的电影情况------\n")
        statistic = sorted(statistic.items(), key=lambda x: x[1], reverse=True)
        for item in statistic:
            f.write("制片：{}，电影数量：{}\n".format(item[0], item[1]))

    # xx 的电影超过1 才算有效xx
    with open('data/inspired/statistic/valid_tails.txt', 'a', encoding='utf-8') as f:
        for item in statistic:
            if item[1] > 1:
                if item[0] == 'no_producer':
                    continue
                f.write(item[0] + '\n')

    # xx 的电影超过1 才算有效xx
    with open('data/inspired/statistic/valid_producer.txt', 'w', encoding='utf-8') as f:
        for item in statistic:
            if item[1] > 1:
                if item[0] == 'no_producer':
                    continue
                f.write(item[0] + '\n')


def all_writer():
    """
    统计编剧
    :return:
    """
    info = dict()
    with open('data/inspired/spider_result/movie_info_genre_reins2.jsonl', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            data = json.loads(line)
            temp = dict()
            temp['movieName'] = data['movieName']
            temp['img'] = data['img']
            temp['Directed_by'] = data['Directed_by']
            temp['Produced_by'] = data['Produced_by']
            temp['Starring'] = data['Starring']
            temp['Language'] = data['Language']
            temp['Writer_by'] = data['Writer_by']
            temp['Genre'] = data['Genre']
            info[data['movieId']] = temp

    movie_count = 0
    has = 0
    hasnot = 0
    overhas = 0
    statistic = dict()
    average = []
    for k, v in info.items():
        movie_count += 1
        if v['Writer_by'][0] == 'no_writer':
            hasnot += 1
        else:
            has += 1
            average.append(len(v['Writer_by']))
            if len(v['Writer_by']) > 1:
                overhas += 1

        for item in v['Writer_by']:
            if item not in statistic:
                statistic[item] = 1
            else:
                statistic[item] += 1

    with open('./data/inspired/statistic/movie_statistic_writer.jsonl', 'w', encoding='utf-8') as f:
        f.write("共有 {} 部电影\n".format(movie_count))
        f.write("有编剧的电影数量：{} \n".format(has))
        f.write("无编剧的电影数量：{}\n".format(hasnot))
        f.write("共有 {} 位编剧\n".format(len(statistic.keys())))
        f.write("拥有一部电影以上的有 {} 位编剧\n".format(overhas))
        f.write("平均每部电影含 {} 位编剧\n".format(float(sum(average)) / len(average)))
        f.write("------每位编剧拥有的电影情况------\n")
        statistic = sorted(statistic.items(), key=lambda x: x[1], reverse=True)
        for item in statistic:
            f.write("编剧：{}，电影数量：{}\n".format(item[0], item[1]))

    # xx 的电影超过1 才算有效xx
    with open('data/inspired/statistic/valid_tails.txt', 'a', encoding='utf-8') as f:
        for item in statistic:
            if item[1] > 1:
                if item[0] == 'no_writer':
                    continue
                f.write(item[0] + '\n')

    # xx 的电影超过1 才算有效xx
    with open('data/inspired/statistic/valid_writer.txt', 'w', encoding='utf-8') as f:
        for item in statistic:
            if item[1] > 1:
                if item[0] == 'no_writer':
                    continue
                f.write(item[0] + '\n')


def all_img():
    """
    统计海报
    :return:
    """
    info = dict()
    with open('data/inspired/spider_result/movie_info_genre_reins2.jsonl', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            data = json.loads(line)
            temp = dict()
            temp['movieName'] = data['movieName']
            temp['img'] = data['img']
            temp['Directed_by'] = data['Directed_by']
            temp['Produced_by'] = data['Produced_by']
            temp['Starring'] = data['Starring']
            temp['Language'] = data['Language']
            temp['Writer_by'] = data['Writer_by']
            temp['Genre'] = data['Genre']
            info[data['movieId']] = temp

    movie_count = 0
    has = 0
    hasnot = 0
    for k, v in info.items():
        movie_count += 1
        if v['img'] == 'no_img':
            hasnot += 1
        else:
            has += 1

    with open('./data/inspired/statistic/movie_statistic_img.jsonl', 'w', encoding='utf-8') as f:
        f.write("共有 {} 部电影\n".format(movie_count))
        f.write("有海报的电影数量：{} \n".format(has))
        f.write("无海报的电影数量：{}\n".format(hasnot))


def statistic_movie_info():
    """
    统计电影相关知识的各种信息(类别、语言、导演、主演、制片人、编剧、图片)
    :return:
    """
    if os.path.exists('data/inspired/statistic/valid_tails.txt'):
        os.remove('data/inspired/statistic/valid_tails.txt')
    all_genre()
    all_language()
    all_director()
    all_starring()
    all_producer()
    all_writer()
    all_img()

    valid_name = set()
    with open('data/inspired/statistic/valid_director.txt', 'r', encoding='utf-8') as inf:
        valid_director = inf.read().splitlines()
    with open('data/inspired/statistic/valid_producer.txt', 'r', encoding='utf-8') as inf:
        valid_producer = inf.read().splitlines()
    with open('data/inspired/statistic/valid_writer.txt', 'r', encoding='utf-8') as inf:
        valid_writer = inf.read().splitlines()
    with open('data/inspired/statistic/valid_starring.txt', 'r', encoding='utf-8') as inf:
        valid_starring = inf.read().splitlines()
    with open('./data/inspired/statistic/valid_genre.txt', 'r', encoding='utf-8') as f:
        valid_genre = f.read().splitlines()
    with open('./data/inspired/statistic/valid_language.txt', 'r', encoding='utf-8') as f:
        valid_language = f.read().splitlines()

    # 整合有效 导演、制片、主演、编剧、类别、语言
    valid_all_entity = {'valid_director': valid_director, 'valid_producer': valid_producer,
                        'valid_starring': valid_starring,
                        'valid_writer': valid_writer, 'valid_genre': valid_genre, 'valid_language': valid_language}
    with open('data/inspired/statistic/valid_all_entity.pkl', 'wb') as outf:
        pkl.dump(valid_all_entity, outf)

    if os.path.exists('data/inspired/statistic/valid_director.txt'):
        os.remove('data/inspired/statistic/valid_director.txt')

    if os.path.exists('data/inspired/statistic/valid_producer.txt'):
        os.remove('data/inspired/statistic/valid_producer.txt')

    if os.path.exists('data/inspired/statistic/valid_writer.txt'):
        os.remove('data/inspired/statistic/valid_writer.txt')

    if os.path.exists('data/inspired/statistic/valid_starring.txt'):
        os.remove('data/inspired/statistic/valid_starring.txt')

    if os.path.exists('data/inspired/statistic/valid_genre.txt'):
        os.remove('data/inspired/statistic/valid_genre.txt')

    if os.path.exists('data/inspired/statistic/valid_language.txt'):
        os.remove('data/inspired/statistic/valid_language.txt')


def statistic_kg():
    """
    统计知识图谱DBpedia和LMKG的信息（三元组、电影实体数、人名实体、类别实体、语言实体）
    :return:
    """

    # ———————————————————————— 统计 DBpedia子图

    # 加载 DBpedia子图
    with open('data/redial/kg/dbpedia/dbpedia_subkg.pkl', 'rb') as f:
        dbpedia_subkg = pkl.load(f)

    # 加载 DBpedia entity2id
    with open('data/redial/kg/dbpedia/dbpedia_entity2id.json', 'r', encoding='utf-8') as f:
        dbpedia_entity2id = json.load(f)

    # 加载 DBpedia relation2id
    with open('data/redial/kg/dbpedia/dbpedia_relation2id.json', 'r', encoding='utf-8') as f:
        dbpedia_relation2id = json.load(f)

    # 开始统计
    with open('./data/redial/statistic/kg_statistic.txt', 'w', encoding='utf-8') as f:
        f.write('------------- 知识图谱：DBpedia 子图\n')
        f.write('三元组数量：{}\n'.format(sum([len(v) for k, v in dbpedia_subkg.items()])))

        # 1. 统计 实体总量
        f.write('实体总量：{}\n'.format(len(dbpedia_entity2id)))

        # 2. 统计 电影实体
        # 2.1 先找出数据集中的所有的电影实体（这些电影实体本身就是通过DBpedia的实体链接模型得出，DBpedia里没有的电影无法找出）
        # 故这种方式不会找出数据集中全部的电影实体。
        # 同时，也会出现【实体链接模型】识别出的实体不存在entity2id中。据统计有【10157】个实体。
        # （原因是模型与使用的DBpedia不是同一版本，可能模型已经很久不更新了，而DBpedia知识图谱会更新）
        dbpedia_movie_entity = set()
        for file in ['test_data_dbpedia.jsonl', 'valid_data_dbpedia.jsonl', 'train_data_dbpedia.jsonl']:
            entity = set()
            with open('./data/redial/datasets/' + file, 'r', encoding='utf-8') as f2:
                for line in tqdm(f2):
                    line = json.loads(line)
                    for message in line['messages']:
                        for e in message['movie']:
                            if e in dbpedia_entity2id:
                                entity.add(e)
            dbpedia_movie_entity |= entity

        # 2.2 判断DBpedia子图里的每个电影实体是否有效，有效是指：至少有一条边，如果没有边那么这个节点对于R-GCN来说也无法学习。
        dbpedia_movie_valid_entity = set()
        for movie_entity in dbpedia_movie_entity:
            if movie_entity in dbpedia_entity2id and len(dbpedia_subkg[movie_entity]) > 0:
                dbpedia_movie_valid_entity.add(movie_entity)
        f.write('有效电影实体数量：{}，无效电影实体：{}，共有：{}，覆盖率：{:.1f}%\n'.format(len(dbpedia_movie_valid_entity),
                                                                   len(dbpedia_movie_entity) - len(
                                                                       dbpedia_movie_valid_entity),
                                                                   len(dbpedia_movie_entity),
                                                                   len(dbpedia_movie_valid_entity) / 6637.0 * 100))

        # 3. 统计 人名实体
        # 3.1 根据【关系】找出电影实体对应的人名实体
        # 这些关系和人没关系：subsequentWork、genre、company、previousWork、location、productionCompany、format、language、distributor、country、network
        # 这些关系和人有关系：composer、starring、cinematography、director、musicComposer、editing、editor、person、producer、executiveProducer、narrator、author、writer
        relation_human = [3, 4, 6, 7, 8, 10, 14, 15, 16, 20, 21, 22, 23]
        dbpedia_human_entity = set()  # 所有人名实体
        for movie_entity in dbpedia_movie_valid_entity:
            for relation_tail in dbpedia_subkg[movie_entity]:
                if relation_tail[0] in dbpedia_relation2id and dbpedia_relation2id[relation_tail[0]] in relation_human:
                    dbpedia_human_entity.add(relation_tail[1])

        # 3.2 确定有效的人名实体，有效是指：该人名至少有两条与电影节点相连的边。如果只有一条边，那么该人名节点对其他电影节点是没作用的。
        dbpedia_human_valid_entity = set()  # 至少有两条边的
        for human_entity in dbpedia_human_entity:
            if human_entity not in dbpedia_entity2id:
                continue
            film_counter = 0
            for relation_tail in dbpedia_subkg[human_entity]:
                # 判断 以人名实体为头实体 的尾实体 是否是电影。
                if relation_tail[1] in dbpedia_movie_valid_entity:
                    film_counter += 1
            # 只有该人名实体与两个电影相连，这个人名才算有效
            if film_counter > 1:
                dbpedia_human_valid_entity.add(human_entity)
        f.write('有效人名实体数量：{}，无效人名实体：{}，共有：{}\n'.format(len(dbpedia_human_valid_entity),
                                                       len(dbpedia_human_entity) - len(dbpedia_human_valid_entity),
                                                       len(dbpedia_human_entity)))

        # 4. 统计 类别
        relation_genre = 1
        dbpedia_movie_with_genre = 0  # 带类别的电影数量
        dbpedia_genre_entity = set()  # 类别种类
        for movie_entity in dbpedia_movie_valid_entity:
            has_genre = False
            for relation_tail in dbpedia_subkg[movie_entity]:
                if relation_tail[0] in dbpedia_relation2id and dbpedia_relation2id[relation_tail[0]] == relation_genre:
                    dbpedia_genre_entity.add(relation_tail[1])
                    has_genre = True
            if has_genre:
                dbpedia_movie_with_genre += 1

        dbpedia_valid_genre = 0  # 有效的类别
        for genre in dbpedia_genre_entity:
            if len(dbpedia_subkg[genre]) > 1:
                dbpedia_valid_genre += 1
        f.write('共有{}种类别，有效的类别有{}种，带有类别的电影有{}部\n'.format(len(dbpedia_genre_entity),
                                                         dbpedia_valid_genre, dbpedia_movie_with_genre))

        # 5. 统计 语言
        relation_language = 13
        dbpedia_movie_with_language = 0  # 带语言的电影数量
        dbpedia_language_entity = set()  # 语言种类
        for movie_entity in dbpedia_movie_valid_entity:
            has_language = False
            for relation_tail in dbpedia_subkg[movie_entity]:
                if relation_tail[0] in dbpedia_relation2id and dbpedia_relation2id[
                    relation_tail[0]] == relation_language:
                    dbpedia_language_entity.add(relation_tail[1])
                    has_language = True
            if has_language:
                dbpedia_movie_with_language += 1

        dbpedia_valid_language = 0  # 有效的语言
        for language in dbpedia_language_entity:
            if len(dbpedia_subkg[language]) > 1:
                dbpedia_valid_language += 1
        f.write('共有{}种语言，有效的语言有{}种，带有语言的电影有{}部\n'.format(len(dbpedia_language_entity),
                                                         dbpedia_valid_language, dbpedia_movie_with_language))

        # 只找 kg_LMKG 中人名关系的（导演、主演、制片、编剧）
        lmkg_relation_human = [4, 6, 16, 20, 23]
        dbpedia_human_entity_special = set()
        for movie_entity in dbpedia_movie_valid_entity:
            for relation_tail in dbpedia_subkg[movie_entity]:
                if relation_tail[0] in dbpedia_relation2id and dbpedia_relation2id[
                    relation_tail[0]] in lmkg_relation_human:
                    dbpedia_human_entity_special.add(relation_tail[1])

        dbpedia_human_valid_entity_special = set()  # 至少有两条边的
        for human_entity in dbpedia_human_entity_special:
            if human_entity not in dbpedia_entity2id:
                continue
            film_counter = 0
            for relation_tail in dbpedia_subkg[human_entity]:
                if relation_tail[1] in dbpedia_movie_valid_entity:
                    film_counter += 1
            if film_counter > 1:
                dbpedia_human_valid_entity_special.add(human_entity)
        f.write('若只使用LMKG的人名关系===>有效人名实体数量：{}，无效人名实体：{}，共有：{}\n'.format(len(dbpedia_human_valid_entity_special),
                                                                        len(dbpedia_human_entity_special) -
                                                                        len(dbpedia_human_valid_entity_special),
                                                                        len(dbpedia_human_entity_special)))

    # ———————————————————————— 统计 kg_LMKG

    # 加载 kg_LMKG
    with open('data/redial/kg/lmkg/lmkg_raw.pkl', 'rb') as f:
        lmkg = pkl.load(f)

    # 加载 kg_LMKG entity2id
    with open('data/redial/kg/lmkg/lmkg_entity2id.json', 'r', encoding='utf-8') as f:
        lmkg_entity2id = json.load(f)

    # 加载 kg_LMKG relation2id
    with open('data/redial/kg/lmkg/lmkg_relation2id.json', 'r', encoding='utf-8') as f:
        lmkg_relation2id = json.load(f)

    # 不同于DBpedia子图，LMKG为数据集ReDial定制的知识图谱，符合该数据集的电影都用映射文件lmkg_movieId2entities.pkl整合了。
    with open('data/redial/kg/lmkg/lmkg_movieId2entities.pkl', 'rb') as f:
        lmkg_movieId2entities = pkl.load(f)

    # lmkg_movieId2entities包含了 电影id相关的所有实体，lmkg_movieId2movieEntities是电影id对应的电影实体
    with open('data/redial/kg/lmkg/lmkg_movieId2movieEntity.pkl', 'rb') as f:
        lmkg_movieId2movieEntity = pkl.load(f)

    # 开始统计
    with open('./data/redial/statistic/kg_statistic.txt', 'a', encoding='utf-8') as f:
        f.write('\n------------- 知识图谱：kg_LMKG\n')
        f.write('三元组数量：{}\n'.format(sum([len(v) for k, v in lmkg.items()])))

        # 1. 统计 实体总量
        f.write('实体总量：{}\n'.format(len(lmkg_entity2id)))

        # 2. 统计 电影实体
        # 2.1 找出LMKG中的电影实体
        lmkg_movie_entity = set()
        for movie_id, movie_entity in lmkg_movieId2movieEntity.items():
            lmkg_movie_entity.add(movie_entity)

        # 找出有效电影实体
        lmkg_movie_valid_entity = set()
        for movie_entity in lmkg_movie_entity:
            if movie_entity in lmkg_entity2id and len(lmkg[movie_entity]) > 0:
                lmkg_movie_valid_entity.add(movie_entity)
        f.write('有效电影实体数量：{}，无效电影实体：{}，共有：{}，覆盖率：{:.1f}%\n'.format(len(lmkg_movie_valid_entity),
                                                                   len(lmkg_movie_entity) - len(
                                                                       lmkg_movie_valid_entity),
                                                                   len(lmkg_movie_entity),
                                                                   len(lmkg_movie_entity) / 6637.0 * 100))

        # 3. 统计 人名实体
        relation_human = [0, 1, 2, 3]
        lmkg_human_entity = set()
        for movie_entity in lmkg_movie_valid_entity:
            for relation_tail in lmkg[movie_entity]:
                if lmkg_relation2id[relation_tail[0]] in relation_human:
                    lmkg_human_entity.add(relation_tail[1])

        # 3.2 找出有效人名实体
        lmkg_human_valid_entity = set()  # 至少有两条边的
        for human_entity in lmkg_human_entity:
            if human_entity not in lmkg_entity2id:
                continue
            film_counter = 0
            for relation_tail in lmkg[human_entity]:
                if relation_tail[1] in lmkg_movie_valid_entity:
                    film_counter += 1
            if film_counter > 1:
                lmkg_human_valid_entity.add(human_entity)
        f.write('有效人名实体数量：{}，无效人名实体：{}，共有：{}\n'.format(len(lmkg_human_valid_entity),
                                                       len(lmkg_human_entity) - len(lmkg_human_valid_entity),
                                                       len(lmkg_human_entity)))

        # 4. 统计 类别
        relation_genre = 4
        lmkg_movie_with_genre = 0  # 带类别的电影数量
        lmkg_genre_entity = set()  # 类别种类
        for movie_entity in lmkg_movie_valid_entity:
            has_genre = False
            for relation_tail in lmkg[movie_entity]:
                if lmkg_relation2id[relation_tail[0]] == relation_genre:
                    lmkg_genre_entity.add(relation_tail[1])
                    has_genre = True
            if has_genre:
                lmkg_movie_with_genre += 1

        lmkg_valid_genre = 0  # 有效的类别
        for genre in lmkg_genre_entity:
            if len(lmkg[genre]) > 1:
                lmkg_valid_genre += 1
        f.write('有{}种类别，有效的类别有{}种，带有类别的电影有{}部\n'.format(len(lmkg_genre_entity),
                                                        lmkg_valid_genre, lmkg_movie_with_genre))

        # 5. 统计 语言
        relation_language = 5
        lmkg_movie_with_language = 0  # 带语言的电影数量
        lmkg_language_entity = set()  # 语言种类
        for movie_entity in lmkg_movie_valid_entity:
            has_language = False
            for relation_tail in lmkg[movie_entity]:
                if lmkg_relation2id[relation_tail[0]] == relation_language:
                    lmkg_language_entity.add(relation_tail[1])
                    has_language = True
            if has_language:
                lmkg_movie_with_language += 1

        lmkg_valid_language = 0  # 有效的语言
        for language in lmkg_language_entity:
            if len(lmkg[language]) > 1:
                lmkg_valid_language += 1
        f.write('共有{}种语言，有效的语言有{}种，带有语言的电影有{}部\n'.format(len(lmkg_language_entity),
                                                         lmkg_valid_language, lmkg_movie_with_language))

        # 末尾解释 有效电影和有效人名
        f.write('\n------------- 解释\n')
        f.write('有效电影实体是指：以该节点为头实体，至少有一个三元组(连接到非电影节点)。\n')
        f.write('有效的人名/类别/语言是指：以该节点为头实体，至少有两个三元组(连接到电影节点)。\n')


if __name__ == '__main__':
    # all_movie()
    all_genre()
    all_language()
    all_director()
    all_starring()
    all_producer()
    all_writer()
    statistic_movie_info()
    # statistic_kg()
