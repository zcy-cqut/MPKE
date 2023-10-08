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


def construct_kg(info_path):
    """
    根据info文件构建知识图谱
    :return:
    """
    # 0. 加载WiKi信息文件
    info = dict()
    # 下面三个字典是为了方便 后面的遍历操作
    movie_human = defaultdict(dict)
    movie_genre = defaultdict(list)
    movie_language = defaultdict(list)
    print('-------------读取并处理info文件-------------')
    with open(info_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in tqdm(lines):
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

            # 当前电影 涉及的所有人(导演、主演、编剧、制片人)
            # movie_human = {电影名: {导演:[], 主演:[], 编剧:[], 制片人:[]}}
            human_list = defaultdict(set)
            for item in dict(data).keys():
                if item in ['Directed_by', 'Produced_by', 'Starring', 'Writer_by']:
                    for name in data[item]:
                        temp_set = set()
                        if name not in ['no_director', 'no_producer', 'no_starring', 'no_writer']:
                            temp_set.add(name)
                    if len(temp_set) > 0:
                        human_list[item] = temp_set
            if len(human_list) > 0:
                movie_human[data['movieName']] = human_list

            # 当前电影 的类别
            genre_list = []
            for genre in data['Genre']:
                if genre != 'no_genre':
                    genre_list.append(genre)
            if len(genre_list) > 0:
                movie_genre[data['movieName']] = genre_list

            # 当前电影 的语言
            language_list = []
            for language in data['Language']:
                if language != 'no_language':
                    language_list.append(language)
            if len(language_list) > 0:
                movie_language[data['movieName']] = language_list

    # 构建知识图谱
    print('\n-------------开始构建知识图谱LMKG-------------')
    lmkg_ttl = []
    base1 = 'http://zcycqut.org/resource/'
    base2 = 'http://zcycqut.org/ontology/'

    # 1. 头实体=电影实体 关系=Genre/Language/Directed_by/Writer_by/Produced_by/Starring 尾实体=具体类别/具体语言/具体人名
    # 具体人名实体必须是至少与其他两个电影实体有连接，即 如果这个实体只有一条边，那么在R-GCN后也没有意义。
    # 确定有效人名（也包括了 所有的类别、语言）
    print('\n-------------处理电影相关-------------')
    movieId2movieEntities = dict()  # 一个电影id 对应的 电影实体
    with open('data/inspired/statistic/valid_tails.txt', 'r', encoding='utf-8') as f:
        valid_tails = list({}.fromkeys(f.read().splitlines()).keys())  # 利用字典对列表去重
    relationList1 = ['Genre', 'Language', 'Directed_by', 'Writer_by', 'Produced_by', 'Starring']
    for k, v in tqdm(info.items()):
        entities = ['<' + base1 + v['movieName'] + '>']  # 为该电影id 添加 对应的电影实体
        movieId2movieEntities[k] = '<' + base1 + v['movieName'] + '>'
        for relation in relationList1:
            if len(v[relation]) == 1 and 'no_' in v[relation][0]:
                continue
            for item in v[relation]:
                if item in valid_tails:
                    lmkg_ttl.append(
                        '<' + base1 + v['movieName'] + '> <' + base2 + relation + '> <' + base1 + item + '> .')
                    entities.append('<' + base1 + item + '>')

    print('此时LMGK大小：{}'.format(len(lmkg_ttl)))
    lmkg_ttl = list({}.fromkeys(lmkg_ttl).keys())  # 去重
    print('去重后LMGK大小：{}'.format(len(lmkg_ttl)))

    # 2. 头实体=具体人名 关系=Director/Write/Produce/Actor 尾实体=电影实体
    print('\n-------------处理人名相关-------------')
    with open('./data/inspired/statistic/valid_all_entity.pkl', 'rb') as f:
        valid_all_entity = pkl.load(f)
    valid_name = set(valid_all_entity['valid_director'] + valid_all_entity['valid_producer'] +
                     valid_all_entity['valid_writer'] + valid_all_entity['valid_starring'])
    relationList2 = {'Directed_by': 'Director', 'Writer_by': 'Write', 'Produced_by': 'Produce', 'Starring': 'Actor'}
    for name in tqdm(valid_name):
        # movie_human = {电影名: {导演:[], 主演:[], 编剧:[], 制片人:[]}}, 遍历所有的电影，找出有效人名name做了什么
        for movie_name, work_person in movie_human.items():
            for work, person in work_person.items():
                # 有效人名在当前电影里担任过 导演/主演/编剧/制片人
                if name in person and work in relationList2:
                    lmkg_ttl.append(
                        '<' + base1 + name + '> <' + base2 + relationList2[work] + '> <' + base1 + movie_name + '> .')

    print('此时LMGK大小：{}'.format(len(lmkg_ttl)))
    lmkg_ttl = list({}.fromkeys(lmkg_ttl).keys())  # 去重
    print('去重后LMGK大小：{}'.format(len(lmkg_ttl)))

    # 3. 头实体=具体类型 关系=Genre_Has 尾实体=电影实体
    print('\n-------------处理类别相关-------------')
    valid_genre = valid_all_entity['valid_genre']
    for genre in tqdm(valid_genre):
        # movie_genre = {电影名: [类别1,类别2,]}, 遍历所有的电影，找出符合某类别的所有电影
        for k, v in movie_genre.items():
            if genre in v:
                lmkg_ttl.append('<' + base1 + genre + '> <' + base2 + 'Genre_Has' + '> <' + base1 + k + '> .')

    print('此时LMGK大小：{}'.format(len(lmkg_ttl)))
    lmkg_ttl = list({}.fromkeys(lmkg_ttl).keys())  # 去重
    print('去重后LMGK大小：{}'.format(len(lmkg_ttl)))

    # 4. 头实体=具体语言 关系=Language_Has 尾实体=电影实体
    print('\n-------------处理语言相关-------------')
    valid_language = valid_all_entity['valid_language']
    for language in tqdm(valid_language):
        # movie_language = {电影名: [语言1,语言2,]}, 遍历所有的电影，找出符合某语言的所有电影
        for k, v in movie_language.items():
            if language in v:
                lmkg_ttl.append('<' + base1 + language + '> <' + base2 + 'Language_Has' + '> <' + base1 + k + '> .')

    print('此时LMGK大小：{}'.format(len(lmkg_ttl)))
    lmkg_ttl = list({}.fromkeys(lmkg_ttl).keys())  # 去重
    print('去重后LMGK大小：{}'.format(len(lmkg_ttl)))

    # 5. 将知识图谱写入文件.ttl
    print('\n-------------保存知识图谱.ttl-------------')
    with open('data/inspired/kg/lmkg/lmkg_raw.ttl', 'w', encoding='utf-8') as f:
        lmkg_ttl = list({}.fromkeys(lmkg_ttl).keys())
        for item in tqdm(lmkg_ttl):
            f.write(item + '\n')

    # 6. 将知识图谱写入文件.pkl
    print('\n-------------保存知识图谱.pkl-------------')
    lmkg_pkl = defaultdict(list)

    for line in tqdm(lmkg_ttl):
        tuples = str(line).strip().split()
        if tuples is not None and len(tuples) == 4 and tuples[-1] == ".":
            h, r, t = tuples[:3]
            if "ontology" in r and "zcycqut" in h and "zcycqut" in t:
                lmkg_pkl[h].append((r, t))
    with open('data/inspired/kg/lmkg/lmkg_raw.pkl', 'wb') as f:
        pkl.dump(lmkg_pkl, f)
    with open('data/inspired/kg/lmkg/lmkg_movieId2movieEntity.pkl', 'wb') as f:
        pkl.dump(movieId2movieEntities, f)

    # 7. 映射知识图谱与数据集
    print('\n-------------映射知识图谱-------------')
    # 7.1 获取所有实体
    print('\n-------------读取LMKG的实体-------------')
    all_entity = set()
    for line in lmkg_ttl:
        tuples = line.strip().split()
        if tuples is not None and len(tuples) == 4 and tuples[-1] == ".":
            h, r, t = tuples[:3]
            all_entity.add(h)
            all_entity.add(t)

    # 7.2 将实体、关系、KG都映射为id
    # 加载关系
    with open('data/inspired/kg/lmkg/lmkg_relation_set.json', 'r', encoding='utf-8') as f:
        relation_set = json.load(f)

    # 映射 实体->id
    entity2id = {e: i for i, e in enumerate(all_entity)}
    print(f"KG实体数量: {len(entity2id)}")  # kg_LMKG = 14526  DBpedia_subkg = 31161

    # 映射 关系->id
    relation2id = {r: i for i, r in enumerate(relation_set)}
    # 添加关系 自环-->self_loop，自环用于R-GCN，因为是有向图，对于节点自身也需要权重
    relation2id['self_loop'] = len(relation2id)
    print(f"KG关系数量: {len(relation2id)}")  # kg_LMKG = 13  DBpedia_subkg = 25

    # 映射 知识图谱->id（之前的lmkg_pkl里都是文本，现在转换为id了）
    kg_idx = {}
    for head, relation_tails in lmkg_pkl.items():
        # 如果KG中某个头实体 存在于 all_entity
        if head in entity2id:
            # 添加到kg_idx字典，{该头实体: [(自环, 该头实体)]} ====》 R-GCN
            head = entity2id[head]
            kg_idx[head] = [(relation2id['self_loop'], head)]  # 先增加自环关系
            for relation_tail in relation_tails:
                if relation_tail[0] in relation2id and relation_tail[1] in entity2id:
                    kg_idx[head].append((relation2id[relation_tail[0]], entity2id[relation_tail[1]]))

    with open('data/inspired/kg/lmkg/lmkg.json', 'w', encoding='utf-8') as f:
        json.dump(kg_idx, f, ensure_ascii=False)
    with open('data/inspired/kg/lmkg/lmkg_entity2id.json', 'w', encoding='utf-8') as f:
        json.dump(entity2id, f, ensure_ascii=False)
    with open('data/inspired/kg/lmkg/lmkg_relation2id.json', 'w', encoding='utf-8') as f:
        json.dump(relation2id, f, ensure_ascii=False)


def extract_movie_entity():
    """
    抽取电影id应该包含的实体
    :return:
    """

    print("------------------ 电影ID 对应的实体")

    with open('data/inspired/statistic/valid_all_entity.pkl', 'rb') as outf:
        valid_all_entity = pkl.load(outf)

    # 电影id 应该含有的 相关实体（导演、制片、主演、编剧、类别、语言）
    lmkg_movieId2entities = defaultdict(dict)
    no_keys = ['no_genre', 'no_director', 'no_starring', 'no_producer', 'no_writer', 'no_language']
    valid_keys = ['valid_genre', 'valid_director', 'valid_starring', 'valid_producer', 'valid_writer', 'valid_language']

    with open('data/inspired/spider_result/movie_info_genre_reins2.jsonl', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in tqdm(lines):
            data = json.loads(line)
            lmkg_movieId2entities[data['movieId']] = {'Genre': list(), 'Directed_by': list(), 'Starring': list(),
                                                      'Produced_by': list(),
                                                      'Writer_by': list(), 'Language': list()}
            for i, key in enumerate(lmkg_movieId2entities[data['movieId']].keys()):
                temp = set()
                for item in data[key]:
                    # 当前实体必须 有效 且 不能是 no_xxxx
                    if item != no_keys[i] and item in valid_all_entity[valid_keys[i]]:
                        temp.add('<http://zcycqut.org/resource/' + item + '>')
                lmkg_movieId2entities[data['movieId']][key] = list(temp)
    with open('./data/inspired/kg/lmkg/lmkg_movieId2entities.pkl', 'wb') as f:
        pkl.dump(lmkg_movieId2entities, f)
    with open('./data/inspired/kg/lmkg/lmkg_entity2id.json', 'r', encoding='utf-8') as f:
        entity2id = json.load(f)
    # 电影id 对应的 电影实体
    with open('./data/inspired/kg/lmkg/lmkg_movieId2movieEntity.pkl', 'rb') as f:
        movie_entity = pkl.load(f)

    # 将电影id 应该含有的相关实体 转换为 实体id
    lmkg_moventityId2entitiesId = defaultdict(dict)
    for movieId, entities in lmkg_movieId2entities.items():
        if movieId in movie_entity and movie_entity[movieId] in entity2id:
            entityId = entity2id[movie_entity[movieId]]
            lmkg_moventityId2entitiesId[entityId] = dict()
            for key, entity_list in entities.items():
                if key == 'Language':
                    continue
                lmkg_moventityId2entitiesId[entityId][key] = []
                for entity in entity_list:
                    if entity in entity2id:
                        lmkg_moventityId2entitiesId[entityId][key].append(entity2id[entity])

    with open('./data/inspired/kg/lmkg/lmkg_moventityId2entitiesId.pkl', 'wb') as f:
        pkl.dump(lmkg_moventityId2entitiesId, f)


def extract_message_entity():
    """
    抽取ReDial对话中的电影实体和非电影
    :return:
    """
    # 电影id 对应的 电影实体
    with open('./data/inspired/kg/lmkg/lmkg_movieId2movieEntity.pkl', 'rb') as f:
        lmkg_movieId2movieEntity = pkl.load(f)

    # 电影id 对应的 非电影实体
    with open('./data/inspired/kg/lmkg/lmkg_movieId2entities.pkl', 'rb') as f:
        lmkg_movieId2entities = pkl.load(f)

    # 有效实体名
    with open('data/inspired/statistic/valid_all_entity.pkl', 'rb') as outf:
        valid_all_entity = pkl.load(outf)

    print("------------------  语句ID 对应的实体")
    message_entity = defaultdict(dict)
    for file_name in ['test', 'valid', 'train']:
        mesg_entity_counter = {'test': dict(), 'valid': dict(), 'train': dict()}
        rele_entity_counter = {'test': dict(), 'valid': dict(), 'train': dict()}
        with open('./data/redial/datasets/' + file_name + '_data_dbpedia.jsonl', 'r', encoding='utf-8') as f:
            mesg_entity = dict()
            rele_entity = dict()
            for line in tqdm(f):
                line = json.loads(line)
                for message in line['messages']:
                    entity = set()  # 人名实体
                    entity_name = set()
                    movie = set()  # 电影实体
                    movie_name = set()

                    # 判断当前话语里是否提及人名/类别/某种语言
                    # valid_all_entity={valid_director:[], }
                    for key, value in valid_all_entity.items():
                        for item in value:
                            if str(item).replace('_', ' ').lower() in str(message['text']).lower():
                                if key not in mesg_entity:
                                    mesg_entity[key] = 1
                                else:
                                    mesg_entity[key] += 1
                                entity.add('<http://zcycqut.org/resource/' + item + '>')

                    # 判断当前话语里是否有 电影ID@xxxx，若有则添加 电影实体以及相关的其他实体（导演、主演、制片、编剧、类别、语言），但这些实体（除了类别和语言的其他实体）每种类型最多添加一个
                    for movieId in re.findall(r'@\d+', message['text']):
                        if movieId[1:] in lmkg_movieId2movieEntity:
                            movie.add(lmkg_movieId2movieEntity[movieId[1:]])  # 添加电影实体
                            mov_name = lmkg_movieId2movieEntity[movieId[1:]].split('<http://zcycqut.org/resource/')
                            if len(mov_name) > 1:
                                movie_name.add(mov_name[1][:-1])  # 添加电影名

                            # # 为当前电影 添加相关实体
                            # if len(entity) < 2:
                            #     for key, entities in lmkg_movieId2entities[movieId[1:]].items():
                            #         if len(entities) > 0:
                            #             entity.add(entities[0])  # 添加非电影实体
                            #             if key not in rele_entity:
                            #                 rele_entity[key] = 1
                            #             else:
                            #                 rele_entity[key] += 1
                            #             if len(entity) >= 2:
                            #                 break
                    # 添加非电影实体名
                    for en in entity:
                        enti_name = en.split('<http://zcycqut.org/resource/')
                        if len(enti_name) > 1:
                            entity_name.add(enti_name[1][:-1].replace('_', ' '))

                    temp = {'entity': list(entity), 'entity_name': list(entity_name), 'movie': list(movie),
                            'movie_name': list(movie_name)}
                    message_entity[message['messageId']] = temp

            mesg_entity_counter[file_name] = mesg_entity
            rele_entity_counter[file_name] = rele_entity
        print("{} 话语中提及时加入的实体：{}".format(file_name, mesg_entity_counter[file_name]))
        print("{} 话语中加入的相关实体：{}".format(file_name, rele_entity_counter[file_name]))

    with open('./data/inspired/datasets/message_entity.pkl', 'wb') as f:
        pkl.dump(message_entity, f)


def process_datasets_redial_lmkg():
    """
    将 UniCRS 数据集里每轮对话的entity和movie进行替换（原本是根据DBpedia设计的，现在根据LMKG设计）
    :return:
    """
    # 提取电影ID 相关的实体
    extract_movie_entity()

    # 抽取语句中的实体
    extract_message_entity()

    with open('./data/redial/datasets/message_entity.pkl', 'rb') as f:
        message_entity = pkl.load(f)

    print("------------------  生成数据集文件")
    for filename in ['test', 'valid', 'train']:
        file = './data/redial/datasets/' + filename + '_data_lmkg.jsonl'
        if os.path.exists(file):
            os.remove(file)

        with open('./data/redial/datasets/' + filename + '_data_dbpedia.jsonl', 'r', encoding='utf-8') as src:
            for line in tqdm(src):
                # 加载原始数据
                line = json.loads(line)
                # 替换原始数据
                for i, message in enumerate(line['messages']):
                    if message['messageId'] in message_entity:
                        line['messages'][i]['entity'] = message_entity[message['messageId']]['entity']
                        line['messages'][i]['entity_name'] = message_entity[message['messageId']]['entity_name']
                        line['messages'][i]['movie'] = message_entity[message['messageId']]['movie']
                        line['messages'][i]['movie_name'] = message_entity[message['messageId']]['movie_name']
                # 保存新的数据
                with open(file, 'a', encoding='utf-8') as tgt:
                    tgt.write(json.dumps(line, ensure_ascii=False) + '\n')


def process_datasets_inspired_lmkg():
    """
    处理inspired数据集，将其构造成redial数据集格式。
    由于inspired里并没有movie_id，需要重新映射inspired里面的电影。
    方法：先根据inspired里每个对话的'movie'字段去信息库里找是否存在，若存在则直接将其映射为信息库里的movie_id
        若不存在，再利用信息库里的电影名称去对话里进行单词匹配，判断每个句子里是否出现过 信息库里的电影名
    :return:
    """
    # extract_message_entity()

    key2index = json.load(open('data/redial/kg/conceptnet/key2index_3rd.json', encoding='utf-8'))
    stopwords = set([word.strip() for word in open('data/redial/kg/conceptnet/stopwords.txt', encoding='utf-8')])
    keyword_sets = set(key2index.keys()) - stopwords

    mid2men = pkl.load(open('data/inspired/kg/lmkg/lmkg_movieId2movieEntity.pkl', 'rb'))
    mid2en = pkl.load(open('data/inspired/kg/lmkg/lmkg_movieId2entities.pkl', 'rb'))

    with open('data/inspired/kg/lmkg/lmkg_entity2id.json', 'r') as f:
        en2id = json.load(f)
    # 把非电影实体找出来，并去掉URI链接，只保留实体名字
    no_men_entity = []
    men = mid2en.values()
    base = '<http://zcycqut.org/resource/'
    for k in en2id.keys():
        if not k: continue
        if k not in men and base in k:
            temp = k.split(base)[1][:-1]
            if not temp:
                continue
            no_men_entity.append(temp)

    movie2ids = dict() # 电影名到id
    movie2ids2 = dict() # 去掉时间的电影名到id
    with open('data/inspired/spider_result/movie_info_genre_reins2.jsonl', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in tqdm(lines):
            data = json.loads(line)
            movie2ids[data['movieName']] = data['movieId']
            res = '_'.join(data['movieName'].split('_')[:-1]) if '_(' in data['movieName'] else data['movieName']
            movie2ids2[res] = data['movieId']


    root_dir = './data/inspired/datasets'
    file_path_list = ['train', 'dev', 'test']
    have_movie = set()
    no_movie = set()
    result = []
    for p in file_path_list:
        dataset = []
        with open(os.path.join(root_dir, p + '.jsonl'), 'r', encoding='utf-8') as f:
            for line in f.readlines():
                dataset.append(json.loads(line))

            for data in tqdm(dataset):
                movieMentions = dict() # 记录一个训练样本里所有被提及的电影
                messages = []
                initiatorWorkerId = 0
                respondentWorkerId = 1  # RECOMMENDER
                for d in data:
                    moen_list = []
                    en_list = []
                    # 根据每个句子里的'movie'字段
                    for m in d['movie']:
                        m1 = '_'.join(m.split(' ')) # 原始字段的样子
                        m2 = '_'.join(m.split(' ')[:-1]) if '_(' in m1 else m1 # 去掉电影时间的样子
                        # 判断是否提及
                        if m1 not in movieMentions:
                            if m1 in movie2ids:
                                movieMentions[movie2ids[m1]] = m1
                                if movie2ids[m1] in mid2men and mid2men[movie2ids[m1]] not in moen_list:
                                    moen_list.append(mid2men[movie2ids[m1]])
                                if movie2ids[m1] in mid2en:
                                    for k, v in mid2en[movie2ids[m1]].items():
                                        ct = 2
                                        for v2 in v:
                                            if v2 not in en_list and ct > 0:
                                                en_list.append(v2)
                                                ct -= 1

                                have_movie.add(m1)
                            elif m2 in movie2ids2:
                                movieMentions[movie2ids2[m2]] = m2
                                if movie2ids2[m2] in mid2men and mid2men[movie2ids2[m2]] not in moen_list:
                                    moen_list.append(mid2men[movie2ids2[m2]])
                                if movie2ids2[m2] in mid2en:
                                    for k, v in mid2en[movie2ids2[m2]].items():
                                        for v2 in v:
                                            if v2 not in en_list:
                                                en_list.append(v2)
                                have_movie.add(m2)
                            else:
                                no_movie.add(m1)
                    if not en_list:
                        for entity in no_men_entity:
                            if str(entity).replace('_', ' ').lower() in str(d['text']).lower():
                                en_data = '<http://zcycqut.org/resource/' + entity + '>'
                                if en_data not in en_list:
                                    en_list.append(en_data)

                    key_word = set()
                    for word in str(d['text']).split(' '):
                        if word in keyword_sets:
                            key_word.add(word)

                    mes = dict()
                    mes['text'] = d['text']
                    mes['movie'] = moen_list
                    mes['entity'] = en_list
                    mes['key_word'] = list(key_word)
                    mes['senderWorkerId'] = 0 if d['role'] != 'RECOMMENDER' else 1
                    messages.append(mes)
                result.append({'movieMentions': movieMentions,
                               'messages': messages,
                               'respondentWorkerId': respondentWorkerId,
                               'initiatorWorkerId': initiatorWorkerId})

        with open(os.path.join(root_dir, p + '_data_lmkg_concept.jsonl'), 'w', encoding='utf-8') as f:
            for line in result:
                f.write(json.dumps(line, ensure_ascii=False) + '\n')
    print(len(have_movie))
    print(len(no_movie))
    print(no_movie)


if __name__ == '__main__':
    construct_kg('./data/inspired/spider_result/movie_info_genre_reins2.jsonl')
    process_datasets_inspired_lmkg()