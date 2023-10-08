import json
import os.path
import pickle as pkl
from tqdm import tqdm


def load_dataset(file_path):
    dataset = []
    with open(file_path+'.jsonl', 'r', encoding='utf-8') as f:
        for line in f.readlines():
            dataset.append(json.loads(line))
    return dataset


def gen_inspired_dataset(dataset, file_path):
    mid2en = pkl.load(open('data/redial/kg/lmkg/lmkg_movieId2entities.pkl', 'rb'))
    mid2moen = pkl.load(open('data/redial/kg/lmkg/lmkg_movieId2movieEntity.pkl', 'rb'))
    movie2ids = dict()
    movie2ids2 = dict()
    with open('data/inspired/spider_result/movie_info_genre_reins.jsonl', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in tqdm(lines):
            data = json.loads(line)
            movie2ids[data['movieName']] = data['movieId']
            res = '_'.join(data['movieName'].split('_')[:-1]) if '_(' in data['movieName'] else data['movieName']
            movie2ids2[res] = data['movieId']

    with open('data/redial/statistic/valid_all_entity.pkl', 'rb') as outf:
        valid_all_entity = pkl.load(outf)

    key2index = json.load(open('data/redial/kg/conceptnet/key2index_3rd.json', encoding='utf-8'))
    stopwords = set([word.strip() for word in open('data/redial/kg/conceptnet/stopwords.txt', encoding='utf-8')])
    keyword_sets = set(key2index.keys()) - stopwords

    result = []
    no_movie = set()
    have_movie = set()
    for data in tqdm(dataset):
        movieMentions = dict()
        messages = []
        initiatorWorkerId = 0
        respondentWorkerId = 1  # RECOMMENDER
        for d in data:
            moen_list = []
            en_list = []
            for m in d['movie']:
                if not m:
                    continue
                m1 = '_'.join(m.split(' '))
                m2 = '_'.join(m.split(' ')[:-1]) if '_(' in m1 else m1
                if m1 not in movieMentions:
                    if m1 in movie2ids:
                        movieMentions[movie2ids[m1]] = m1
                        if movie2ids[m1] in mid2moen and mid2moen[movie2ids[m1]] not in moen_list:
                            moen_list.append(mid2moen[movie2ids[m1]])
                        if movie2ids[m1] in mid2en:
                            for k, v in mid2en[movie2ids[m1]].items():
                                for v2 in v:
                                    if v2 not in en_list:
                                        en_list.append(v2)
                        have_movie.add(m1)
                    elif m2 in movie2ids2:
                        movieMentions[movie2ids2[m2]] = m2
                        if movie2ids2[m2] in mid2moen and mid2moen[movie2ids2[m2]] not in moen_list:
                            moen_list.append(mid2moen[movie2ids2[m2]])
                        have_movie.add(m2)
                    else:
                        # movieMentions[m1] = -1
                        no_movie.add(m1)

            for key, value in valid_all_entity.items():
                for item in value:
                    if str(item).replace('_', ' ').lower() in str(d['text']).lower():
                        en_data = '<http://zcycqut.org/resource/' + item + '>'
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


    with open(file_path + '_data_lmkg_concept.jsonl', 'w', encoding='utf-8') as f:
        for line in result:
            f.write(json.dumps(line, ensure_ascii=False)+'\n')
    return dataset

def get_notinlmkg_movie(file_path):
    movie2ids = dict()
    movie2ids2 = dict()
    with open('./data/redial/spider_result/movie_info_genre.jsonl', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in tqdm(lines):
            data = json.loads(line)
            movie2ids[data['movieName']] = data['movieId']
            res = '_'.join(data['movieName'].split('_')[:-1]) if '_(' in data['movieName'] else data['movieName']
            movie2ids2[res] = data['movieId']

    movieId_index = 9000000
    movieMentions = dict()
    exist_movieMentions = dict()
    root_dir = './data/inspired/datasets'
    file_path_list = ['train', 'dev', 'test']
    for p in file_path_list:
        dataset = load_dataset(os.path.join(root_dir, p))
        for data in tqdm(dataset):
            for d in data:
                for m in d['movie']:
                    if not m:
                        continue
                    m1 = '_'.join(m.split(' '))
                    m2 = '_'.join(m.split(' ')[:-1]) if '_(' in m1 else m1
                    if m1 in movie2ids or m2 in movie2ids2:
                        if m1 not in exist_movieMentions:
                            exist_movieMentions[m1] = 1
                        else:
                            exist_movieMentions[m1] += 1
                    else:
                        if m1 not in movieMentions:
                            movieMentions[m1] = {'movieId': str(movieId_index), 'movieName': m1, 'frequency': 1}
                            movieId_index += 1
                        else:
                            movieMentions[m1]['frequency'] += 1
    movieMentions = dict(sorted(movieMentions.items(), key=lambda x: x[1]['frequency'], reverse=True))
    result = [v for v in movieMentions.values()]
    with open(file_path, 'w', encoding='utf-8') as f:
        for data in result:
            f.write(json.dumps(data, ensure_ascii=False)+'\n')

if __name__ == '__main__':
    root_dir = './data/inspired/datasets'
    file_path_list = ['train', 'dev', 'test']
    for p in file_path_list:
        dataset = load_dataset(os.path.join(root_dir, p))
        gen_inspired_dataset(dataset, os.path.join(root_dir, p))
    # get_notinlmkg_movie('data/redial/spider_result/movie_id_name_fre.jsonl')