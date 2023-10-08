import json

from tqdm import tqdm


def add_conword2redial():
    key2index = json.load(open('data/redial/kg/conceptnet/key2index_3rd.json', encoding='utf-8'))
    stopwords = set([word.strip() for word in open('data/redial/kg/conceptnet/stopwords.txt', encoding='utf-8')])
    keyword_sets = set(key2index.keys()) - stopwords

    for filename in ['test', 'valid', 'train']:
        file = './data/redial/datasets/' + filename + '_data_lmkg_concept.jsonl'
        with open('./data/redial/datasets/' + filename + '_data_lmkg.jsonl', 'r', encoding='utf-8') as src:
            for line in tqdm(src):
                # 加载原始数据
                line = json.loads(line)
                # 记录关键词
                for i, message in enumerate(line['messages']):
                    key_word = set()
                    for word in str(message['text']).split(' '):
                        if word in keyword_sets:
                            key_word.add(word)
                        line['messages'][i]['key_word'] = list(key_word)

                # 保存新的数据
                with open(file, 'a', encoding='utf-8') as tgt:
                    tgt.write(json.dumps(line, ensure_ascii=False) + '\n')

if __name__ == '__main__':
    add_conword2redial()