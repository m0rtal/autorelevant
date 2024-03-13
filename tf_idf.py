import re
from collections import Counter

import nltk
import numpy as np
import pandas as pd
from nltk.corpus import stopwords
from pymystem3 import Mystem

mystem = Mystem()

# Загружаем набор стоп-слов для русского языка
nltk.download('punkt')
nltk.download('stopwords')
STOP_WORDS = stopwords.words('russian')


async def get_tf_scores(db_data: dict) -> dict:
    """
    Асинхронная функция для подсчёта TF (частоты слов) в исходном и целевых текстах,
    учитывая лемматизацию слов и исключая стоп-слова и не-слова.
    """

    def cleanup_text(text):
        text = re.sub(r"[^а-яА-ЯёЁa-zA-Z]+", " ", text)
        tokens = nltk.word_tokenize(text)
        filtered_tokens = [token.lower() for token in tokens if token.lower() not in STOP_WORDS and len(token) >= 3]
        return ' '.join(filtered_tokens)

    def lemmatize(text):
        lemmas = mystem.lemmatize(text)
        return [lemma.lower() for lemma in lemmas if lemma not in STOP_WORDS and lemma.strip()]

    source_text = db_data.get('parsed_data').get('content')
    target_texts = [record.get('content') for record in db_data.get('parsed_search_data')]

    # Лемматизация и очистка исходного текста
    source_lemmas = lemmatize(cleanup_text(source_text))
    source_counter = Counter(source_lemmas)

    target_lemmas_list = []
    all_words = []
    for text in target_texts:
        text = cleanup_text(text)
        lemmas = lemmatize(text)
        target_lemmas_list.append(lemmas)

        words = [word.lower() for word in text.split() if
                 mystem.lemmatize(word.lower())[0] not in STOP_WORDS and word.lower().strip()]
        all_words.extend(words)

    words_counts = Counter(all_words)
    all_words_df = pd.DataFrame.from_dict(words_counts, orient='index').reset_index()
    all_words_df.columns = ['word', 'count']
    all_words_df['source_word'] = all_words_df['word'].apply(lambda word: mystem.lemmatize(word)[0])
    max_counts_per_lemma = all_words_df.loc[all_words_df.groupby('source_word')['count'].idxmax()]

    # Собираем все слова из всех текстов для определения уникальных слов
    all_lemmas = set(source_lemmas)
    for lemmas in target_lemmas_list:
        all_lemmas.update(lemmas)

    # Считаем медиану для каждого слова в целевых текстах
    lemmas_medians = {}
    for lemma in all_lemmas:
        counts = []
        for target_lemmas in target_lemmas_list:
            counts.append(target_lemmas.count(lemma))
        lemmas_medians[lemma] = np.median(counts)

    # Создаем список для DataFrame
    data_for_df = []
    for word in all_lemmas:
        source_freq = source_counter.get(word, 0)
        target_median = lemmas_medians[word]
        data_for_df.append({"source_word": word, "source_freq": source_freq,
                            "target_word": word, "target_freq": target_median})

    # Создаем DataFrame
    df = pd.DataFrame(data_for_df)
    df['diff'] = df['target_freq'] - df['source_freq']
    df = df.sort_values(by='diff', ascending=False)
    df = df.merge(max_counts_per_lemma, how='left', on='source_word')
    df = df[:20]

    return df
