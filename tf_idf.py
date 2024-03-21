import re
from collections import Counter

import nltk
import numpy as np
import pandas as pd
from nltk.corpus import stopwords
from pymystem3 import Mystem
from math import ceil

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
        # Удаляем HTML-теги
        text = re.sub(r'<[^>]+>', ' ', text)
        # Удаляем все символы, не являющиеся буквами
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


async def get_all_scores(db_data: dict) -> dict:
    """
    Асинхронная функция для подсчёта TF (частоты слов) в исходном и целевых текстах,
    учитывая лемматизацию слов и исключая стоп-слова и не-слова.
    """

    def cleanup_text(text):
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
    lemmas_medians = dict()
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

    # Добавить
    less_then_rivals = df.sort_values(by='diff', ascending=False)
    less_then_rivals = less_then_rivals[(less_then_rivals['source_freq'] > 0) & (less_then_rivals['diff'] > 0)]
    less_then_rivals = less_then_rivals.merge(max_counts_per_lemma, how='left', on='source_word')
    less_then_rivals = less_then_rivals[:20]
    less_then_rivals = less_then_rivals[['word', 'diff']]
    less_then_rivals['diff'] = less_then_rivals['diff'].apply(ceil)

    # Переспам
    more_then_rivals = df.sort_values(by='diff', ascending=True)
    more_then_rivals = more_then_rivals[(more_then_rivals['target_freq'] > 0) & (more_then_rivals['diff'] < 0)]
    more_then_rivals = more_then_rivals.merge(max_counts_per_lemma, how='left', on='source_word')
    more_then_rivals = more_then_rivals[:20]
    more_then_rivals = more_then_rivals[['source_word', 'diff']].rename(columns={'source_word': 'word'})
    more_then_rivals['diff'] = more_then_rivals['diff'].apply(abs).apply(ceil)

    # Удалить
    for_deletion = df.sort_values(by='diff', ascending=True)
    for_deletion = for_deletion[for_deletion['target_freq'] == 0]
    for_deletion = for_deletion[:20]
    for_deletion = for_deletion[['source_word', 'diff']].rename(columns={'source_word': 'word'})
    for_deletion['diff'] = for_deletion['diff'].apply(abs).apply(ceil)

    # Находим слова, которых нет в исходном тексте, но есть в целевых
    missing_words = set(all_lemmas) - set(source_lemmas)
    missing_words_data = []
    for word in missing_words:
        missing_words_data.append({"source_word": word, "source_freq": 0,
                                   "target_word": word, "target_freq": lemmas_medians[word]})
    missing_words_df = pd.DataFrame(missing_words_data)
    missing_words_df['diff'] = missing_words_df['target_freq'] - missing_words_df['source_freq']
    missing_words_df = missing_words_df.merge(max_counts_per_lemma, how='left', on='source_word')
    missing_words_df = missing_words_df[missing_words_df['target_freq'] > 0]
    missing_words_df = missing_words_df.sort_values('diff', ascending=False)
    missing_words_df = missing_words_df[['word', 'diff']]
    missing_words_df['diff'] = missing_words_df['diff'].apply(ceil)

    # Объединяем результаты
    results = {
        'lsi': missing_words_df,
        'add': less_then_rivals,
        'remove': more_then_rivals,
        'delete': for_deletion
    }

    return results
