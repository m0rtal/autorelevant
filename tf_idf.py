import string

import numpy as np
from collections import Counter
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd
import nltk

nltk.download('stopwords')
from nltk.corpus import stopwords

russian_stopwords = stopwords.words('russian')
russian_stopwords.extend(['руб', 'шт', 'http', 'ул', 'ru', 'пн', 'вт', 'ср', 'чт', 'пт', 'сб', 'вс'])


async def get_idf_scores(db_data):
    source_text = db_data.get('parsed_data').get('content')
    target_texts = [record.get('content') for record in db_data.get('parsed_search_data')]

    # Инициализируем TfidfVectorizer
    vectorizer = TfidfVectorizer(stop_words=russian_stopwords)  # , ngram_range=(1, 3)
    tfidf_matrix_target = vectorizer.fit_transform(target_texts)
    tfidf_matrix_source = vectorizer.transform([source_text])

    # Получаем названия признаков (n-gramm)
    features = vectorizer.get_feature_names_out()

    # Вычисляем средние значения TF-IDF для каждого признака
    tfidf_means_source = np.mean(tfidf_matrix_source, axis=0)
    tfidf_means_target = np.mean(tfidf_matrix_target, axis=0)

    # Формируем итоговую таблицу
    df_tfidf = pd.DataFrame(
        {'n-gramm': features, 'source_score': tfidf_means_source.A1, 'target_score': tfidf_means_target.A1})
    # df_tfidf['difference'] = df_tfidf['source_score'] - df_tfidf['target_score']

    # Сортируем по разнице в пользу сторонних сайтов
    df_tfidf_sorted = df_tfidf.sort_values(by='target_score', ascending=False)
    df_tfidf_sorted['search_query'] = db_data.get('parsed_data').get('h1')

    def process_text(text):
        """
        Функция для предобработки текста: приведение к нижнему регистру и удаление пунктуации.
        """
        text = text.lower()
        return text.translate(str.maketrans('', '', string.punctuation))

    source_word_count = Counter(process_text(source_text).split())
    all_words_count = Counter()
    total_texts = len(target_texts)

    for text in target_texts:
        all_words_count += Counter(process_text(text).split())

    # Вычисляем среднюю частотность
    average_frequency = {word: count / total_texts for word, count in all_words_count.items()}

    # Добавляем встречаемость слов в оригинальном и целевых документах
    df_tfidf_sorted['source_freq'] = df_tfidf_sorted['n-gramm'].apply(lambda x: source_word_count.get(x, 0))
    df_tfidf_sorted['target_freq'] = df_tfidf_sorted['n-gramm'].apply(lambda x: average_frequency.get(x, 0))

    df_tfidf_sorted = df_tfidf_sorted.query("source_freq != 0 and target_freq != 0")

    return df_tfidf_sorted
