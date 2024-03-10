import re  # Для работы с регулярными выражениями
from collections import Counter

import numpy as np
import pandas as pd
from pymystem3 import Mystem

mystem = Mystem()

# Примерный список стоп-слов, может быть расширен или изменен
STOP_WORDS = set([
    "и", "в", "во", "не", "что", "он", "на", "я", "с", "со", "как",
    "а", "то", "все", "она", "так", "его", "но", "да", "ты", "к", "у",
    "же", "вы", "за", "бы", "по", "только", "ее", "мне", "было",
    "вот", "от", "меня", "еще", "нет", "о", "из", "ему", "теперь",
    "когда", "даже", "ну", "вдруг", "ли", "если", "уже", "или", "ни",
    "быть", "был", "него", "до", "вас", "нибудь", "опять", "уж",
    "вам", "ведь", "там", "потом", "себя", "ничего", "ей", "может",
    "они", "тут", "где", "есть", "надо", "ней", "для", "мы", "тебя",
    "их", "чем", "была", "сам", "чтоб", "без", "будто", "чего", "раз",
    "тоже", "себе", "под", "будет", "ж", "тогда", "кто", "этот",
    "того", "потому", "этого", "какой", "совсем", "ним", "здесь",
    "этом", "один", "почти", "мой", "тем", "чтобы", "нее", "сейчас",
    "были", "куда", "зачем", "всех", "никогда", "можно", "при",
    "наконец", "два", "об", "другой", "хоть", "после", "над", "больше",
    "тот", "через", "эти", "нас", "про", "всего", "них", "какая",
    "много", "разве", "три", "эту", "моя", "впрочем", "хорошо", "свою",
    "этой", "перед", "иногда", "лучше", "чуть", "том", "нельзя",
    "такой", "им", "более", "всегда", "конечно", "всю", "между", "л"
])


async def get_tf_scores(db_data: dict) -> dict:
    """
    Асинхронная функция для подсчёта TF (частоты слов) в исходном и целевых текстах,
    учитывая лемматизацию слов и исключая стоп-слова и не-слова.
    """
    source_text = db_data.get('parsed_data').get('content')
    target_texts = [record.get('content') for record in db_data.get('parsed_search_data')]

    def clean_and_lemmatize(text):
        # Удаляем не-слова
        text = re.sub(r"[^а-яА-ЯёЁa-zA-Z]+", " ", text)
        # Лемматизация
        lemmas = mystem.lemmatize(text)
        # Удаляем стоп-слова и пустые строки после лемматизации
        return [lemma.lower() for lemma in lemmas if lemma not in STOP_WORDS and lemma.strip()]

    # Лемматизация и очистка исходного текста
    source_lemmas = clean_and_lemmatize(source_text)
    source_counter = Counter(source_lemmas)

    target_lemmas_list = []
    all_words = []
    for text in target_texts:
        lemmas = clean_and_lemmatize(text)
        target_lemmas_list.append(lemmas)
        text = re.sub(r"[^а-яА-ЯёЁa-zA-Z]+", " ", text)
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
