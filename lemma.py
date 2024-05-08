from joblib import delayed, Parallel

from spacy.lang.ru.stop_words import STOP_WORDS
import spacy_cleaner
from spacy_cleaner.processing import removers, replacers, mutators, remove_number_token, transformers, evaluators
import spacy
from string import punctuation
STOP_WORDS = STOP_WORDS

# Загрузка русскоязычной модели
nlp = spacy.load('ru_core_news_sm')

import pandas as pd
texts = pd.read_csv('db_request_data.csv')['content'].astype(str).to_list()


def lemmatize_text(text):
    pipeline = spacy_cleaner.Cleaner(
        nlp,
        removers.remove_stopword_token,
        remove_number_token,
        removers.remove_punctuation_token,
        mutators.mutate_lemma_token
    )

    return pipeline.clean(text)


def flatten(list_of_lists):
    "Flatten a list of lists to a combined list"
    return [item for sublist in list_of_lists for item in sublist]


def process_chunk(texts):
    preproc_pipe = []
    for doc in nlp.pipe(texts, batch_size=20):
        preproc_pipe.append(lemmatize_text([doc]))
    return preproc_pipe


def preprocess_parallel(texts):
    executor = Parallel(n_jobs=3, backend='multiprocessing', prefer="processes")
    do = delayed(process_chunk)
    # task = do(texts)
    result = executor(process_chunk(texts))

    return result


preprocess_parallel(texts)