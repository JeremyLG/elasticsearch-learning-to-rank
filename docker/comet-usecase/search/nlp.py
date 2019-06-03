# import spacy
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
import nltk
from nltk.tokenize import word_tokenize
from processing import elasticsearch_easy_read_pandas
import numpy as np
import logging

from config import FIELD_TO_EMBED, SEARCHING_INDEX, RETRAIN_MODEL, MODEL_PATH

nltk.download('punkt')

logger = logging.getLogger(__name__)
gensim_logger = logging.getLogger('gensim')
gensim_logger.setLevel(logging.WARNING)


def split_train_dev_elasticsearch():
    df = elasticsearch_easy_read_pandas(SEARCHING_INDEX)
    msk = np.random.rand(len(df)) < 0.8
    return df[msk].reset_index(drop=True), df[~msk].reset_index(drop=True)


def tokenize(df):
    data = df.loc[:, FIELD_TO_EMBED].values
    return [TaggedDocument(words=word_tokenize(_d.lower()),
                           tags=[str(i)]) for i, _d in enumerate(data)]


def init_model(tagged_data):
    max_epochs = 200
    vec_size = 200
    alpha = 0.025

    model = Doc2Vec(vector_size=vec_size,
                    alpha=alpha,
                    min_alpha=0.00025,
                    min_count=1,
                    dm=1)

    logging.info("Building vocab")
    model.build_vocab(tagged_data)

    logging.info("Starting to train")
    for epoch in range(max_epochs):
        logging.info('iteration {0}'.format(epoch))
        model.train(tagged_data,
                    total_examples=model.corpus_count,
                    epochs=model.epochs)
        # decrease the learning rate
        model.alpha -= 0.0002
        # fix the learning rate, no decay
        model.min_alpha = model.alpha
    return model


def save_model(model, path=MODEL_PATH):
    model.save(path)


def load_model(path=MODEL_PATH):
    model = Doc2Vec.load(path)
    return model


def inference(model, data):
    # to find the vector of a document which is not in training data
    test_data = word_tokenize(data.lower())
    v1 = model.infer_vector(test_data)
    return v1


def most_similar(model, tag):
    # to find most similar doc using tags
    try:
        similar_doc = model.docvecs.most_similar(tag)
    except TypeError:
        similar_doc = model.docvecs.most_similar(positive=[tag])
    return(similar_doc)


def return_vector(model, tag):
    # to find vector of doc in training data using tags or in other words, printing the vector of
    # document at index 1 in training data
    return model.docvecs[tag]


def logging_column_similars(similars, training_frame):
    for (tag, similarity) in similars:
        logging.info(f"Similarity of {similarity}")
        logging.info(training_frame.loc[int(tag), FIELD_TO_EMBED])


def main(retrain=RETRAIN_MODEL):
    np.random.seed(1337)
    # nlp = spacy.load("fr_core_news_sm")
    train, dev = split_train_dev_elasticsearch()
    if retrain:
        tagged_data = tokenize(train)
        model = init_model(tagged_data)
        save_model(model)
    model = load_model()
    index = 0
    tagged_example = dev.iloc[index][FIELD_TO_EMBED]
    inferred_vector_dev = inference(model, tagged_example)
    tag = '4'
    most_similar(model, tag)
    similars = most_similar(model, inferred_vector_dev)
    return_vector(model, tag)
    logging.info(f"Most similar results for {dev.iloc[index]['title']}")
    logging.info(dev.iloc[index][FIELD_TO_EMBED])
    logging_column_similars(similars, train)
    # nlp.add


if __name__ == '__main__':
    main()
