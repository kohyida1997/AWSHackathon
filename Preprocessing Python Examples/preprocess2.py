import nltk
from nltk.stem.porter import *
from nltk.stem import WordNetLemmatizer
import re
import pandas as pd

stop_list = nltk.corpus.stopwords.words('english')
stop_list += ['stayed','stay','got', 'went','said','say','told','hotel','asked', 'called']

import gensim

def load_corpus(dir):
    # dir is a directory with plain text files to load.
    corpus = nltk.corpus.PlaintextCorpusReader(dir, '.+\.txt')
    return corpus

def corpus2docs(corpus):
    # corpus is a object returned by load_corpus that represents a corpus.
    fids = corpus.fileids()
    docs1 = []
    for fid in fids:
        doc_raw = corpus.raw(fid)
        doc = nltk.word_tokenize(doc_raw)
        docs1.append(doc)
    docs2 = [[w.lower() for w in doc] for doc in docs1]
    docs3 = [[w for w in doc if re.search('^[a-z]+$', w)] for doc in docs2]
    docs4 = [[w for w in doc if w not in stop_list] for doc in docs3]
    return docs4

def showCorpusIDs(corpus):
    for (c, x) in enumerate(corpus.fileids()):
        print("Doc {}: {}\n".format(c, x))

def docs2vecs(docs, dictionary):
    # docs is a list of documents returned by corpus2docs.
    # dictionary is a gensim.corpora.Dictionary object.
    vecs1 = [dictionary.doc2bow(doc) for doc in docs]
    return vecs1



def reviewsToDocs(csvFilePath, columnName):
    dat = pd.read_csv(csvFilePath)[columnName]
    print("Read {} rows from {}\n".format(len(dat), csvFilePath))
    return list(dat)
    
def reviewsToDocsRatingsLessThan(csvFilePath, columnName, threshold):
    dat = pd.read_csv(csvFilePath)
    dat = dat[dat['Rating'] <= threshold]
    dat = dat[columnName]
    print("Read {} rows from {} where Rating <= {}\n".format(len(dat), csvFilePath, str(threshold)))
    return list(dat)
    
def reviewsToDocsRatingsMoreThan(csvFilePath, columnName, threshold):
    dat = pd.read_csv(csvFilePath)
    dat = dat[dat['Rating'] >= threshold]
    dat = dat[columnName]
    print("Read {} rows from {} where Rating >= {}\n".format(len(dat), csvFilePath, str(threshold)))
    return list(dat)    
    
def fixFullStops(string):
    # Fix cases such as "Today is Monday.Tomorrow is"
    # Insert space after full stop to become "Today is Monday. Tomorrow is"
    x = 0
    ll = len(string)
    while x < ll:
        if x == ll - 1:
            break
        if string[x] == '.' and string[x + 1] != ' ':
            ll += 1
            string = '{} {}'.format(string[:x+1], string[x+1:])
        x += 1
    return string
    
def stemNormalize(corpus):
    stemmer = PorterStemmer()
    docs1 = []
    for doc in corpus:
        doc = nltk.word_tokenize(fixFullStops(doc))
        docs1.append(doc)
    docs2 = [[w.lower() for w in doc] for doc in docs1]
    docs3 = [[w for w in doc if re.search('^[a-z]+$', w)] for doc in docs2]
    docs4 = [[stemmer.stem(w) for w in doc if w not in stop_list] for doc in docs3]
    return docs4
    
def lemNormalize(corpus):
    lem = WordNetLemmatizer()
    docs1 = []
    for doc in corpus:
        doc = nltk.word_tokenize(fixFullStops(doc))
        docs1.append(doc)
    docs2 = [[w.lower() for w in doc] for doc in docs1]
    docs3 = [[w for w in doc if re.search('^[a-z]+$', w)] for doc in docs2]
    docs4 = [[lem.lemmatize(w) for w in doc if w not in stop_list] for doc in docs3]
    return docs4


def get_hdp_topics(hdp, top_n=10):
    '''Wrapper function to extract topics from trained tomotopy HDP model 
    
    ** Inputs **
    hdp:obj -> HDPModel trained model
    top_n: int -> top n words in topic based on frequencies
    
    ** Returns **
    topics: dict -> per topic, an arrays with top words and associated frequencies 
    '''
    
    # Get most important topics by # of times they were assigned (i.e. counts)
    sorted_topics = [k for k, v in sorted(enumerate(hdp.get_count_by_topics()), key=lambda x:x[1], reverse=True)]

    topics=dict()
    
    # For topics found, extract only those that are still assigned
    for k in sorted_topics:
        if not hdp.is_live_topic(k): continue # remove un-assigned topics at the end (i.e. not alive)
        topic_wp =[]
        for word, prob in hdp.get_topic_words(k, top_n=top_n):
            topic_wp.append((word, prob))

        topics[k] = topic_wp # store topic word/frequency array
        
    return topics


def eval_coherence(topics_dict, word_list, coherence_type='c_v'):
    '''Wrapper function that uses gensim Coherence Model to compute topic coherence scores
    
    ** Inputs **
    topic_dict: dict -> topic dictionary from train_HDPmodel function
    word_list: list -> lemmatized word list of lists
    coherence_typ: str -> type of coherence value to comput (see gensim for opts)
    
    ** Returns **
    score: float -> coherence value
    '''
    
    # Build gensim objects
    vocab = corpora.Dictionary(word_list)
    corpus = [vocab.doc2bow(words) for words in word_list]
    
    # Build topic list from dictionary
    topic_list=[]
    for k, tups in topics_dict.items():
        topic_tokens=[]
        for w, p in tups:
            topic_tokens.append(w)
            
        topic_list.append(topic_tokens)
            

    # Build Coherence model
    print("Evaluating topic coherence...")
    cm = CoherenceModel(topics=topic_list, corpus=corpus, dictionary=vocab, texts=word_list, 
                    coherence=coherence_type)
    
    score = cm.get_coherence()
    print ("Done\n")
    return score