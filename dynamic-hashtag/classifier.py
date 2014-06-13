"""
This is an implementation of the adaptive twitter filtering system
described in "Adaptive Method for Following Dynamic Topics on Twitter" by
Walid Magdy and Tamer Elsayed from the Qatar Computing Research Institute
in Doha.
"""
import heapq
import MySQLdb

from .config import (MYSQL_DB, MYSQL_HOST, MYSQL_PWD, MYSQL_USER,
    TOP_K, LANGUAGE, CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN,
    ACCESS_TOKEN_SECRET, REMODEL)


from collections import namedtuple
from datetime import timedelta, datetime
from nltk import word_tokenize
from nltk.corpus import stopwords
from math import log
from sklearn import svm
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# used to efficently compute tf_idf
TweetCount = namedtuple('tweet_count', ['frequency', 'doc_frequency'])

class AdaptiveTweetClassifierTrainer(object):
    """
    Parameters:
    @param window_size: Size of the window used for T_b,
    the positive set of tweets to train the classifier (in hours)
    @param period: How often retrain the classifier
    @param language: ['english', 'french', 'german']
    """

    # We have tweet_window (T_w), the tweets in window w

    query = """SELECT user, tweet FROM tweets WHERE created_at > %s"""

    def __init__(self, init_users, init_keywords):
        """
        Initialization of the classifier. Give initial
        keywords and user handles to initate the process
        of tweet collection.
        """
        for lang in LANGUAGE:
            self.stop_words.extend(stopwords(self.language))

    def get_pos_tweets(self):
        """
        Read tweets from a mysql database
        """
        connection = MySQLdb.connect(
            host=MYSQL_HOST, passwd=MYSQL_PWD,
            user=MYSQL_USER, db=MYSQL_DB)
        cursor = connection.cursor()

        date = datetime.now() - timedelta(hours=REMODEL)

        cursor.execute(self.query, date)
        tweets = cursor.fetchall()
        cursor.close()
        connection.close()

        return tweets

    def pos_term_selection(self):
        """
        Get a positive sample of tweets. Tweets are stored
        in the twitter json format.
        1) Tokenize tweet
        2) Remove stopwords
        3) add to dict or count + 1
        """

        word_count = dict(int)

        def _doc_freq(term):
            word_count[term]['doc_frequency'] += 1
            return

        pos_tweets = self.get_pos_tweets()
        for tweet in pos_tweets:
            text = tweet['text']
            text_tokenized = word_tokenize(text)
            for term in text_tokenized:
                if term in self.stop_words:
                    continue
                if term in word_count:
                    word_count[term] += 1
                else:
                    word_count[term] = TweetCount(1, 0)

            map(_doc_freq, set(text_tokenized))

        self.query_set = [x for x in word_count if word_count[x] > 10]

    def filter_negative(self):
        """
        Get a negative sample of tweets (filtering with
        the exclusion terms)
        """
        sample = self.get_random_sample()

        sample_tweets = []

        p_rel_terms = self.term_selection()

        for tweet in sample:
            text = tweet['text']
            text_tokenized = set(word_tokenize(text))
            if text_tokenized.intersection(p_rel_terms):
                sample_tweets.append(tweet)

        # create feature and target vectors
        feature_tweets = []
        for tweet in sample_tweets:
            features = self.get_feature_vector(tweet)
            feature_tweets.append(features)

        target = [0 for x in range(feature_tweets)]

        return (feature_tweets, target)

    def filter_positive(self):

        sample = self.get_pos_tweets()

        feature_tweets = []
        for tweet in self.pos_tweets:
            features = self.get_feature_vector(tweet)
            feature_tweets.append(features)

        target = [1 for x in range(self.pos_tweets)]

        return (feature_tweets, target)

    def term_selection(self):
        """
        Possibly relevant terms in T_n. Tweets containing those terms
        should not be included in T_n. TF_IDF is used to compute those
        terms.
        tf_b(t): How often does term t appear in T_b
        df_w(t): In how many tweets does t appear
        N_w: Total number of tweets
        tf_idf(t) = tf_b(t) * log N_w / df_w(t)

        @return K_LARGEST terms with highest tf_idf
        """
        total_sum = sum(map(lambda x: x['frequency'], self.query_set))

        tf_idf = {x: self.query_set[x]['frequency'] * \
                               log(total_sum / self.query_set[x]['doc_frequency'])
                  for x in self.query_set}

        return heapq.nlargest(TOP_K, tf_idf, key=tf_idf.get)

    def train_classifier(self):
        """
        Train an SVM classifier with both the T_b (positive tweets)
        and T_n (negative tweets).
        """
        neg_features, neg_target = self.filter_negative()
        pos_features, pos_target = self.filter_positive()

        feature_vector = neg_features.extend(pos_features)
        target_vector = neg_target.extend(pos_target)

        classifier = svm.SVC()
        classifier.fit(feature_vector, target_vector)

        return classifier

    def get_random_sample(self):
        """
        Get a random sample of tweets for T_n (10x size of T_b)
        """
        tweets = []
        return tweets

    def get_query_set(self):
        return self.query_set

    def get_feature_vector(self, tweet):
        """
        Feature vector contains
        1) 1 or 0 if word is contained in query set
        2) 1 or 0 if user is contained in query set
        3) missed = # words not in query set / # words in tweet
        @param tweet: A tweet in the twitter json format
        @return list: feature vector
        """
        text = tweet['text']
        #TODO(rkempter): verify AND extract hashtags
        users = tweet['user']['handle']

        filtered_text = []

        text_tokenized = word_tokenize(text)
        for term in text_tokenized:
            if term not in self.stop_words:
                filtered_text.append(term)

        term_features = [1 if term in self.query_set else 0 for term in filtered_text]
        user_features = [1 if user in self.query_set else 0 for user in users]
        missed = float(sum(term_features)) / len(filtered_text)

        return term_features.extend(user_features.append(missed))



        # every REMODEL, recompute model