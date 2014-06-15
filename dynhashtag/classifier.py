"""
This is an implementation of the adaptive twitter filtering system
described in "Adaptive Method for Following Dynamic Topics on Twitter" by
Walid Magdy and Tamer Elsayed from the Qatar Computing Research Institute
in Doha.
"""
import heapq
import MySQLdb

from dynhashtag.config import (MYSQL_DB, MYSQL_HOST, MYSQL_PWD, MYSQL_USER,
                     TOP_K, LANGUAGE, CONSUMER_KEY, CONSUMER_SECRET,
                     ACCESS_TOKEN, ACCESS_TOKEN_SECRET, REMODEL)
from dynhashtag.util import extract_entity, clean_entity


from collections import namedtuple
from datetime import timedelta, datetime
from nltk import word_tokenize
from nltk.corpus import stopwords
from math import log
from sklearn import svm
from tweepy import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# used to efficently compute tf_idf
TweetCount = namedtuple('tweet_count', ['frequency', 'doc_frequency'])


class SampleStreamListener(StreamListener):
    """
    Used to get a sample of size @param required_size of random tweets
    """
    def __init__(self, required_size):
        super(StreamListener).__init__()
        self.required_size = required_size
        self.sample = []

    def on_status(self, status):
        if self.required_size <= 0:
            return False
        else:
            self.sample.append(status)
            return True

    def get_sample(self):
        return self.sample


class AdaptiveTweetClassifierTrainer(object):
    """
    Used to train a classifier every x hours.
    """

    # We have tweet_window (T_w), the tweets in window w

    query = """SELECT user, tweet, user_mentions, hashtags FROM tweets WHERE created_at > %s"""

    def __init__(self, queue):
        """
        Initialization of the classifier. Give initial
        keywords and user handles to initiate the process
        of tweet collection.
        :param queue: Queue to store the classifier to use in another Thread.
        """
        self.stop_words = list()
        for lang in LANGUAGE:
            self.stop_words.extend(stopwords.words('english'))

        self.query_set = dict()
        self.queue = queue

    def get_pos_tweets(self):
        """
        Read tweets from a mysql database
        """
        connection = MySQLdb.connect(
            host=MYSQL_HOST, passwd=MYSQL_PWD,
            user=MYSQL_USER, db=MYSQL_DB)
        cursor = connection.cursor()

        date = datetime.now() - timedelta(hours=REMODEL)

        cursor.execute(self.query, date.strftime("%Y-%m-%d %H:%M:%S"))
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

        word_count = dict(TweetCount)
        user_count = dict(TweetCount)

        def _doc_freq(_term):
            word_count[_term]['doc_frequency'] += 1
            return

        def _add_term(_term, _index, _stopwords=list()):
            if _term in _stopwords:
                return
            if _term in _index:
                _index[_term] += 1
            else:
                _index[_term] = TweetCount(1, 0)

        pos_tweets = self.get_pos_tweets()
        for tweet in pos_tweets:
            text = tweet['text']
            text_tokenized = word_tokenize(text)
            text_cleaned = clean_entity(text_tokenized)
            for term in text_cleaned:
                _add_term(term, word_count, self.stop_words)

            hashtags, user_mentions = extract_entity(tweet)

            user_mentions.append(tweet['user']['id_str'])
            for hashtag in hashtags:
                _add_term(hashtag, word_count, self.stop_words)

            for user in user_mentions:
                _add_term(user, user_count)

            hashtags.extend(user_mentions)
            text_cleaned.extend(hashtags)

            map(_doc_freq, set(text_cleaned))

        self.query_set['keyword'] = [x for x in word_count if word_count[x]['frequency'] > 10]
        self.query_set['user'] = [x for x in user_count if user_count[x]['frequency'] > 10]

    def filter_negative(self, size):
        """
        Get a negative sample of tweets (filtering with
        the exclusion terms)
        :type self: object
        :param size:
        """
        sample = self.get_random_sample(size)

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

        return feature_tweets, target


    def filter_positive(self, sample):

        feature_tweets = []
        for tweet in sample:
            features = self.get_feature_vector(tweet)
            feature_tweets.append(features)

        target = [1 for x in range(len(sample))]

        return feature_tweets, target

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
        total_sum = sum(map(lambda x: x['frequency'], self.query_set['keyword']))

        tf_idf = {x: self.query_set['keyword'][x]['frequency'] *
                     log(total_sum / self.query_set['keyword'][x]['doc_frequency'])
                  for x in self.query_set['keyword']}

        return heapq.nlargest(TOP_K, tf_idf, key=tf_idf.get)

    def train_classifier(self):
        """
        Train an SVM classifier with both the T_b (positive tweets)
        and T_n (negative tweets).
        """
        pos_sample = self.get_pos_tweets()
        pos_features, pos_target = self.filter_positive(pos_sample)
        neg_features, neg_target = self.filter_negative(len(pos_sample))

        feature_vector = neg_features.extend(pos_features)
        target_vector = neg_target.extend(pos_target)

        classifier = svm.SVC()
        classifier.fit(feature_vector, target_vector)
        self.queue.put(classifier)

        return

    def get_random_sample(self, size):
        """
        Get a random sample of tweets for T_n (10x size of T_b)
        """
        auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
        l = SampleStreamListener(size)
        stream = Stream(auth, l)
        stream.sample(async=True)
        tweets = l.get_sample()

        return tweets

    def get_query_set(self):
        query_set = self.query_set['keyword']
        query_set_user = self.query_set['user']
        query_set.extend(query_set_user)
        return query_set

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
        hashtags, user_mentions = extract_entity(tweet)

        filtered_text = []

        # remove hashtags from text
        text_tokenized = word_tokenize(text)
        for term in text_tokenized:
            if term not in self.stop_words:
                filtered_text.append(term)

        term_features = [1.0 if term in self.query_set['keyword'] else 0 for term in filtered_text]
        user_features = [1.0 if user in self.query_set['user'] else 0 for user in user_mentions]
        missed = float(sum(term_features)) / len(filtered_text)

        user_features.append(missed)
        term_features.extend(user_features)

        return term_features