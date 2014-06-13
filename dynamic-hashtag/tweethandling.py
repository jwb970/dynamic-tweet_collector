"""
Handles new tweets
"""
import MySQLdb
import json

from .config import (MYSQL_DB, MYSQL_USER, MYSQL_PWD, MYSQL_HOST)
from .classifier import AdaptiveTweetClassifierTrainer

from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import StreamListener

class StdOutListener(StreamListener):
    """ A listener handles tweets are the received from the stream.
    This is a basic listener that just prints received tweets to stdout.

    """

    insert_tweet_query = u"""INSERT INTO tweets (\
            tweet_id,user,created_at,in_reply_to_user_id_str,\
            in_reply_to_status_id_str,retweeted,favorited,\
            favorite_count,retweet_count,source,\
            tweet,urls,hashtags,user_mentions) VALUES (\
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    insert_user_query = u"""INSERT IGNORE INTO users (\
            user_id,created_at,description,favourites_count,\
            followers_count,friends_count,status_count,\
            listed_count,time_zone,verified,geo_enabled,lang,\
            location,screen_name) VALUES (%s,%s,%s,%s,%s,%s,
            %s, %s, %s, %s, %s, %s, %s, %s)"""

    def __init__(self, init_user, init_keyword):
        super(StreamListener).__init__()
        self.trainer = AdaptiveTweetClassifierTrainer(init_user, init_keyword)
        self.query_set = {}
        self.query_set['user'] = init_user
        self.query_set['init_keyword'] = init_keyword

    def retrain(self):
        """
        Retrain the model, restart streaming
        """
        self.classifier = self.trainer.train_classifier()
        self.query_set = self.trainer.get_query_set()

    def connect_db(self):
        self.connection = MySQLdb.connect(
            host=MYSQL_HOST, passwd=MYSQL_PWD,
            user=MYSQL_USER, db=MYSQL_DB)
        self.cursor = self.connection.cursor()

    def on_connect(self):
        self.connect_db()

    def on_data(self, data):
        """
        Write received tweets to file
        Format:

        """
        data_dict = json.JSONDecoder(encoding='utf-8').decode(data)
        if 'text' not in data_dict:
            return
        print "Tweet arriving"

        data_dict.update({
            'user_id' : data_dict['user']['id_str'],
            'user_mentions': ",".join([str(user['id']) for user in data_dict['entities']['user_mentions']]),
            'hashtags' : ",".join([hashtag['text'] for hashtag in data_dict['entities']['hashtags']]),
            'urls' : ",".join([url['expanded_url'] for url in data_dict['entities']['urls']]),
            'tweet' : data_dict['text']
            })

        try:
            cursor = self.connection.cursor()
            self.cursor.execute(self.insert_tweet_query, (
                data_dict['id'], data_dict['user_id'],
                data_dict['created_at'], data_dict['in_reply_to_user_id_str'],
                data_dict['in_reply_to_status_id_str'], data_dict['retweeted'],
                data_dict['favorited'], data_dict['favorite_count'],
                data_dict['retweet_count'],data_dict['source'], data_dict['tweet'].encode('utf-32', errors='ignore'),
                data_dict['urls'].encode('utf-8', errors='ignore'),
                data_dict['hashtags'],data_dict['user_mentions']))
            cursor.close()
            cursor = self.connection.cursor()
            self.cursor.execute(self.insert_user_query, (data_dict['user']['id_str'],
                data_dict['user']['created_at'], data_dict['user']['description'],
                data_dict['user']['favourites_count'], data_dict['user']['followers_count'],
                data_dict['user']['friends_count'], data_dict['user']['statuses_count'],
                data_dict['user']['listed_count'], data_dict['user']['time_zone'],
                data_dict['user']['verified'], data_dict['user']['geo_enabled'],
                data_dict['user']['lang'], data_dict['user']['location'],
                data_dict['user']['screen_name']))
            cursor.close()
            self.connection.commit()
        except Exception as inst:
            print type(inst)     # the exception instance
            print inst.args      # arguments stored in .args
            print inst           # __str__ allows args to printed directly
        return True

    def on_error(self, status):
        print status