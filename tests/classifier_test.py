"""
Test the classifier API
"""

from dynhashtag.classifier import AdaptiveTweetClassifierTrainer


import unittest
import Queue

class AdaptiveTweetClassifierTrainerTests(unittest.TestCase):

    def setUp(self):
        self.q = Queue.Queue()

    # def test_random_sample(self):
    #     """
    #     Check if size of random sample correct
    #     """
    #     required_size = 20
    #
    #     trainer = AdaptiveTweetClassifierTrainer(self.q)
    #     sample = trainer.get_random_sample(required_size)
    #
    #     self.assertEqual(required_size, len(sample))

    def test_feature_vector(self):

        tweet = {}

        tweet['text'] = "This is a test #bla @rkempter Sommerferien and Lorem Ipsum"

        tweet['entities'] = {}
        tweet['entities']['hashtags'] = [{"text" : "bla"}, {"text" : "blu"}]
        tweet['entities']['urls'] = [{"expanded_url" : "http://www.twitter.com"}]
        tweet['entities']['user_mentions'] = [{"id" : "1111"}, {"id" : "2222"}]
        tweet['user'] = {"str_id" : "1111"}

        trainer = AdaptiveTweetClassifierTrainer(self.q)

        trainer.query_set = {}
        trainer.query_set['keyword'] = ["Sommerferien", "blu", "empty", "leer"]
        trainer.query_set['user'] = ["2222", "3333"]

        vector = trainer.get_feature_vector(tweet)
        real_vector = [1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.2222222222222222]

        self.assertEqual(vector, real_vector)