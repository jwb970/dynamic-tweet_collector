__author__ = 'rkempter'

def extract_entity(entity):
    hashtags = ",".join(
        [hashtag['text']
         for hashtag in entity['entities']['hashtags']]
    )
    user_mentions = ",".join(
        [str(user['id'])
         for user in entity['entities']['user_mentions']]
    )

    return (hashtags, user_mentions)

def clean_entity(tweet):
    tweet_terms = []
    for term in tweet:
        if term[0] not in ('@', '#'):
            tweet_terms.append(term)

    return tweet_terms