__author__ = 'rkempter'

def join_entity(entity):
    hashtags = ",".join(
        [hashtag['text']
         for hashtag in entity['entities']['hashtags']]
    )
    user_mentions = ",".join(
        [str(user['id'])
         for user in entity['entities']['user_mentions']]
    )

    urls = ",".join(
        [str(url['expanded_url'])
         for url in entity['entities']['urls']]
    )

    return hashtags, user_mentions,urls

def extract_entity(entity):
    hashtags = [hashtag['text']
                for hashtag in entity['entities']['hashtags']]
    user_mentions =  [str(user['id'])
                      for user in entity['entities']['user_mentions']]

    urls = [str(url['expanded_url'])
            for url in entity['entities']['urls']]

    return hashtags, user_mentions,urls

def clean_entity(tweet):
    tweet_terms = []
    for term in tweet:
        if term[0] not in ('@', '#'):
            tweet_terms.append(term)

    return tweet_terms