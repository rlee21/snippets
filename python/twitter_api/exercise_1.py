"""
Dependencies (i.e. requirements.txt)
    certifi==2018.4.16
    chardet==3.0.4
    future==0.16.0
    idna==2.7
    oauthlib==2.1.0
    python-twitter==3.4.2
    requests==2.19.0
    requests-oauthlib==1.0.0
    urllib3==1.23
"""

import json
import os
import twitter


def get_tweets(screen_name, count, filename):
    """
    Fetch recent tweets for given screen name and
    write them to a newline-delimited json file
    """
    tweets = api.GetUserTimeline(screen_name=screen_name, count=count)
    with open(filename, 'w') as f:
        for tweet in tweets:
            data = tweet.AsDict()
            f.write(json.dumps(data) + '\n')


def search_term(term, count):
    """
    Search recent tweets for given term and dictionary
    of hashtags and their frequency. Hashtags from
    retweets are also counted when present.
    """
    hashtags = {}
    results = api.GetSearch(term=term, count=count)
    for result in results:
        data = result.AsDict()
        retweet_status = data.get('retweeted_status', None)
        if retweet_status:
            for i in data['retweeted_status']['hashtags']:
                hashtag_retweet = i['text'].lower()
                if hashtag_retweet not in hashtags:
                    hashtags[hashtag_retweet] = 1
                else:
                    hashtags[hashtag_retweet] += 1
        for i in data['hashtags']:
            hashtag = i['text'].lower()
            if hashtag not in hashtags:
                hashtags[hashtag] = 1
            else:
                hashtags[hashtag] += 1
    return hashtags


if __name__ == '__main__':
    api = twitter.Api(
        consumer_key=os.environ['TWITTER_KEY'],
        consumer_secret=os.environ['TWITTER_SECRET'],
        access_token_key=os.environ['TWITTER_TOKEN'],
        access_token_secret=os.environ['TWITTER_TOKEN_SECRET']
    )

    screen_name = 'realself'
    count = 100
    filename = '{}_most_recent_{}_tweets.json'.format(screen_name, count)
    term = '#Seattle'

    get_tweets(screen_name, count, filename)
    hashtags = search_term(term, count)
    for hashtag, freq in hashtags.items():
        print(hashtag, freq)

