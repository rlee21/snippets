import json
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
    Search recent tweets for given term and 
    return list of hashtags and their frequency
    """
    hashtags = {}
    results = api.GetSearch(term=term, count=count)
    for result in results:
        data = result.AsDict()
        print(data) # TODO: remove print statement
        for i in data['hashtags']:
            hashtag = i['text'].lower()
            if hashtag not in hashtags:
                hashtags[hashtag] = 1
            else:
                hashtags[hashtag] += 1
    return [(k, v) for k, v in hashtags.items()]


if __name__ == '__main__':
    with open('credentials.json', 'r') as f:
        auth = json.loads(f.read())
    api = twitter.Api(
        consumer_key=auth['consumer_key'],
        consumer_secret=auth['consumer_secret'],
        access_token_key=auth['access_token'],
        access_token_secret=auth['access_token_secret']
    )

    screen_name = 'realself'
    count = 10 # TODO: change back to 100
    filename = '{}_most_recent_{}_tweets.json'.format(screen_name, count)
    term = '#Seattle'

    get_tweets(screen_name, count, filename)
    print(search_term(term, count))
