#!venv/bin/python

from yahoo_oauth import OAuth2


def yahoo_client(oauth_file):
    """ yahoo fantasy sports client """
    oauth = OAuth2(None, None, from_file=oauth_file)
    if not oauth.token_is_valid():
        oauth.refresh_access_token()

    return oauth

def get_transactions(client, url):
    """ fetch fantasy football league transactions """
    request = client.session.get(url, params={'format': 'json'})

    return request.json()

def current_transactions(transactions):
    """ parse last 10 transactions """
    data = transactions['fantasy_content']['league'][1]['transactions']
    players = []
    for i in range(10):
        player = data[str(i)]['transaction'][1]['players']['0']['player'][0][2]['name']['full']
        players.append(player + '\n')

    return players

def previous_transactions(filename):
    """ read file to get the previous transactions """
    with open(filename, 'r') as fh:
        transactions = fh.read()

    return transactions

def write_transactions(filename, current):
    """ write current transactions to file """
    with open(filename, 'w') as fh:
        fh.writelines(current)

def compare_transactions(current, previous, filename):
    """ convert current and previous transactions to sets,
        compare and write current to file if there's a difference  """
    curr = { ''.join(current) }
    prev = { previous }
    diff = curr.difference(prev)
    if diff:
        write_transactions(filename, current)
        print('NEW TRANSACTIONS:')
        for idx, value in enumerate(current, 1):
            player = value.rstrip('\n') 
            print('{0}) {1}'.format(idx, player))
    else:
        print('No changes')

def main():
    # setup
    OAUTH_FILE = '/Users/relee/code/sandbox/ff/oauth2.json'
    LEAGUE_ID = '738705'
    TRANSACTIONS_ENDPOINT = 'https://fantasysports.yahooapis.com/fantasy/v2/league/nfl.l.{league_key}/transactions'.format(league_key=LEAGUE_ID)
    TRANSACTION_FILE = '/Users/relee/code/sandbox/ff/current_transactions.txt'

    # execution
    client = yahoo_client(OAUTH_FILE)
    transactions = get_transactions(client, TRANSACTIONS_ENDPOINT)
    current = current_transactions(transactions)
    previous = previous_transactions(TRANSACTION_FILE)
    compare_transactions(current, previous, TRANSACTION_FILE)




if __name__ == '__main__':
    main()
