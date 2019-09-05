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

def latest_transaction(transactions):
    """ parse transactions for latest transaction """
    data = transactions['fantasy_content']['league'][1]['transactions']['0']['transaction'][1]['players']['0']['player']
    player_name = data[0][2]['name']['full']
    transaction_type = data[1]['transaction_data']['type']
    team_name = data[1]['transaction_data']['source_team_name']
    composite_key = '{0}-{1}-{2}'.format(player_name, transaction_type, team_name)

    return composite_key.replace(' ', '')

def previous_transaction(filename):
    """ read file to get the previous transaction """
    with open(filename, 'r') as fh:
        transaction = fh.read()

    return transaction

def write_transaction(filename, current):
    """ write current transaction to file """
    with open(filename, 'w') as fh:
        fh.write(current)

def compare_transactions(current, previous, filename):
    """ convert current and previous transactions to sets,
        compare and write current to file if there's a difference  """
    curr = { current }
    prev = { previous }
    diff = curr.difference(prev)
    if diff:
        print("NEW TRANSACTION: {}".format(current))
        write_transaction(current, filename)
        # TODO: send email or slack notification
    else:
        print('No new transaction')

def main():
    # setup
    OAUTH_FILE = 'oauth2.json'
    LEAGUE_ID = '738705'
    TRANSACTIONS_ENDPOINT = 'https://fantasysports.yahooapis.com/fantasy/v2/league/nfl.l.{league_key}/transactions'.format(league_key=LEAGUE_ID)
    TRANSACTION_FILE = 'last_transaction.txt'

    # execution
    client = yahoo_client(OAUTH_FILE)
    transactions = get_transactions(client, TRANSACTIONS_ENDPOINT)
    current = latest_transaction(transactions)
    previous = previous_transaction(TRANSACTION_FILE)
    compare_transactions(current, previous, TRANSACTION_FILE)




if __name__ == '__main__':
    main()
