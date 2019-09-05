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

def compare_transactions(current, previous, filename):
    """ convert current and previous transactions to sets,
        compare and write current to file if there is a difference  """
    curr = { current }
    prev = { previous }
    if curr.difference(prev):
        new_transaction = current.split('-')
        # print("There's a new transaction!! => {}".format(''.join(new_transaction)))
        print("There's a new transaction!! => {}".format(current))
        with open(filename, 'w') as fh:
            fh.write(current)
        # TODO: send email or slack notification
    else:
        print('No new transaction')

def main():
    # set vars
    OAUTH_FILE = 'oauth2.json'
    TRANSACTIONS_ENDPOINT = 'https://fantasysports.yahooapis.com/fantasy/v2/league/nfl.l.738705/transactions'
    TRANSACTION_FILE = 'last_transaction.txt'

    # script execution
    client = yahoo_client(OAUTH_FILE)
    transactions = get_transactions(client, TRANSACTIONS_ENDPOINT)
    current = latest_transaction(transactions)
    previous = previous_transaction(TRANSACTION_FILE)
    compare_transactions(current, previous, TRANSACTION_FILE)




if __name__ == '__main__':
    main()
