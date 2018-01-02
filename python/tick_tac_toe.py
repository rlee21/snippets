#!/usr/bin/python


def create_board(positions):
    """ Return dictionary of game board positions with empty values """
    board = {}
    for position in positions:
        board[position] = ' '
    return board


def update_board(board, position, value):
    """ Update board with position and value """
    return board.update({position: value})


def print_board(board):
    """ Print board """
    print('_' + board['top-l'] + '_|_' + board['top-m'] + '_|_' + board['top-r'] + '_\n' +
          '_' + board['mid-l'] + '_|_' + board['mid-m'] + '_|_' + board['mid-r'] + '_\n' +
          ' ' + board['low-l'] + ' | ' + board['low-m'] + ' | ' + board['low-r'] + ' \n')


def get_position(positions):
    """ Prompt user for game board position """
    while True:
        position = input('Enter position: ').strip().lower()
        if position in positions:
            return position
        print('ERROR: not a valid position, enter one of the valid positions below \n', positions)


def get_value(values):
    """ Prompt user for game board value """
    while True:
        value = input('Enter value: ').strip().lower()
        if value in values:
            return value
        print('ERROR: not a valid value, enter one of the valid values below \n', values)


def play_game(board):
    """ Run game for total of 9 moves and check for winner after every move """
    for _ in range(9):
        position = get_position(positions)
        value = get_value(values)
        update_board(board, position, value)
        print_board(board)
        if board['top-l'] != ' ' and board['top-l'] == board['top-r'] and board['top-l'] == board['top-m']:
            print('YOU WON!!')
            print_board(board)
            break
        elif board['mid-l'] != ' ' and board['mid-l'] == board['mid-r'] and board['mid-l'] == board['mid-m']:
            print('YOU WON!!')
            print_board(board)
            break
        elif board['low-l'] != ' ' and board['low-l'] == board['low-r'] and board['low-l'] == board['low-m']:
            print('YOU WON!!')
            print_board(board)
            break
        elif board['top-l'] != ' ' and board['top-l'] == board['mid-m'] and board['top-l'] == board['low-r']:
            print('YOU WON!!')
            print_board(board)
            break
        elif board['top-r'] != ' ' and board['top-r'] == board['mid-m'] and board['top-r'] == board['low-l']:
            print('YOU WON!!')
            print_board(board)
            break


if __name__ == '__main__':
    positions = ['top-l', 'top-m', 'top-r', 'mid-l', 'mid-m', 'mid-r', 'low-l', 'low-m', 'low-r']
    values = ['x', 'o']
    board = create_board(positions)
    print('valid positions:\n', ', '.join(positions), '\nvalid values:\n', ', '.join(values), '\nctrl+c to exit:')
    print_board(board)
    play_game(board)
