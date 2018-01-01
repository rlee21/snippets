#!/usr/bin/python


def create_board(positions):
    board = dict()
    for position in positions:
        board[position] = ' '

    return board


def update_board(board, position, value):
    return board.update({position: value})


def print_board(board):
    print('_' + board['top-L'] + '_|_' + board['top-M'] + '_|_' + board['top-R'] + '_\n' +
          '_' + board['mid-L'] + '_|_' + board['mid-M'] + '_|_' + board['mid-R'] + '_\n' +
          ' ' + board['low-L'] + ' | ' + board['low-M'] + ' | ' + board['low-R'] + ' \n')



if __name__ == '__main__':
    positions = ['top-L', 'top-M', 'top-R',
                 'mid-L', 'mid-M', 'mid-R',
                 'low-L', 'low-M', 'low-R']
    values = ['X', 'O']
    board = create_board(positions)
    print('valid positions:\n', positions, '\nvalid values:\n', values)
    print_board(board)
    for _ in range(9):
        position = input('Enter position: ')
        while position not in positions:
            print('ERROR: not a valid position, enter one of the valid positions below \n', positions)
            position = input('Enter position: ')
        value = input('Enter value: ').upper()
        while value not in values:
            print('ERROR: not a valid value, enter one of the valid values below \n', values)
            value = input('Enter value: ').upper()
        update_board(board, position, value)
        if board['top-L'] != ' ' and board['top-L'] == board['top-R'] and board['top-L'] == board['top-M']:
            print('YOU WON!!')
            print_board(board)
            break
        elif board['mid-L'] != ' ' and board['mid-L'] == board['mid-R'] and board['mid-L'] == board['mid-M']:
            print('YOU WON!!')
            print_board(board)
            break
        elif board['low-L'] != ' ' and board['low-L'] == board['low-R'] and board['low-L'] == board['low-M']:
            print('YOU WON!!')
            print_board(board)
            break
        elif board['top-L'] != ' ' and board['top-L'] == board['mid-M'] and board['top-L'] == board['low-R']:
            print('YOU WON!!')
            print_board(board)
            break
        elif board['top-R'] != ' ' and board['top-R'] == board['mid-M'] and board['top-R'] == board['low-L']:
            print('YOU WON!!')
            print_board(board)
            break
        print_board(board)
