import collections


Card = collections.namedtuple('Card', ['rank', 'suit'])

class FrenchDeck:
    ranks = [str(n) for n in range(2, 11)] + list('JQKA')
    suits = 'spades diamonds clubs hearts'.split()

    def __init__(self):
        self._cards = [Card(rank, suit) for suit in self.suits
                                        for rank in self.ranks]

    def __len__(self):
        return len(self._cards)

    def __getitem__(self, position):
        return self._cards[position]


# winning_card = Card('A', 'spades')
# print(winning_card)

myDeck = FrenchDeck()
# print(len(myDeck))
# print(myDeck._cards)
# print(myDeck[1])
# print(myDeck[:3])
print(myDeck[12::13])
# for card in myDeck:
#     print(card)


# from random import choice
# print(choice(myDeck))


