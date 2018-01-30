class Programmer:

    def __init__(self, fname, lname, language, years_exp, open_source):
        self.fname = fname
        self.lname = lname
        self.language = language
        self.years_exp = years_exp
        self.open_source = open_source


    def full_name(self):
        return self.fname + ' ' + self.lname


class Ruby(Programmer):

    def __init__(self, fname, lname, language, years_exp, open_source, rails_exp):
        super().__init__(fname, lname, language, years_exp, open_source)
        self.rails_exp = rails_exp

dev1 = Programmer('foo', 'barr', 'python', 2, 'Y')
dev2 = Ruby('bar', 'foo', 'ruby', 5, 'Y', 'Y')
print('dev1: ', dev1.full_name(), dev1.language, dev1.years_exp, dev1.open_source)
print('dev2: ', dev2.full_name(), dev2.language, dev2.years_exp, dev2.open_source)
