import re


text = """ This PEP contains the index of all Python Enhancement Proposals,
    known as PEPs.  PEP numbers are assigned by the PEP editors, and
    once assigned are never changed[1].  The Mercurial history[2] of
    the PEP texts represent their historical record. """

pattern = r'hist+'
match_obj = re.findall(pattern, text)
match_obj2 = re.search(pattern, text)
print(match_obj)
print(match_obj2.group())
