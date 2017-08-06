with open('sf_contact_2016-12-17.csv','r') as infile:
    data = infile.read()
# print (data)

with open('test.csv', 'w') as outfile:
    outfile.write(data)