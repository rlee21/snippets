from datetime import datetime


with open('de.log') as datafile:
    for line in datafile:
        line = line.rstrip('\n')
        dt = datetime.fromtimestamp(int(line))
        print((line, dt))
    
