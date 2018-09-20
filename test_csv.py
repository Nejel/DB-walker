import csv

first = True #flag, if first line
header = None
list = []
for line in csv.reader(open('reward-2017-02-01.csv', 'r'), delimiter=','):
    if first :
        header = line
        first = False
    else:
        list.append(dict(zip(header, line)))

#print(list[1]['user'])
print(list[0])
