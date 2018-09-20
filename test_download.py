'''
Архитектурка
    1. Открываем файл csv, открываем json, открываем SQL (некий файл с тектом запроса)
    2. Создаем БД1 с таблицей raw_data
    3. Исполняем SQL: в поля таблицы кладем всю инфу из файлов csv json.
        raw_data​ (
        1. api_source (varchar)
        2. api_method (varchar)
        3. api_date (timestamptz) # таймштапм с часовым поясом
        4. result (text)
        5. api_param (varchar)
        6. insert_ts (timestamptz default ‘now()’)
    4. Записываем построчно
'''

import json
import urllib
import sqlite3
import re
import csv

conn = sqlite3.connect('DB1.sqlite')
cur = conn.cursor()
cur.execute('DROP TABLE IF EXISTS raw_data')
cur.execute('''CREATE TABLE raw_data (api_source VARCHAR(999999), api_method VARCHAR(999999), api_date TIMESTAMPTZ, result, api_param VARCHAR(999999), insert_ts CURRENT_TIMESTAMPTZ)''')

jsonhand = open('input-2017-02-01.json')

first = True #flag, if first line
header = None
list = []
result1 = []
for line in csv.reader(open('reward-2017-02-01.csv', 'r'), delimiter=','):
    if first :
        header = line
        first = False
    else:
        list.append(dict(zip(header, line)))
        for line in jsonhand.readlines():
            result2 = line
            print(result2)
            cur.execute('''INSERT INTO raw_data (result, insert_ts) VALUES (?, CURRENT_TIMESTAMP)''', (result2,))
print(list)
    print('hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh')

conn.commit()
cur.close()



#info = json.dumps(fhand)
#print(info)
#fhand = json.loads(open ('input-2017-02-01.json').read())
#info = json.load(fhand)






''' Для индекса

for line in jsonhand.readlines():
    try:
        dict_list.append(json.loads(line))
        user = dict_list[int(count)]['user']
        ts = dict_list[int(count)]['ts']
        hard = dict_list[int(count)]['context']['hard']
        soft = dict_list[int(count)]['context']['soft']
        level = dict_list[int(count)]['context']['level']
        ip = dict_list[int(count)]['ip']
        print(user)
        print(ip)
        print(hard)
    except ValueError:
        continue
    count += 1


'''
