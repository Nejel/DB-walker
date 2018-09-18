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
import sqlite3
import re
import csv

fhand = list(open('input-2017-02-01.json', 'r'))
info = json.dumps(fhand)
print(info)
#fhand = json.loads(open ('input-2017-02-01.json').read())
#info = json.load(fhand)




conn = sqlite3.connect('DB1.sqlite')
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS raw_data')
cur.execute('''CREATE TABLE raw_data (api_source VARCHAR(999999), api_method VARCHAR(999999), api_date TIMESTAMPTZ, result TEXT, api_param VARCHAR(999999), insert_ts CURRENT_TIMESTAMPTZ)''')
