import json
import csv
import sqlite3
import re
import urllib
import codecs

conn = sqlite3.connect('DB2.sqlite') #create our DB with 'DB1.sqlite' name and connect to it
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS raw_data') #delete other table if exists
cur.execute('''CREATE TABLE raw_data (api_source VARCHAR(999999), api_method VARCHAR(999999), api_date TIMESTAMPTZ, result TEXT, api_param VARCHAR(999999), insert_ts CURRENT_TIMESTAMPTZ)''')

cur.execute('DROP TABLE IF EXISTS data_method_input') #delete other table if exists
cur.execute('''CREATE TABLE data_method_input (user INT, ts TIMESTAMPTZ, context JSONB, ip INT)''')

cur.execute('DROP TABLE IF EXISTS data_method_reward_1') #delete other table if exists
cur.execute('''CREATE TABLE data_method_reward_1 (user INT, ts TIMESTAMPTZ, reward_id INT, reward_money INT)''')

cur.execute('DROP TABLE IF EXISTS data_method_reward_2') #delete other table if exists
cur.execute('''CREATE TABLE data_method_reward_2 (user INT, ts TIMESTAMPTZ, reward_id INT)''') #вставить сюда PRIMARY KEY

cur.execute('DROP TABLE IF EXISTS data_error') #delete other table if exists
cur.execute('''CREATE TABLE data_error (api_source VARCHAR(999999), api_method VARCHAR(999999), api_date TIMESTAMPTZ, result TEXT, error_text VARCHAR(999999))''')

file = open('input-2017-02-01.json')
for line in file.readlines():
    cur.execute('''INSERT INTO raw_data (api_source, api_method, api_date, result, api_param, insert_ts) VALUES ('Amazon', 'input', 'TIMESTAMPTZ', ?, 'value', CURRENT_TIMESTAMP)''', (line,))

list = []
cur.execute("SELECT * FROM raw_data")
rows = cur.fetchall()
for row in rows:
    data = row[0:6]
    list.append(data)
    for line in list:
        lines = line[3]
        print(lines)
        selectuser = str(re.findall("\"user\":(\S+),\"ts", lines))
        selectts = str(re.findall("\"ts\":(\S+),\"context", lines))
        selectcontext = str(re.findall("\"context\":(\S+),\"ip", lines))# не записал, потому что там надо JSONB
        selectip = str(re.findall("\"ip\":(\S+)", lines))
    cur.execute('''INSERT INTO data_method_input (user, ts, context, ip) VALUES (?, ?, ?, ?)''', (selectuser, selectts, selectcontext, selectip,))


count = 0
for line in csv.reader(open('reward-2017-02-01.csv', 'r'), delimiter=','):
    if count == 0:
        count += 1
        header = line
    else:
        list.append(dict(zip(header, line)))
        #print(list)
        for line in list:
            lines = str(line)






conn.commit()
cur.close()
