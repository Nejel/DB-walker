import json
import csv
import sqlite3
import re
import urllib
import codecs

conn = sqlite3.connect('DB2.sqlite') #create our DB with 'DB1.sqlite' name and connect to it
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS raw_data') #delete other table if exists
cur.execute('DROP TABLE IF EXISTS raw_data_1') #delete other table if exists
cur.execute('''CREATE TABLE raw_data_1 (api_source VARCHAR(999999), api_method VARCHAR(999999), result TEXT, api_param VARCHAR(999999), insert_ts CURRENT_TIMESTAMPTZ)''')

cur.execute('DROP TABLE IF EXISTS raw_data_2') #delete other table if exists
cur.execute('''CREATE TABLE raw_data_2 (api_source VARCHAR(999999), api_method VARCHAR(999999), result TEXT, api_param VARCHAR(999999), insert_ts CURRENT_TIMESTAMPTZ)''')

cur.execute('DROP TABLE IF EXISTS data_method_input') #delete other table if exists
cur.execute('''CREATE TABLE data_method_input (user INT, ts TIMESTAMPTZ, context JSONB, ip INT)''')

cur.execute('DROP TABLE IF EXISTS data_method_reward_1') #delete other table if exists
cur.execute('''CREATE TABLE data_method_reward_1 (user INT, ts TIMESTAMPTZ, reward_id INT, reward_money INT)''')

cur.execute('DROP TABLE IF EXISTS data_method_reward_2') #delete other table if exists
cur.execute('''CREATE TABLE data_method_reward_2 (user INT, ts TIMESTAMPTZ, reward_id PRIMARY KEY)''')

cur.execute('DROP TABLE IF EXISTS data_error') #delete other table if exists
cur.execute('''CREATE TABLE data_error (api_source VARCHAR(999999), api_method VARCHAR(999999), result TEXT, error_text VARCHAR(999999))''')


# вывела ошибки из джейсона
dict_list = []
error_list = []
file = open('input-2017-02-01.json')
for line in file.readlines():
    cur.execute('''INSERT INTO raw_data_1 (api_source, api_method, result, api_param, insert_ts) VALUES ('Amazon', 'input', ?, 'value', CURRENT_TIMESTAMP)''', (line,))
    try:
        dict_list.append(json.loads(line)) #parse json-line and put it into dict list
    except ValueError:
        error_list.append(line)
        for line in error_list:
            lines = str(line)
            #print(lines)
            cur.execute('''INSERT INTO data_error (api_source, api_method, result, error_text) VALUES ('Amazon', 'reward', ?, 'ValueError')''', (lines,))


dict_listcsv = []
error_listcsv = []
filecsv = open('reward-2017-02-01.csv')
for linecsv in filecsv.readlines():
    #cur.execute('''INSERT INTO raw_data_1 (api_source, api_method, result, api_param, insert_ts) VALUES ('Amazon', 'input', ?, 'value', CURRENT_TIMESTAMP)''', (linecsv,))
    try:
        dict_listcsv.append(json.loads(linecsv)) #parse csv-line and put it into dict list
    except ValueError:
        error_listcsv.append(linecsv)
        for linecsv in error_listcsv:
            linescsv = str(linecsv)
            print(linescsv)
            cur.execute('''INSERT INTO data_error (api_source, api_method, result, error_text) VALUES ('Amazon', 'reward', ?, 'ValueError')''', (linescsv,))


list = []
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
        cur.execute('''INSERT INTO raw_data_2 (api_source, api_method, result, api_param, insert_ts) VALUES ('Amazon', 'reward', ?, 'value', CURRENT_TIMESTAMP)''', (lines,))


list = []
cur.execute("SELECT * FROM raw_data_1")
rows = cur.fetchall()
for row in rows:
    data = row[0:5]
    list.append(data)
    for line in list:
        lines = line[2]
        #print(lines)
        selectuser = str(re.findall("\"user\":(\S+),\"ts", lines))
        selectts = str(re.findall("\"ts\":(\S+),\"context", lines))
        selectcontext = str(re.findall("\"context\":(\S+)", lines))# не нашел полностью, только хард
        selectip = str(re.findall("\"ip\":(\S+)", lines))
    cur.execute('''INSERT INTO data_method_input (user, ts, context, ip) VALUES (?, ?, ?, ?)''', (selectuser, selectts, selectcontext, selectip,))


list_1 = []
cur.execute("SELECT * FROM raw_data_2")
rows = cur.fetchall()
for row in rows:
    data = row[0:5]
    list_1.append(data)
    for line in list_1:
        lines = line[2]
        #print("this is lines", lines)
        selectuser = str(re.findall("\'user\':( \S+)", lines))
        selectts = str(re.findall("\'ts\':( \S+)", lines))
        selectreward_id = str(re.findall("\'reward_id\':( \S+),", lines))
        selectreward_money = str(re.findall("\'reward_money\':( \S+)", lines))
    cur.execute('''INSERT INTO data_method_reward_1 (user, ts, reward_id, reward_money) VALUES (?, ?, ?, ?)''', (selectuser, selectts, selectreward_id, selectreward_money))

conn.commit()
cur.close()
