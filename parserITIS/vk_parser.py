import re

import psycopg2
import requests
from psycopg2 import OperationalError

def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError or psycopg2.errors.SyntaxError as e:
        print(f"The error '{e}' occurred")

connection = create_connection(
    "postgres", "postgres", "mansur1213", "127.0.0.1", "5432"
)





map = {}
count = 100
offset = 0
data = []
rang = 300

while offset < rang:
    response = requests.get('https://api.vk.com/method/wall.get',
                            params={
                                'access_token': 'e2432b27e2432b27e2432b2753e235938bee243e2432b278203cc34da6f042f5f10ec79',
                                'v': 5.130,
                                'domain': 'itis_kfu',
                                'count': count,
                                'offset': offset
                            }
                            )
    data1 = response.json()['response']['items']
    offset += 100
    data.extend(data1)

def add(word):
    a = map.get(word)
    if a is None:
        map.update(dict({word:1}))
    else:
        map.update(dict({word:a+1}))

def add_db(d):
    for i in d.keys():
        req = "INSERT INTO words(word,count) VALUES ('" + str(i) + "',"+str(d.get(i))+") ON CONFLICT(word) do update set count="+str(d.get(i))+";"
        execute_query(connection,req)

for i in range(rang):
    text = data[i]['text']
    for j in re.split('\.|,|\'|\"|\s|«|»|-|\)|\(|/|:|0|1|2|3|4|5|6|7|8|9|/]|/}|—|⠀',text):
        if j != '':
            add(j)
add_db(map)




