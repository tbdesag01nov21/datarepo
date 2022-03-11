import pandas as pd
import pymysql

host=""
port=3306
dbname="post"
user=""
password=""

conn = pymysql.connect(host=host, user=user, port=port, passwd=password, db=dbname)