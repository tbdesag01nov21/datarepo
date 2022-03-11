import pandas as pd
import pymysql

host="database-1.chxzwwbeyr7d.us-east-2.rds.amazonaws.com"
port=3306
dbname="post"
user="adminmysql"
password="GaORNaXGqZPp6033jnYa"

conn = pymysql.connect(host=host, user=user, port=port, passwd=password, db=dbname)