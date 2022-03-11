import pandas as pd
import pymysql

host="dsftnov21-db.cluster-chxzwwbeyr7d.us-east-2.rds.amazonaws.com"
port=3306
dbname="dsftnov21-db"
user="admin_dsftnov21"
password="cDTJ0sw1muJukAnPGOeb"

conn = pymysql.connect(host=host, user=user, port=port, passwd=password, db=dbname)