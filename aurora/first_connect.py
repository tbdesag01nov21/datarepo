import pandas as pd
import pymysql

def get_aws_creds():
    myvars = {}
    with open("aws_info.txt") as myfile:
        for line in myfile:
            name, var = line.partition("=")[::2]
            myvars[name.strip()] = var.rstrip()

    return myvars

aws_info = get_aws_creds()
host = aws_info["host"]
port=3306
user = aws_info["user"]
password = aws_info["password"]

conn = pymysql.connect(host=host, user=user, port=port, passwd=password)