import numpy as np
import pandas as pd
from pandas import Series, DataFrame
import sqlalchemy
from sqlalchemy import create_engine
import json
import psycopg2
#read the raw_department.txt
file_path = r'C:\raw_department.txt'
raw_department = pd.read_csv(file_path, delimiter = '-')
#print(raw_department)
#print("\\")
#read the raw_department_budget.txt
file_path1 = r'C:\raw_department_budget.txt'
raw_department_budget = pd.read_json(file_path1, lines='true')
#print(raw_department_budget)
#print("\\")
#read the raw_department_budget2.txt
file_path2 = r'C:\raw_department_budget2.txt'
raw_department_budget2 = pd.read_json(file_path2)
#print(raw_department_budget2)
#print("\\")
#union raw_department_budget,raw_department_budget
raw_department_budget3 = pd.concat([raw_department_budget,raw_department_budget2])
#print(raw_department_budget3)
#print("\\")
#join raw_department, raw_department_budget3
merged_raw_department = pd.merge(raw_department, raw_department_budget3, how='inner',on='department_id')
#print(merged_raw_department)
#drop colums sub_dep_id','sub_dep_name'
merged_raw_department = merged_raw_department.drop(['sub_dep_id','sub_dep_name'],axis=1)
#print(merged_raw_department)
#group by department_id
final_department_budget = merged_raw_department.groupby(['department_id', 'department_name']).sum()
print(final_department_budget)
#establish a connection to the postgres with sqlalchemy create_engine
conn_string = 'postgresql://postgres:og131078@127.0.0.1/chinook' #fill user, password, host(ip), database name
engine = sqlalchemy.create_engine(conn_string)
conn = engine.connect()
table_name = 'department_budget'
# Write the DataFrame to the PostgreSQL database
final_department_budget.to_sql(table_name, engine, if_exists='replace', index=False)
