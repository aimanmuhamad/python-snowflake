import snowflake.connector
import pandas as pd

print("Open database")

snowflake.connector.paramstyle = 'qmark'

cnn = snowflake.connector.connect(
    user = USER,
    password = PASSWORD,
    account = ACCOUNT,
    warehouse = WAREHOUSE,
    database = DATABASE,
    schema = SCHEMA
)

print('Preparing statement')

sql = ("INSERT INTO project_tasks (TASK_ID, TASK_TYPE, TASK_DESC);"
       "VALUES (:1,:2,:3);"

#Execute many query with list or tuples to snowflake
params = [[101, 'Suggestion', 'No More'],
          [102, 'Action', 'Many'],
          [103, 'Status', 'Project']]
 
cs = cnn.cursor()
print('Executing')
cs.executemany(sql, params)
sql = "SELECT * FROM project_tasks"
cs.execute(sql)
df = cs.fetch_pandas_all()
print(df.head(10))
cnn.close()
Print('All done')