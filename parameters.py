import snowflake.connector
import pandas as pd

print('Opening Database')

#Can using 'qmark' or 'numeric'
snowflake.connector.paramstyle = 'qmark'

cnn = snowflake.connector.connect(
    user = USER,
    password = PASSWORD,
    account = ACCOUNT,
    warehouse = WAREHOUSE,
    database = DATABASE,
    schema = SCHEMA
)

cs = cnn.cursor()

sql = ("SELECT * FROM project_tasks" 
        "WHERE task_type = ?"
        " AND TASK_ID < ?")

cs.execute(sql, ['Action'], 15)
df = cs.fetch_pandas_all()
cs.close()
cnn.close()

print(df.head(10))