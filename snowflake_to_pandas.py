import snowflake.connector
import pandas as pd

print('Opening...')

cnn = snowflake.connector.connect(
    user = USER,
    password = PASSWORD,
    account = ACCOUNT,
    warehouse = WAREHOUSE,
    database = DATABASE,
    schema = SCHEMA
)

cs = cnn.cursor()
sql = 'SELECT * FROM project_tasks'
cs.execute(sql)
df = cs.fetch_pandas_all()

cs.close()
cnn.close()

print(df.head(10))
