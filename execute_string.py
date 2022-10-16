import snowflake.connector
import pandas as pd

print('Open database')

cnn = snowflake.connector.connect(
    user = USER,
    password = PASSWORD,
    account = ACCOUNT,
    warehouse = WAREHOUSE,
    database = DATABASE,
    schema = SCHEMA
)

cs_list = cnn.execute_string(
    "SELECT * FROM project_tasks WHERE TASK_ID < 7;"
    "SELECT * FROM project_comments;"
)

print("Here is the data")
for cs in cs_list:
    for row in cs:
        print(row)

print('Close connection')        
cnn.close()
print('Done')