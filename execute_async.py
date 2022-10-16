import snowflake.connector
import pandas as pd

print("Open database")

cnn = snowflake.connector.connect(
    user = USER,
    password = PASSWORD,
    account = ACCOUNT,
    warehouse = WAREHOUSE,
    database = DATABASE,
    schema = SCHEMA
)

cs = cnn.cursor()

sql1 = ('INSERT INTO ONE_MILLION_EXAMPLE(number_values, text_values)'
        ' SELECT TOP 100000 numbervalues + 2000000, text_values'
        ' FROM ONE MILLION_EXAMPLE;')
sql2 = 'Select * FROM project_tasks'

print('Insert async')
cs.execute_async(sql1)
print('Selecting')
cs.execute(sql2)
for row in cs:
    print(row)

cnn.close()
print("Done")