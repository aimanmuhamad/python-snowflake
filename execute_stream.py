import snowflake.connector
from codecs import open

print('Open snowflake')

cnn = snowflake.connector.connect(
    user = USER,
    password = PASSWORD,
    account = ACCOUNT,
    warehouse = WAREHOUSE,
    database = DATABASE,
    schema = SCHEMA
)

sql_file = sql_file

with open(sql_file, 'r', encoding='utf-8') as f:
    for cs in cnn.execute_stream(f):
        for rt in cs:
            print(rt)

cnn.close()
print('Done')
