import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

print('Open file')

df = pd.read_csv(YOUR_FILE)
df.reset_index(drop=True, inplace=True)

print(df.head())
print('--------')
print('Opening snowflake')

cnn = snowflake.connector.connect(
    user = USER,
    password = PASSWORD,
    account = ACCOUNT,
    warehouse = WAREHOUSE,
    database = DATABASE,
    schema = SCHEMA
)

success, nchuncks, nrows, _ = write_pandas(cnn, df, 'project_tasks', quote_identifiers = False)
print(f'{success}, {nchuncks}, {nrows}')
print('Done')

cnn.close()
