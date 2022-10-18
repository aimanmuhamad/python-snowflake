import snowflake.connector
import pandas as pd

print('Opening Database')

cnn = snowflake.connector.connect(
    user = USER,
    password = PASSWORD,
    account = ACCOUNT,
    warehouse = WAREHOUSE,
    database = DATABASE,
    schema = SCHEMA
)

cs = cnn.cursor()

sql = ("SELECT stf.LastName, stf.FirstName, prj.project_name, psf.DateStart"
       "FROM project_staff psf"
       "JOIN project prj ON prj.id = psf.projectid"
       "JOIN staff stf on stf.id = psf.staffid")
cs.execute(sql)
df = cs.fetch_pandas_all()

print('Closing connection')
cs.close()
cnn.close()
print('Data results')
print(df.head(10))