import snowflake.connector as sc

print("Open database")

try :
    cnn = sc.connect(
        user = USER,
        password = PASSWORD,
        account = ACCOUNT,
        warehouse = WAREHOUSE,
        database = DATABASE,
        schema = SCHEMA)
    cs = cnn.cursor()
    # merge statement
    print('merging')
    sql = ("MERGE INTO PROJECT using"
           "PROJECT_UPDATE"
           "ON PROJECT.ID = PROJECT_UPDATE.ID"
           "WHEN MATCHED THEN"
           "UPDATE SET PROJECT.PROJECT_NAME = PROJECT_UPDATE.PROJECT_NAME"
           "           PROJECT.PROJECT_DESCRIPTION = PROJECT_UPDATE.PROJECT_DESCRIPTION"
           "WHEN NOT MATCHED THEN"
           "Insert (ID, PROJECT_NAME, PROJECT_DESCRIPTION)"
           "        VALUES (PROJECT_UPDATE.ID,"
           "                PROJECT_UPDATE.PROJECT_NAME,"
           "                PROJECT_UPDATE.PROJECT_DESCRIPTION")
    cs.execute(sql)
    cnn.commit()
    print('Selecting results')
    sql = "SELECT * FROM PROJECT_UPDATE;"
    recs = cs.execute(sql)
    for rec in recs:
        print(rec)
except Exception as e:
    print(e)
finally:
    print('Disconnecting')
    cnn.close()
    print('Closed...')
print('Done')