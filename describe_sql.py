import snowflake.connector as sc
print('Connecting')
try :
    cnn = snowflake.connector.connect(
        user = USER,
        password = PASSWORD,
        account = ACCOUNT,
        warehouse = WAREHOUSE,
        database = DATABASE,
        schema = SCHEMA
    )

    cs = cnn.cursor()
    sql - "SELECT * FROM PROJECT;"
    ds = cs.describe(sql)
    for i in ds:
        print(str(i))
    rs = cs.execute(sql)
    for r in rs:
        print(r)
except Exception as e:
    print(e)
finally:
    print('Disconnect')
    cs.close()
    print('Done')