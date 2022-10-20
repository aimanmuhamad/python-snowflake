import snowflake.connector as sc
improt json

print('Connecting')
try :
    cnn = sc.connect(
        user = USER,
        password = PASSWORD,
        account = ACCOUNT,
        warehouse = WAREHOUSE,
        database = DATABASE,
        schema = SCHEMA)
    cs = cnn.cursor()
    print('loading json')
    file = open('customer1.json')
    json = file.read()
    file.close()
    sql = (r"INSERT INTO CUSTOMER (CUST_JSON)"
           " SELECT parse_json('" + json + "');"
    cs.execute(sql)
    cnn.commit()
    print('Select data')
    sql = "SELECT CUST_JSON:fullName::string,
          "CUST_JSON:age::int,"
          "CUST_JSON:gender::string" 
          "FROM CUSTOMER;"
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