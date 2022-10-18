import snowflake.connector
import pandas as pd

df = pd.read_excel(DATA, "Sheet1", engine = 'openpyxl')
queries = len(df)
query = []
complete = []
for item in range(queries):
    query.append(item)
    complete.append(0)

print('Opening Database')
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
    process_complete = 0
    process_pass = 0
    for qry in query:
        sql = df.iloc[qry][0]
        cs.execute_async(sql)
        query[qry] = cs.sfqid
    while process_complete == 0:
        item = -1
        process_pass +=1
        if sum(complete) == queries or process_pass == 10:
            process_complete = 1
        for result in query:
            item +=1
            if complete[item] == 0:
                print('Result for : +' str(result))
                status = cnn.get_query_status(result)
                if str(status) == 'QueryStatus':
                    complete[item] = 1
                    cs.get_results_from_sfqid(result)
                    recs = cs.fetchall()
                    for rec in recs:
                        print(rec)
                else:
                    time.sleep(1)
except Exception as e:
    print(e)
finally:
    cnn.close()
