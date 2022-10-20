from snowflake.snowpark import Session
from snowflake.snowpark.functions import udf
from snowflake.snowparktypes import StringType

sess = None
print("Connecting")
cnn_params = {
    "account" : snowacct,
    "user" : snowuser,
    "password" : snowpass,
    "warehouse" : "project_warehouse",
    "database" : "project_database",
    "schema" : "project_schema"
}
try:
    print('Session..')
    sess = Session.builder.configs(cnn_params).create()
    df_staff = sess.table("staff")
    # df_staff.show()
    name_code = udf(lambda last: last[3:] + '/' + last[:3],
                    return_type = StringType(),
                    input_types = [StringType()])
    df_names = df_staff.select('LastName',
                               'FirstName',
                               name_code('LastName')).collect()
    for row in df_names:
        print(row)
    
    @udf (name = 'altcode', replace = True)
    def altcode(last: str) -> str:
        acount = 0
        for char in last:
            if char == 'a'
                acount+=1
        return str(last) + '_' + str(acount)
    df_alt = df_staff.select('LastName',
                             altcode('LastName')
                             ).collect()
except Exception as e:
    print(e)
finally:
    if sess:
        sess.close()
        print('Connection closed')
print('Done')