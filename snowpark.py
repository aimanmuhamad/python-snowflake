from snowflake.snowpark import Session

print('Connecting')

cnn_params = {
    "account" : snowacct,
    "user" : snowuser,
    "password" : snowpass,
    "warehouse" : "project_warehouse",
    "database" : "project_database",
    "schema" : "project_schema"
}

print('Session..')
sess = Session.builder.configs(cnn_params).create()
print('test data...')
print(sess.sql("SELECT * FROM projectl")).collect())
sess.close()
print('Done..')