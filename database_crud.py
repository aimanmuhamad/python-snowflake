import snowflake.connector

cnn = snowflake.connector.connect(
    user = USER,
    password = PASSWORD,
    account = ACCOUNT
)

cs = cnn.cursor()

try:
    cs.execute(SELECT current_version())
    row = cs.fetchone()
    print(row[0])
    print('Creating warehouse')
    sql = "CREATE WAREHOUSE IF NOT EXIST project_warehouse"
    cs.execute(sql)
    print("Creating database")
    sql = "CREATE DATABASE IF NOT EXISTS project_databases"
    cs.execute(sql)
    print('using databases')
    sql = "USE DATABASE project_databases"
    cs.execute(sql)
    print('creating schema')
    sql = "CREATE SCHEMA IF NOT EXISTS project_schema"
    cs.execute(sql)
    print('creation complete')
    sql ="USE WAREHOUSE project_warehouse"
    cs.execute(sql)
    sql = "USE DATABASE project_databases"
    cs.execute(sql)
    sql = "USE SCHEMA project_schema"
    cs.execute(sql)
    print("Create a table")
    sql = ("CREATE OR REPLACE TABLE project_comments"
           "(ID integer, comments string")
    cs.execute(sql)
    print('insert a few rows')
    sql = ("INSERT into project_comments (ID, comments"
           "VALUES (1, 'my comments about the project'")
    cs.execute(sql)
    print('read some rows')
    sql = "SELECT * FROM project_comments"
    cs.execute(sql)
    for row in cs.fetchall():
        print(row)
    print('complete')
finally:
    cs.close()

cnn.close()
