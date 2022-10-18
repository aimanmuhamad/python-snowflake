import snowflake.connector

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

sql = ("Create or Replace Procedure new_project("
       "project_no float, project_name text, project_desc text)\r\n"
       "Returns varchar\r\n"
       "Language Javascript\r\n"
       "As\r\n"
       "$$\r\n"
       "try {\r\n"
       "    var cmd = `Insert Into project Values (:1, :2, :3);`\r\n"
       "    var result = snowflake.execute (\r\n"
       "    {\r\n"
       "      sqlText: cmd,\r\n"
       "      binds: [PROJECT_NO, PROJECT_NAME, PROJECT_DESC]\r\n"
       "    }\r\n"
       "    );\r\n"
       "    return `Success.`\r\n"
       "    }\r\n"
       "catch (err) {\r\n"
       "    return `Error: ` + err;\r\n"
       "}\r\n"
       "$$;")

cs = cnn.cursor()
cs.execute(sql)
sql = "Call new_project(9112, 'Apple Orchard', 'Create an apple orchard!');"
cs.execute(sql)
sql = "Select * From project order by id desc;"
cs.execute(sql)
for row in cs:
    print(row)
print('closing connection..')
cs.close()
cnn.close()

cs.close()
cnn.close()
print('Closing connection')