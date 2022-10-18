import snowflake.connector

cnn = snowflake.connector.connect(
    user = USER,
    password = PASSWORD,
    account = ACCOUNT,
    warehouse = WAREHOUSE,
    database = DATABASE,
    schema = SCHEMA
)

print('Connect database')
cs = cnn.cursor()

try :
    sql_staff = ("INSERT INTO staff (ID,LastName, FirstName, DateofBirth)"
                 "VALUES (806, 'Chang', 'Nancy', '1984-05-30'")
    sql_project = ("INSERT INTO project (ID, projectName, projectDescription"
                   "VALUES (9113,'Project_Name', 'project_description'")
    sql_junction  = ("INSERT INTO project_staff (ID, projectID, staffID, DateStart"
                     "VALUES (100,9113,806,'2022-10-18")
    cs.execute(sql_staff)
    cs.execute(sql_project)
    cs.execute(sql_junction)
    cnn.commit()
except Exception as e:
    cnn.rollback()
    raise e
finally:
    cnn.close()
    print('Operation completed')