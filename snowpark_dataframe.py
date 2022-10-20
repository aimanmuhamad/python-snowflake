from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

print('Connecting')

sess = None
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
    df_project = sess.table("project")
    df_project.show()
    df_staff = sess.table("staff")
    df_staff.show()
    df_project_staff = sess.table("project_staff")
    df_project_staff.show()
    df_view = sess.table("vw_project_staff".filter(col("LASTNAME")=="Cooper").
                         select(col("PROJECT_NAME"), col("DATESTART"))
    df_view.show()
    df_pandas = df_view.to_pandas()
    print(df_pandas.head(10))
    df_view.write.mode("overwrite").save_as_table("cooper_projects")
    df_cooper = sess.table("cooper_projects")
    df_cooper.show()
except Exception as e:
    print(e)
finally:
    if sess:
        sess.close()
        print('Connection closed')
print('Done')