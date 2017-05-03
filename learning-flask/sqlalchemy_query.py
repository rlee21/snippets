from sqlalchemy import create_engine
engine = create_engine('mysql://localhost/flask')
connection = engine.connect()
result = connection.execute("select * from users")
for row in result:
        #print("firstname:", row['firstname'])
        print(row)
connection.close()
