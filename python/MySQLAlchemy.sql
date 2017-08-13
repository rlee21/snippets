--------------------------------
-- MySQL and SQLAlchemy Snippets
--------------------------------

-- Python MySQL Dependencies
sudo apt-get install python-dev python3-dev
sudo apt-get install libmysqlclient-dev
pip install pymysql
pip install mysqlclient

-- Create MySQL user and grant
CREATE USER 'rlee'@'localhost' IDENTIFIED BY '';
update user set password=PASSWORD("") where User='rlee';

GRANT ALL ON sandbox.* TO 'rlee'@'localhost';

-- Create MySQL table
create table users (
uid serial primary key,
firstname varchar(100) not null,
lastname varchar(100) not null,
email varchar(120) not null unique,
pwdhash varchar(100) not null
);

-- SQLAlchemy connection and query
from sqlalchemy import create_engine
engine = create_engine('mysql://localhost/sandbox')
connection = engine.connect()
result = connection.execute("select * from users")
for row in result:
        #print("firstname:", row['firstname'])
        print(row)
connection.close()

-- SQLAlchemy DML
INSERT INTO users (firstname, lastname, email, pwdhash) VALUES (%s, %s, %s, %s)