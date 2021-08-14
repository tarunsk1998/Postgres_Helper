import psycopg2


class ConnectToPostgres:
    # psycopg2.connect establishes connection to Postgres and stores it in self.con variable
    # self.con variable is a Class variable and can be accessed form all functions inside the class
    # default port no: 5432
    # A better way of connection is below:
    # postgres://username:password@hostname:port/database
    def __init__(self):
        self.con = psycopg2.connect(
            host='localhost',
            database='python_postgres_db',
            user='postgres',
            password='postgres'
        )

    def deleting_all_values_from_python_postgres_table(self):
        self.cur = self.con.cursor()
        self.cur.execute('DELETE from python_postgres_table')
        self.con.commit()
        self.cur.close()

    def drop_table_python_postgres_table(self):
        self.cur = self.con.cursor()
        self.cur.execute('DROP table python_postgres_table')
        self.con.commit()
        self.cur.close()

    def create_python_postgres_table(self):
        self.cur = self.con.cursor()
        self.cur.execute('CREATE TABLE IF NOT EXISTS python_postgres_table (user_id serial PRIMARY KEY, '
                         'username VARCHAR ( 50 ) UNIQUE NOT NULL, '
                         'password VARCHAR ( 50 ) NOT NULL,'
                         'email VARCHAR ( 255 ) UNIQUE NOT NULL,'
                         'created_on TIMESTAMP NOT NULL)'
                         )
        self.con.commit()
        self.cur.close()

    def insert_data_into_python_postgres_table(self, username, password, email):
        self.cur = self.con.cursor()
        self.cur.execute('INSERT into python_postgres_table (username, password, email, created_on)'
                         'values (%s, %s, %s, current_timestamp::timestamp(0))', [username, password, email])
        self.con.commit()
        self.cur.close()

    def close_con(self):
        self.con.close()

if __name__ == '__main__':
    connect_to_postgres = ConnectToPostgres()
    #connect_to_postgres.drop_table_python_postgres_table()
    connect_to_postgres.create_python_postgres_table()

    #connect_to_postgres.deleting_all_values_from_python_postgres_table()
    username, password, email = "User02", "user_password02", "user02@email.com"
    connect_to_postgres.insert_data_into_python_postgres_table(username, password, email)

    connect_to_postgres.close_con()

