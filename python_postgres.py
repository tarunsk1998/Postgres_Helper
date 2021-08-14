import psycopg2
from psycopg2._json import Json


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
                         'created_on TIMESTAMP NOT NULL,'
                         'pg_list_elements character varying[],'
                         'dict_elements jsonb)'
                         )
        self.con.commit()
        self.cur.close()

    def insert_data_into_python_postgres_table(self, username, password, email, list_elements, dict_elements):
        self.cur = self.con.cursor()
        self.cur.execute('INSERT into python_postgres_table (username, password, email, created_on, pg_list_elements,'
                         'dict_elements)'
                         'values (%s, %s, %s, current_timestamp::timestamp(0), %s, %s)', [username, password, email,
                                                                                          list_elements,
                                                                                          Json(dict_elements)])
        self.con.commit()
        self.cur.close()

    def select_list_items_unnest_with_other_columns(self):
        self.cur = self.con.cursor()
        self.cur.execute('SELECT user_id, password, UNNEST(pg_list_elements), dict_elements FROM python_postgres_table')
        result = self.cur.fetchall()
        print("Result : ",result)
        self.cur.close()

    def select_record_with_one_element_of_list(self):
        one_element_of_list = 'User0'
        self.cur = self.con.cursor()
        self.cur.execute('SELECT user_id, password, UNNEST(pg_list_elements), dict_elements FROM python_postgres_table '
                         'where %s = ANY(pg_list_elements)', [one_element_of_list])
        result = self.cur.fetchall()
        print("Result : ",result)
        self.cur.close()

    def select_record_with_known_key_element_of_Jsonb_in_postgres(self):
        one_known_key_of_jsonb_in_postgres = 'username'
        self.cur = self.con.cursor()
        self.cur.execute('SELECT user_id, password, pg_list_elements, dict_elements FROM python_postgres_table '
                         'where dict_elements ? %s', [one_known_key_of_jsonb_in_postgres])
        result = self.cur.fetchall()
        print("Result : ",result)
        self.cur.close()

    def close_con(self):
        self.con.close()


if __name__ == '__main__':
    connect_to_postgres = ConnectToPostgres()
    # connect_to_postgres.drop_table_python_postgres_table()
    # connect_to_postgres.create_python_postgres_table()

    # connect_to_postgres.deleting_all_values_from_python_postgres_table()

    for i in range(6):
        username, password, email = "User"+str(i), "user_password"+str(i), "user"+str(i)+"@email.com"
        list_elements = [username, password, email]
        dict_elements = {
            'username' : username,
            'password' : password,
            'email' : email
        }
        connect_to_postgres.insert_data_into_python_postgres_table(username, password, email, list_elements, dict_elements)

    connect_to_postgres.select_list_items_unnest_with_other_columns()
    connect_to_postgres.select_record_with_one_element_of_list()
    connect_to_postgres.select_record_with_known_key_element_of_Jsonb_in_postgres()

    connect_to_postgres.close_con()
