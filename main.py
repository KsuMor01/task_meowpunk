import datetime
import os
import pandas as pd
import sqlite3

from memory_profiler import profile

f = open('log.txt', 'w')  # memory usage log file


@profile(stream=f)
def file_not_found(filepath):
    """
    Helps to raise FileNotFoundError if the file not exists
    :param filepath: string path to the file
    :return: empty
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File {filepath} not found")


class Cheaters:

    @profile(stream=f)
    def __init__(self, cl_path, serv_path, db_path, tb_name, out_tb_name, col_names):
        """
        The consrtuctor of the Cheaters class. Loads the data from csv and db files to memory
        :param cl_path: string path to clients.csv
        :param serv_path: string path to server.csv
        :param db_path: string path to cheaters.db
        :param tb_name: string name of sql table in cheaters.db
        :param out_tb_name: string name the output db file
        :param col_names: list of string names of the output table columns
        """
        print('Loading data...')
        self.client = pd.read_csv(cl_path)
        self.server = pd.read_csv(serv_path)
        conn = sqlite3.connect(db_path)
        self.db_table = self.read_sql(conn, tb_name)
        self.df = self.create_empty_sqlite_table(out_tb_name, col_names)
        print('Data loaded!')

    @profile(stream=f)
    def edit_data(self, date_str):
        """
        Edits the data from files. Takes data for goal date, merges 2 csv files, filters cheaters and forms output table
        :param date_str: string the goal date in yyyy-mm-dd format
        :return: empty
        """
        print('Editing the data...')

        # the goal date
        def df_on_date(date, df):
            return df[df.timestamp.map(lambda x: datetime.datetime.fromtimestamp(x).date())
                      == datetime.datetime.fromisoformat(date).date()]

        self.client = df_on_date(date_str, self.client)
        self.server = df_on_date(date_str, self.server)

        # merge 2 tables
        self.df = self.client.merge(self.server, on='error_id', how='outer')

        # filter cheaters
        to_delete_df = self.client.merge(self.server, on='error_id', how='inner')
        to_delete_df = to_delete_df.merge(self.db_table, on='player_id', how='inner')

        to_delete_df = \
            to_delete_df[(to_delete_df.ban_time.map(lambda x: pd.to_datetime(x)) + datetime.timedelta(hours=24))
                         <= to_delete_df.timestamp_y.map(lambda x: datetime.datetime.fromtimestamp(x))]

        # delete filtered cheaters from the merged table
        self.df = self.df.loc[~self.df.player_id.isin(list(to_delete_df.player_id))]

        # format the output table
        self.df = self.df.drop('timestamp_x', axis=1)
        self.df = self.df.rename(columns={'timestamp_y': 'timestamp', 'description_x': 'json_client',
                                          'description_y': 'json_server'})
        print('The output table formed!')

    @profile(stream=f)
    def read_sql(self, conn, name):
        """
        Reads sql table into pandas Dataframe
        :param conn: the sqlite3 connection to db
        :param name: string the name of the table in db
        :return: pandas Dataframe of the table
        """
        sql_query = pd.read_sql_query(f'''
                                       SELECT
                                       *
                                       FROM {name}
                                       ''', conn)

        db = pd.DataFrame(sql_query)
        conn.close()
        return db

    @profile(stream=f)
    def create_empty_sqlite_table(self, out_tb_name, col_names):
        """
        Creates an empty table with column names and reads it in the pandas Dataframe
        :param out_tb_name: string name of the output table
        :param col_names: # list of string column names
        :return: pandas Dataframe of the empty db
        """
        conn = sqlite3.connect(f'{out_tb_name}.db')
        cur = conn.cursor()
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {out_tb_name}(
                           {col_names[0 ]} TEXT,
                           {col_names[1]} TEXT,
                           {col_names[2]} TEXT,
                           {col_names[3]} INT PRIMARY KEY,
                           {col_names[4]} TEXT,
                           {col_names[5]} TEXT);
                        """)
        conn.commit()
        return self.read_sql(conn, out_tb_name)

    @profile(stream=f)
    def write_pd_to_sqlite(self, out_tb_name):
        """
        Writes the pandas Dataframe into sqlite3 db file
        :param out_tb_name: string name of the output db file
        :return: empty
        """
        print('Writing table into database...')

        conn = sqlite3.connect(f'{out_tb_name}.db')
        self.df.to_sql(f'{out_tb_name}', conn, if_exists='replace', index=False)
        # print(list(conn.execute(f"select * from {out_tb_name}")))
        conn.commit()
        conn.close()
        print(f'The data has been written to {out_tb_name}.db')


@profile(stream=f)
def run():

    table_name = 'cheaters'
    output_table_name = 'output'

    columns_names = ['timestamp1', 'player_id', 'event_id', 'error_id', 'json_server', 'json_client']

    print('Enter csv files paths')
    print('First file path: ')
    client_path = input()
    # client_path = 'data/client.csv'
    file_not_found(client_path)

    print('Second file path: ')
    server_path = input()
    # server_path = 'data/server.csv'
    file_not_found(server_path)

    print('Enter database path:')
    database_path = input()
    # database_path = 'data/cheaters.db'
    file_not_found(database_path)

    print('Enter the date in format  YYYY-MM-DD')
    date_string = input()
    # date_string = '2021-03-07'
    datetime.datetime.strptime(date_string, '%Y-%m-%d')  # check if the string in the goal format

    ch = Cheaters(client_path, server_path, database_path, table_name, output_table_name, columns_names)
    ch.edit_data(date_string)
    ch.write_pd_to_sqlite(output_table_name)

    print('The memory log has been written to log.txt')


run()
