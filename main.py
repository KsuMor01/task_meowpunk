import datetime
import pandas as pd
import sqlite3

from typing import Optional

class Cheaters:

    def __init__(self, client_path, server_path):
        self.client = pd.read_csv(client_path)
        self.server = pd.read_csv(server_path)
        self.df = Optional[pd.DataFrame]
        self.db_table = Optional[pd.DataFrame]

    def edit_data(self, date):

        # merge 2 tables
        self.df = self.client.merge(self.server, on='error_id', how='outer')

        # read database of cheaters
        self.read_sql()

        # find cheaters ids to delete

        to_delete_df = self.client.merge(self.server, on='error_id', how='inner')
        to_delete_df = to_delete_df.merge(self.db_table, on='player_id', how='inner')
        print(to_delete_df.shape)

        print(to_delete_df.columns)

        to_delete_df = \
            to_delete_df[(to_delete_df.ban_time.map(lambda x: pd.to_datetime(x)) + datetime.timedelta(hours=24))
                         <= to_delete_df.timestamp_y.map(lambda x: datetime.datetime.fromtimestamp(x))]

        print(to_delete_df.shape)

        # delete cheaters from the merged table

        self.df = self.df.loc[~self.df.player_id.isin(list(to_delete_df.player_id))]
        print(self.df.shape)

        print(self.df.columns)

        # format the output table

        self.df = self.df.drop('timestamp_y', axis=1)

        self.df = self.df.rename(columns={'timestamp_x': 'timestamp', 'description_x': 'json_client',
                                          'description_y': 'json_server'})
        print(self.df.columns)



    def read_sql(self):
        conn = sqlite3.connect(database_path)
        sql_query = pd.read_sql_query(f'''
                                       SELECT
                                       *
                                       FROM {table_name}
                                       ''', conn)

        self.db_table = pd.DataFrame(sql_query)
        conn.close()

    def csv_to_sqlite(self):
        pass


client_path = 'data/client.csv'
server_path = 'data/server.csv'
database_path = 'data/cheaters.db'
table_name = 'cheaters'
date_str = '2021-03-07'
date_time = datetime.datetime.fromisoformat(date_str)

ch = Cheaters(client_path, server_path)
ch.read_sql()
ch.edit_data(date_time)

