import pandas as pd
import sqlite3


class Cheaters:

    def __init__(self):
        self.client = None
        self.server = None

    def load_csv(self, client_path, server_path):
        try:
            self.client = pd.read_csv(client_path)
            self.server = pd.read_csv(server_path)
        except FileNotFoundError:
            print('FileNotFoundError! File does not exists')
        print('Data loaded!')

    def edit_data(self, date):
        pass

    def csv_to_sqlite(self):
        pass


ch = Cheaters()
ch.load_csv('data/client.csv', 'data/server.csv')
ch.edit_data((20, 2, 2021))

