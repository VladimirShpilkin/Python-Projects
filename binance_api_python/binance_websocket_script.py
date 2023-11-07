# Import necessary modules

import sqlite3  # For working with SQLite database
import json  # For JSON data manipulation
import uuid  # For generating unique identifiers

# Create a database management class
class DB:
    def __init__(self, db_file='market_stream.db'):
        # Initialize the class with the database file name
        self.db_file = db_file

    def init_db(self):
        # Initialize the database structure
        con = sqlite3.connect(self.db_file)
        cur = con.cursor()

        # Create a table for storing streaming data if it doesn't exist
        cur.execute('''CREATE TABLE IF NOT EXISTS Streaming
                       (ID text, Symbol text, Interval text, Close Value, Highest Value, Closing Value, Timestamp integer )''')

        con.commit()
        con.close()

    def log_db(self, message):
        # Log streaming data to the database
        parsed = json.loads(message)
        k = parsed['k']
        id = str(uuid.uuid4().hex)
        symbol = k['s']
        interval = k['i']
        close = float(k['c'])
        high = float(k['h'])
        low = float(k['l'])
        time = int(k['t'])

        print("log:")
        print("symbol:" + symbol)
        print("interval:" + interval)
        print("close:" + str(close))
        print("high:" + str(high))
        print("low:" + str(low))
        print("time:" + str(time))

        row = (id, symbol, interval, close, high, close, time)
        con = sqlite3.connect(self.db_file)
        cur = con.cursor()
        cmd = 'INSERT INTO Streaming(ID,Symbol, Interval, Close,Highest,Closing, Timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)'
        cur.execute(cmd, row)
        con.commit()
        con.close()

# Create a currency streaming class
import websocket  # For WebSocket communication
import pandas as pd  # For data analysis and manipulation
import time  # For adding time-related functionality
import os
import datetime

class CurrencyStreamer:
    def __init__(self, symbols, interval):
        # Initialize the currency streaming class with symbols and interval
        self.symbols = symbols
        self.interval = interval
        self.df = pd.DataFrame(columns=['symbol', 'price', 'time'])
        self.db = DB()  # Initialize the database

    def on_message(self, ws, message):
        # Handle incoming WebSocket messages
        print(message)
        self.db.log_db(message)
        self.update_dataframe(message)

    def on_error(self, ws, error):
        # Handle WebSocket errors
        print(error)

    def on_open(self, ws):
        # Handle WebSocket connection opened
        print("WebSocket connection opened")

    def on_close(self, ws, close_status_code, close_msg):
    # Handle WebSocket connection closed
        print('WebSocket closed')
        current_time = datetime.datetime.now()
        timestamp = current_time.strftime("%Y-%m-%d_%H-%M")
        self.connection_closed = True
        csv_filename = f'analytics_{timestamp}.csv'
        csv_directory = os.path.expanduser('~')  # User's home directory
        self.export_to_csv(csv_filename, csv_directory)

    def export_to_csv(self, csv_filename, csv_directory):
        if self.connection_closed:
            data_df = pd.DataFrame(self.get_data_from_db())
            csv_path = os.path.join(csv_directory, csv_filename)
            data_df.to_csv(csv_path, index=False)
            print(f'Data exported to {csv_path}')

    def update_dataframe(self, message):
        # Placeholder for updating the Pandas DataFrame with message data
        pass

    def start_websocket(self):
        # Start the WebSocket connection
        websocket.enableTrace(False)
        socket = 'wss://stream.binance.com:443/ws/' + '/'.join([f'{symbol}@kline_{self.interval}' for symbol in self.symbols])
        ws = websocket.WebSocketApp(socket, on_message=self.on_message, on_error=self.on_error, on_close=self.on_close, on_open=self.on_open)
        ws.run_forever()

# Create an instance of CurrencyStreamer and start the WebSocket connection
symbols = ['btcusdt', 'btcusdc', 'ethusdt', 'ethusdc', 'trxusdt', 'trxusdc', 'maticbtcusdt', 'maticusdc', 'arbbtcusdt', 'arbusdc']
interval = '1m'
streamer = CurrencyStreamer(symbols, interval)
streamer.start_websocket()


class DataChecker:
    def __init__(self,con):
        self.con=con

    def connection(self):
        self.con=sqlite3.connect('market_stream.db')
        self.con.cursor()
    
    def sql_command(self):
        self.command="SELECT * FROM Streaming"
        self.cursor=self.con.cursor()
        self.cursor.execute(self.command)
        self.rows=self.cursor.fetchall()

        for row in self.rows:
            print(row)
        self.con.close()

# Establish a database connection
import sqlite3
con = sqlite3.connect('market_stream.db')

# Create a DataChecker instance
data_checker = DataChecker(con)

# Perform the database operations
data_checker.connection()
data_checker.sql_command()

# Close the database connection
con.close()
