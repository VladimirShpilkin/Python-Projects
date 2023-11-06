import sqlite3
import json
import uuid #to create unique identifiers


def init_db():
    con=sqlite3.connect('market_stream.db')
    cur=con.cursor()

    cur.execute(''' CREATE TABLE IF NOT EXISTS Streaming
                (ID text, Symbol text, Interval text, Close Value, Highest Value, Closing Value, Timestamp integer ) ''')
    con.commit()
    con.close()

def log_db(message):
    parsed=json.loads(message)#message from binance is parsed into json
    k=parsed['k']
    id=str(uuid.uuid4().hex)
    symbol=k['s']
    interval=k['i']
    close=float(k['c'])
    high=float(k['h'])
    low=float(k['l'])
    time=int(k['t'])

    print("log:")
    print("symbol:"+ symbol)
    print("interval:"+ interval)
    print("close:"+str(close) )
    print("high:"+str(high) )
    print("low:"+str(low) )
    print("time:"+str(time) )

    row=(id, symbol, interval, close, high, close, time )
    con=sqlite3.connect('market_stream.db')
    cur=con.cursor()
    cmd='insert into Streaming(ID,Symbol, Interval, Close,Highest,Closing, Timestamp) values (?,?,?,?,?,?,?)'
    cur.execute(cmd,row)
    con.commit()
    con.close()