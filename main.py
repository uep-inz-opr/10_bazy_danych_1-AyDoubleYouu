import csv
import sqlite3

def main():
  sqlite_con = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
  cur= sqlite_con.cursor()


  cur.execute('''CREATE TABLE polaczenia (from_subscriber data_type INTEGER, 
                  to_subscriber data_type INTEGER, 
                  datetime data_type timestamp, 
                  duration data_type INTEGER , 
                  celltower data_type INTEGER);''') 

  with open('polaczenia_duze.csv','r') as fin: 
    reader = csv.reader(fin, delimiter = ";") 
    next(reader, None) 
    rows = [x for x in reader]
    cur.executemany("INSERT INTO polaczenia (from_subscriber, to_subscriber, datetime, duration , celltower) VALUES (?, ?, ?, ?, ?);", rows)
    sqlite_con.commit()
    print(cur.fetchall()[0])
if __name__ == "__main__":
    main
