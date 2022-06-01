import csv
import sqlite3

def main():
  sqlite_connection = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
  cursor= sqlite_connection.cursor()
  cursor.execute(f"Select sum(duration) from polaczenia")
  print(cursor.fetchall()[0])

  cursor.execute('''CREATE TABLE polaczenia (from_subscriber data_type INTEGER, 
                  to_subscriber data_type INTEGER, 
                  datetime data_type timestamp, 
                  duration data_type INTEGER , 
                  celltower data_type INTEGER);''') 

  with open('polaczenia_duze.csv','r') as file: 
    reader = csv.reader(file, delimiter = ";") 
    header = next(reader) 
    rows = [x for x in reader]
    cursor.executemany("INSERT INTO polaczenia (from_subscriber, to_subscriber, datetime, duration , celltower) VALUES (?, ?, ?, ?, ?);", rows)
    sqlite_connection.commit()
    
    
if __name__ == "__main__":
    main
