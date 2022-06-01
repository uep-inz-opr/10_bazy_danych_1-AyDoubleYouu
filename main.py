import csv
import sqlite3

polaczenia_duze = input()

class ReportGenerator:
  def __init__(self,connection, escape_string = "(%s)"):
    self.connection = connection
    self.report_text = None
    self.escape_string = escape_string

  def generate_report(self):
    cursor = self.connection.cursor()
    sql_query = f"Select sum(duration) from polaczenia"
    cursor.execute(sql_query)
    result = cursor.fetchone()[0]
    self.report_text = result

  def get_report(self):	
    return self.report_text
       
if __name__ == "__main__":

  sqlite_connection = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
  cur= sqlite_connection.cursor()

  cur.execute('''CREATE TABLE polaczenia (from_subscriber data_type INTEGER, 
                  to_subscriber data_type INTEGER, 
                  datetime data_type timestamp, 
                  duration data_type INTEGER , 
                  celltower data_type INTEGER);''') 

  with open(polaczenia_duze,'r') as file: 
        reader = csv.reader(file, delimiter = ";") 
        next(reader, None) 
        rows = [x for x in reader]
        cur.executemany("INSERT INTO polaczenia (from_subscriber, to_subscriber, datetime, duration , celltower) VALUES (?, ?, ?, ?, ?);", rows)
        sqlite_connection.commit()

        rg = ReportGenerator(sqlite_connection, escape_string="?")
        rg.generate_report()
        print(rg.get_report())