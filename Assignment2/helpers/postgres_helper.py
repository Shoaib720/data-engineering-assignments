import psycopg2
import os
import csv

def postgres2csv(source_tables, destination_folder, connection_details):
  conn = psycopg2.connect(
    host=connection_details['host'],
    database=connection_details['database'],
    user=connection_details['user'],
    password=connection_details['password']
  )
  cursor = conn.cursor()
  for table in source_tables:
    cursor.execute(f'select * from {table}')
    data = cursor.fetchall()
    with open(os.path.join(destination_folder,f'{table}.csv'), "w") as csvfile:
      writer = csv.writer(csvfile)
      writer.writerow([i[0] for i in cursor.description])
      for row in data:
        writer.writerow(row)
    print(f'Imported table {table} in {os.path.join(destination_folder,f"{table}.csv")}')
  cursor.close()
  conn.close()