import os
import sqlite3
import hashlib
import csv
from time import sleep

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d



bd = sqlite3.connect('codex2.db')
# bd.row_factory = dict_factory


path_files = bd.execute(""" SELECT DISTINCT a.id, a.archivo, a.checksum
                        FROM archivos a
                        INNER JOIN archivos b ON b.checksum = a.checksum
                        WHERE a.archivo <> b.archivo
                        ORDER BY a.checksum DESC""") 

# for row in path_files.fetchall():
#     print(row['id'], row['archivo'],row['checksum'])


rows = path_files.fetchall()
fp = open('duplicados_codex2.csv', 'w')
myFile = csv.writer(fp)
myFile.writerows(rows)
fp.close()