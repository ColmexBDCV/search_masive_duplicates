import os
import sqlite3
import hashlib
from time import sleep

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d



bd = sqlite3.connect('codex2.db')
bd.row_factory = dict_factory


path_files = bd.execute(""" SELECT * FROM archivos WHERE checksum IS NULL ORDER BY id DESC""") 

for row in path_files.fetchall():
    os.system("clear")
    try:
        print("Deteneme con Ctrl+c")
        sleep(0)
    except KeyboardInterrupt:
        os.system("clear")
        print("Adios!")
        exit()
    else:
        os.system("clear")
        print("calculando Checksum",row['id']) 
        cs = hashlib.md5(open(row['archivo'],'rb').read()).hexdigest()
        bd.execute("UPDATE archivos set checksum = '{checksum}' WHERE id = {id}".format(checksum=cs, id=row['id']))
        bd.commit()
   