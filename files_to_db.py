import os
import sqlite3

def sqlite_insert(conn, table, row):
    cols = ', '.join('"{}"'.format(col) for col in row.keys())
    vals = ', '.join(':{}'.format(col) for col in row.keys())
    sql = 'INSERT INTO "{0}" ({1}) VALUES ({2})'.format(table, cols, vals)
    conn.cursor().execute(sql, row)
    conn.commit()


# list = os.listdir("/run/user/1000/gvfs/smb-share:server=codex2,share=archivosdigitales")

bd = sqlite3.connect('dropbox.db')
bd.execute(""" CREATE TABLE IF NOT EXISTS archivos (
                                        id integer PRIMARY KEY,
                                        archivo text NOT NULL,
                                        checksum text
                                    ); """)



for (path, directorios, archivos) in os.walk("/run/user/1000/gvfs/smb-share:server=codex2,share=archivosdigitales"):
    print(path)
    for a in archivos:
        sqlite_insert(bd, 'archivos', {
            'archivo': path+"/"+a})


bd.close()

print("Registro de archivos exitoso")
