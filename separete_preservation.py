import sqlite3
import csv


def dict2csv(dictlist, csvfile):
    """
    Takes a list of dictionaries as input and outputs a CSV file.
    """
    f = open(csvfile, 'w')

    fieldnames = dictlist[0].keys()

    csvwriter = csv.DictWriter(f, delimiter=',', fieldnames=fieldnames)
    csvwriter.writerow(dict((fn, fn) for fn in fieldnames))
    for row in dictlist:
        csvwriter.writerow(row)
    f.close()


def has_preservation_digitalization(i, v):

    digi = False

    pres = False

    for text in v:
        if '/preservacion/' in text:
            pres = True
        if '/digitalizacion/' in text:
            digi = True

    for text in v:
        if digi and pres:
            deletables.append({'archivo': text, 'checksum': i})


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


bd = sqlite3.connect('codex2.db')
bd.row_factory = dict_factory


path_files = bd.execute(""" SELECT DISTINCT a.id, a.archivo, a.checksum
                        FROM archivos a
                        INNER JOIN archivos b ON b.checksum = a.checksum
                        WHERE a.archivo <> b.archivo
                        ORDER BY a.checksum DESC """)

values = {}

deletables = []

for row in path_files.fetchall():

    if row['checksum'] not in values:
        values[row['checksum']] = [row['archivo']]
    else:
        values[row['checksum']].append(row['archivo'])


for i, v in values.items():
    has_preservation_digitalization(i, v)

dict2csv(deletables, 'borrables.csv')
