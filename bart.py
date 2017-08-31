import psycopg2
import glob
import xlrd
import pandas
import zipfile
import os
import shutil




def extract_zip(saveDir, mainDir):
    for filename in glob.glob(os.path.join(mainDir, '*.zip')):
        zip = zipfile.ZipFile(filename)
        zip.extractall(saveDir)




def melt_df(rider_data, mon, yr, day_type):

    rider_data = rider_data.stack().reset_index()
    rider_data.columns = ['terms', 'start', 'riders']
    rider_data.insert(0, 'month', mon)
    rider_data.insert(1, 'year', yr)
    rider_data.insert(2, 'day_type', day_type)
    return rider_data



def read_xls(saveDir):
    final_df = pandas.DataFrame()
    a = list()
    for root, dirs, files in os.walk(saveDir):
        for file in files:
            book = xlrd.open_workbook(root+'/'+file)
            sheets = book.sheets()

            for i in range(0,3):
                sh = sheets[i]
                for column in range(sh.ncols):
                        if "Exits" == sh.cell(1, column).value:
                            col_end = column-1

                row_end = 0

                for row in range(sh.nrows):
                        if "Entries" == sh.cell(row, 0).value:
                            row_end = row-2

                daytype = sheets[0].cell(0, 3).value.lower()

                for word in ['weekday', 'sunday', 'saturday']:
                    if word in daytype:
                        daytype = word
                date = sheets[0].cell(0, 6).value
                year, month, day, hour, minute, second = xlrd.xldate_as_tuple(date, book.datemode)



                rider_df = pandas.read_excel(root+'/'+file, i, header=1, skiprows=0, index_col=0,
                                             skip_footer=1, parse_cols=col_end)
                rider_df = rider_df.head(row_end)

                melted_riders = melt_df(rider_df, month, year, daytype)
                final_df = final_df.append(melted_riders, ignore_index=True)
    final_df.to_csv('final_df.csv', mode='w', header=False, index=False)
    return final_df




def connect_db():
    try:
        conn = psycopg2.connect("dbname='MSAN691' user='postgres' host='localhost' port='5433' password='av'")
    except:
        print "I am unable to connect to the database"

    print "--connected---"
    return conn




def table_ops(conn, schema, table):
    cur = conn.cursor()
    cur.execute("""create schema IF NOT EXISTS cls""")
    cur.execute("""CREATE TABLE IF NOT EXISTS %s.%s  (mon int, yr int, daytype varchar(15), start varchar(2), 
                term varchar(2), riders float);""" % (schema, table))

    f = open('final_df.csv', 'r')
    cur.copy_from(f, 'cls.bart', sep=',')
    f.close()
    conn.commit()
    conn.close()
    print "---done---"




def ProcessBart(saveDir, mainDir, SQLConn, schema, table):
    extract_zip(saveDir, mainDir)
    final_df = read_xls(saveDir)
    conn = connect_db()
    table_ops(conn, schema, table)


tmpDir = "tmpDIR"
dataDir = "BART_DATA"


ProcessBart(tmpDir, dataDir, SQLConn=None, schema="cls", table="bart")


