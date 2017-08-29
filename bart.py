import psycopg2
import glob
import xlrd
import pandas
import zipfile
import os
import shutil




def extract_zip(saveDir, mainDir):
    for filename in glob.glob(os.path.join(mainDir, '*.zip')):
#       print filename
        zip = zipfile.ZipFile(filename)
        zip.extractall(saveDir)




def melt_df(rider_data, mon, yr, day_type):

    rider_data = rider_data.stack().reset_index()
    rider_data.columns = ['terms', 'start', 'riders']
    rider_data['year'] = yr
    rider_data['month'] = mon
    rider_data['day_type'] = day_type
    return rider_data
    
def read_xls(saveDir, sheets_name):
    final_df = pandas.DataFrame()
    for root, dirs, files in os.walk(saveDir):
        for file in files:
            book = xlrd.open_workbook(root+'/'+file)
            sheets = book.sheets()

            for column in range(sheets[0].ncols):
                    if "Exits" == sheets[0].cell(1, column).value:
                        col_end = column-1

            daytype = sheets[0].cell(0,3).value.lower()
           
            for word in ['weekday', 'sunday', 'saturday']:
                if word in daytype:
                    daytype = word
            date = sheets[0].cell(0, 6).value
            year, month, day, hour, minute, second = xlrd.xldate_as_tuple(date, book.datemode)


            for sheet in sheets_name:
                rider_df = pandas.read_excel(root+'/'+file, sheet, header=1, skiprows=0, index_col=0,
                                         skip_footer=1, parse_cols=col_end)

            melted_riders = melt_df(rider_df, month, year, daytype)
            final_df.append(melted_riders)
    print final_df    




def ProcessBart(saveDir, mainDir, SQLConn, schema, table):
    extract_zip(saveDir, mainDir)
    sheets = ['Wkdy Adj OD', 'Sat Adj OD', 'Sun Adj OD']
    read_xls(saveDir, sheets)



tmpDir = "tmpDIR"
dataDir = "BART_DATA"



ProcessBart(tmpDir, dataDir, SQLConn=None, schema="cls", table="bart")
