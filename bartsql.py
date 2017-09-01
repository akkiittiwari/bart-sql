# -*- coding: utf-8 -*-
# @author: Lenz

import zipfile
import xlrd
import pandas as pd
import psycopg2
import os

def unzip(tmpDir, dataDir):
    # unzip files and save it to tmpDir
    for year in xrange(2001, 2017):
        zipfilePath = dataDir + r'/ridership_' + str(year) + '.zip'
        zip_ref = zipfile.ZipFile(zipfilePath, 'r')
        zip_ref.extractall(tmpDir + r'/ridership_' + str(year))
        zip_ref.close()
    
def readxls(filename):
    # read xls and store the data into a list
    workbook = xlrd.open_workbook(filename)
    # ridershipInfo: [wday data, sat data, sun data, date]
    ridershipInfo = []
    for i in xrange(0, 3):
        sheet = workbook.sheet_by_index(i)
        # extract the date in the first row
        if i == 0: date = xlrd.xldate.xldate_as_datetime(sheet.row(0)[6].value, workbook.datemode)
        # load into dataframe
        rawDf = pd.read_excel(workbook, 0, index_col = 0, header=1, engine = 'xlrd')
        data = rawDf.loc[:'Entries',:'Exits']
        data = data.drop('Entries',0).drop('Exits',1)
        ridershipInfo.append(data)
    ridershipInfo.append(date)
    return ridershipInfo
        
def reshape(ridershipInfo):
    # extract month and year
    mon = ridershipInfo[3].month
    yr = ridershipInfo[3].year
    # iteratively extract data from 3 df (wday, sat and sun)
    ridershipTable = None
    for i in xrange(0,3):
        table = ridershipInfo[i]
        table['term'] = table.index
        table = table.melt(id_vars=['term'], value_vars=table.columns.drop('term'),
                           var_name='start', value_name='riders')
        
        # assign the daytype accroding to table index
        if i == 0: daytype = 'Weekday'
        elif i == 1: daytype = 'Saturday'
        else: daytype = 'Sunday'
        table['daytype'] = daytype
        
        try:
            ridershipTable = pd.concat([ridershipTable, table])
        except NameError: # when ridershipTable hasn't been created
            ridershipTable = table
            
    ridershipTable['mon'] = mon
    ridershipTable['yr'] = yr
        
    return ridershipTable

# A simple wrapper of psycopg2 to run sql command 
class PsqlConnector(object):
    def __init__(self, SQLConn=None, schema='cls', ahost='localhost',
               adbname='msan691', aUser='postgres', apass='142857'):
        if SQLConn:
            self.Sconn = SQLConn
        else:
            connString = "host='%s' dbname='%s' user='%s'password='%s'"%(ahost, adbname, aUser, apass)
            self.Sconn = psycopg2.connect(connString)
        self.Scur = self.Sconn.cursor()

    def run(self, cmds):
        err = 0 # error indicator
        for x in cmds:
            try:
                self.Scur.execute(x)
                self.Sconn.commit()
            except psycopg2.ProgrammingError as e:
                print "CAUTION FAILED: '%s'"%e
                self.Sconn.rollback()
                err = 1
        if err == 0: print 'Commit Success'

def ProcessBart(tmpDir, dataDir, SQLConn=None, schema = 'cls', table='bart'):
    
    # Process data and stored into a csv file
    unzip(tmpDir, dataDir)
    os.chdir(tmpDir)
    
    for folder in os.listdir('.'):
        os.chdir(folder)
        folderInside = 0
        if len(os.listdir('.')) == 1: #folder inside folder
            os.chdir(os.listdir('.')[0])
            folderInside = 1
        
        for xlsfile in os.listdir('.'):
            ridershipInfo = readxls(xlsfile)
            ridershipTable = reshape(ridershipInfo)
            
            try:
                allData = pd.concat([allData, ridershipTable])
            except NameError:
                allData = ridershipTable
        
        print folder + ' loaded' 
        os.chdir('..')
        if folderInside: os.chdir('..')
    
    os.chdir('..')
    allData.to_csv('BartRidership.csv', index=False)
    
    # Loading data into the database
    p = PsqlConnector(SQLConn=SQLConn, schema=schema)
    createTableCmds = ["drop table if exists cls."+table+";",
                        """create table cls."""+table+""" (
                        term varchar(2),
                        start varchar(2),
                        riders float,
                        daytype varchar(15),
                        mon int,
                        yr int);"""]
    p.run(createTableCmds)
    
    csvpath = os.path.abspath('BartRidership.csv')
    copyCSVCmds = ["copy cls." + table + " from '" + csvpath +
                   "' header csv delimiter ','"]
    p.run(copyCSVCmds)
    os.remove('BartRidership.csv')
        
ProcessBart('tmpDir', 'BART DATA')