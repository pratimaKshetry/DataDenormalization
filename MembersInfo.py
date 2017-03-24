# -*- coding: utf-8 -*-
"""
Created on Thu Mar 24 15:43:46 2017

@author: Pratima
"""
import os
import sys
import csv
import pymysql

def main():
    db=pymysql.connect("localhost","root","admin","opiodDB")
    cursor=db.cursor()

    queryCreate="drop table if exists `opiodDB`.`members`"
    cursor.execute(queryCreate)
    queryCreate="CREATE TABLE `opiodDB`.`members` (`id` INT NOT NULL AUTO_INCREMENT,`STATEFP` VARCHAR(10) NOT NULL,`STATE` VARCHAR(45) NOT NULL, `STATEABBREVIATION` VARCHAR(45) DEFAULT NULL, `YEAR` YEAR DEFAULT NULL, `DATASOURCE` VARCHAR(45) NOT NULL, `DATALEVEL` VARCHAR(45) NOT NULL, `CONGRESSIONALDISTRICT` VARCHAR(45) DEFAULT NULL,`INDICATOR` VARCHAR(255) NOT NULL,`Data` VARCHAR(235) DEFAULT NULL,PRIMARY KEY (`id`))"
    cursor.execute(queryCreate)
    db.commit()
    extractLinkersInfoFromCSV(cursor)
    db.commit()
    db.close()
    return 'Complete!'

def extractLinkersInfoFromCSV(cursor):
    path='./members/'
    filenames=[
               filename
               for filename in os.listdir(path)
               if filename.endswith('.csv')]

    for filename in filenames:
        print('Processing'+filename+'...')
        exctractLinkersInfo(path+filename,cursor)

def exctractLinkersInfo(filename,cursor):
    #db=pymysql
    colHeader={'STATEFP':0,'STATE':1,'STATEABBREVIATION':2,'YEAR':3,'DATASOURCE':4, 'DATALEVEL':5, 'CONGRESSIONALDISTRICT':6,'INDICATOR':7,'DATA':8}
    linkerFile=open(filename,'r')
    linkerReadFile=csv.reader(linkerFile)
    query="""INSERT INTO members (STATEFP,STATE,STATEABBREVIATION,YEAR,DATASOURCE,DATALEVEL,CONGRESSIONALDISTRICT,INDICATOR,DATA) VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s)"""
    isFirstRow=True
    for row in linkerReadFile:
        if (isFirstRow):
            isFirstRow=False
        else:
            col1=unicodeconvert(str(row[colHeader['STATEFP']]).strip())
            col2=unicodeconvert(str(row[colHeader['STATE']]).strip())
            col3=unicodeconvert(str(row[colHeader['STATEABBREVIATION']]).strip())
            col4=unicodeconvert(str(row[colHeader['YEAR']]).strip())
            col5=unicodeconvert(str(row[colHeader['DATASOURCE']]).strip())
            col6=unicodeconvert(str(row[colHeader['DATALEVEL']]).strip())
            col7=unicodeconvert(str(row[colHeader['CONGRESSIONALDISTRICT']]).strip())
            col8=unicodeconvert(str(row[colHeader['INDICATOR']]).strip())
            col9=unicodeconvert(str(row[colHeader['DATA']]).strip())
            data=(col1,col2,col3,col4,col5,col6,col7,col8,col9)
            print(str(row))
            cursor.execute(query,data)
    linkerFile.close()
    print ('Finished Processing')

def unicodeconvert(line):
    # Three!
    line = line.replace('\xe2\x80\x93', '-').replace('\xe2\x80\x96', '"').replace('\xef\x82\xb7', '-').replace('\xe2\x80\x95', '"').replace('\xe2\x80\x94', '-').replace('\xe2\x80\x98', "'").replace('\xe2\x80\xa2', '\n-').replace('\xe2\x80\x9c', '"').replace('\xe2\x80\xa6', '...').replace('\xe2\x80\x99', "'").replace('\xe2\x84\xa2', '(TM)').replace('\xe2\x80\x9d', '"').replace('\xc3\xa9', 'e').replace('\xe2\x82\xac', 'E')
    # Two!
    line = line.replace('\xc2\xb4', "'").replace('\xc3\x83', 'A').replace('\xc3\xa7', 'c').replace('\xc3\xa3', 'a').replace('\xc2\xa9', '(c)').replace('\xc3\xb5', 'o').replace('\xc3\xb3', 'o').replace('\xc3\xa0', 'a').replace('\xc3\xba', 'u').replace('\xc2\xbd', '1/2').replace('\xc3\xa8', 'e').replace('\xc3\xaf', 'i').replace('\xc2\xbe', '3/4').replace('\xc2\xac', '').replace('\xc2\xa3', 'L').replace('\xc2\xae', '(R)').replace('\xc3\xb4', 'o').replace('\xc3\xae', 'i').replace('\xc2\xab', '<<').replace('\xc2\xbb', '>>').replace('\xc2\xb0', ' deg').replace('\xc3\xab', 'e').replace('\xc2\xb3', '3').replace('\xc2\xb5', 'u').replace('\xc3\xaa', 'e').replace('\xc3\xa2', 'a').replace('\xc5\x93', 'oe').replace('\xc3\x89', 'E').replace('\xc3\xad', 'i').replace('\xc3\xb1', 'n').replace('\xc2\xa7', '\n-').replace('\xc2\xbc', '1/4').replace('\xc3\x81', 'A').replace('\xc3\xbc', 'u').replace('\xc3\x8f', 'I').replace('\xc3\xbb', 'u').replace('\xc3\xa1', 'a').replace('\xc2\xa0','.')
    # One!
    line = line.replace('\xae', '').replace('\xbd', '').replace('\xbe', '').replace('\x1c', '"').replace('\x1d','"').replace('\xb7', '-').replace('\xe1', 'a').replace('\xef','i').replace('\xe9', 'e').replace('\xc9', 'E').replace('\xfa', 'u').replace('\xf4', 'o').replace('\xbf', 'e').replace('\xe7', 'c').replace('\xe8', 'e').replace('\xe0', 'a').replace('\xf1', 'n').replace('\xe3', 'a').replace('\xf3', 'o').replace('\xea', 'e').replace('\xe2', 'a').replace('\xf5', 'o')
    line = line.replace("\x96", "-")
    #line=line.replace('\u2013', '')
    try:
        line=line.encode('utf-8', 'replace')
    except UnicodeDecodeError:
        temp=repr(line)
    return line

if __name__ == '__main__':
    status = main()
    sys.exit(status)