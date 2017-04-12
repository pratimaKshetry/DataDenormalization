# -*- coding: utf-8 -*-
"""
Created on Thu Apr 03 09:27:01 2017
"""
import sys
import urllib3
from bs4 import BeautifulSoup
import urllib
import hashlib
import certifi
import ssl
import pymysql

http = urllib3.PoolManager(
    cert_reqs='CERT_REQUIRED', # Force certificate check.
    ca_certs=certifi.where(),  # Path to the Certifi bundle.
)
urllib3.disable_warnings()

db=pymysql.connect("localhost","root","root","opiodDB")

def getRecordCount(url,cursor):
    global db
    cursor2=db.cursor()
    query="select count(*) as countRec from opioddb.updatecheck WHERE URL like'%"+url+"%'"
    cursor2.execute(query)
    rows=cursor2.fetchall()
    recCount=0
    for row in rows:
        recCount=int(row[0])
    return (recCount)

def insertNewData(newRow):
    global db
    cursor1=db.cursor()
    query="Insert into updatecheck(URL,md5Hash) VALUES (%s,%s)"
    col1=str(newRow['URL'])
    col2=str(newRow['hashKey'])
    data=(col1,col2)
    cursor1.execute(query,data)
    db.commit()

def updateQuery(url,hashKey,cursor):
        query="UPDATE opiodDB.updatecheck SET md5Hash='"+hashKey+"' where URL='"+url+"'"
        cursor.execute(query)

def checkForData(newRow,cursor):
    recCount=getRecordCount(newRow["URL"],cursor)
    if(recCount==0):
        insertNewData(newRow)
    else:
        updateQuery(str(newRow["URL"]),str(newRow["hashKey"]),cursor)

def censusCountyDataUpdate(cursor):
    newRow={}
    newRow["URL"]='CENSUS'
    soup_url=BeautifulSoup(urllib.request.urlopen('https://www.census.gov/geo/reference/county-changes.html').read())
    result=soup_url.find("div", {"id":"middle-column"}).get_text().encode('utf-8')
    m = hashlib.md5()
    m.update(result)
    newRow["hashKey"]=(str(m.hexdigest()))
    checkForData(newRow,cursor)

def MedicaidEnrollDataUpdate(cursor):  #Requires html parser in python
    newRow={}
    newRow["URL"]='MEDICAID'
    soup_url = BeautifulSoup(urllib.request.urlopen('https://www.medicaid.gov/medicaid/program-information/medicaid-and-chip-enrollment-data/enrollment-mbes/index.html').read())
    result=soup_url.find(attrs={'class':'threeColumns'}).get_text()
    url_data=result.split("Quarterly Medicaid Enrollment and Expenditure Reports")[1].split("About the Reports")[0]
    data=url_data.split(" ")
    reqData=str(' '.join(map(str, data))).strip().encode('utf-8')
    m = hashlib.md5()
    m.update(reqData)
    newRow["hashKey"]=(str(m.hexdigest()))
    checkForData(newRow,cursor)

def sahieDataUpdate(cursor):
    newRow={}
    newRow["URL"]='SAHIE'
    soup_url=BeautifulSoup(urllib.request.urlopen('https://www.census.gov/did/www/sahie/data/20082014/index.html').read())
    reqData=str(soup_url.get_text()).encode('utf-8')
    m = hashlib.md5()
    m.update(reqData)
    newRow["hashKey"]=(str(m.hexdigest()))
    checkForData(newRow,cursor)

def nsDuhUpdate(cursor):  #This one has the last modified date in its header description
    ssl._create_default_https_context = ssl._create_unverified_context
    newRow={}
    newRow["URL"]='NSDUH'
    req=urllib.request.urlopen("https://www.samhsa.gov/data/population-data-nsduh/reports?tab=38#tgr-tabs-34")
    charset=req.info().get_content_charset()
    content=req.read().decode(charset)
    test=content.encode('utf-8')
    m = hashlib.md5()
    m.update(test)
    newRow["hashKey"]=(str(m.hexdigest()))
    checkForData(newRow,cursor)

def aidsVuUpdate(cursor):
    newRow={}
    newRow["URL"]='AIDSVU'
    soup_url =BeautifulSoup( http.request('GET', 'https://aidsvu.org/resources/downloadable-maps-and-resources/',preload_content=False).read())
    result=soup_url.find(attrs={'class':'tab-nav'}).get_text().encode('utf-8')
    m = hashlib.md5()
    m.update(result)
    newRow["hashKey"]=str(m.hexdigest())
    checkForData

def main():
    cursor=db.cursor()
    count=0
    query="SELECT count(*) FROM information_schema.tables WHERE table_schema = 'opioddb' AND table_name = 'updatecheck'"
    cursor.execute(query)
    rows=cursor.fetchall()
    for row in rows:
        count=row[0]
    if(count==0):
        queryCreate="CREATE TABLE `opioddb`.`updatecheck`(`idupdateCheck` INT NOT NULL AUTO_INCREMENT,`URL` VARCHAR(105) NULL,`md5Hash` VARCHAR(105) NULL,PRIMARY KEY (`idupdateCheck`))"
        cursor.execute(queryCreate)
        print("table created")
    censusCountyDataUpdate(cursor)
    MedicaidEnrollDataUpdate(cursor)
    sahieDataUpdate(cursor)
    nsDuhUpdate(cursor)
    aidsVuUpdate(cursor)
    db.commit()
    db.close()
    return 'Complete!'


if __name__ == '__main__':
    status = main()
    sys.exit(status)