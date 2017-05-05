# -*- coding: utf-8 -*-
"""
Created on Thu Apr 03 09:27:01 2017
#Send email notification to users when no authentication is required
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

db=pymysql.connect("localhost","root","admin","opiodDB")

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
        query="Select md5Hash from opiodDB.updatecheck where URL like '%"+str(newRow["URL"])+"%'"  #get existing MD5Hash key from table and compare it to the new MD5Hash Key
        cursor.execute(query)
        existingMD5=" "
        rows=cursor.fetchall()
        for row in rows:
            existingMD5=row[0]
        if(existingMD5!=str(newRow["hashKey"])): #if existing MD5Hash value is not equal to new MD5Hash, this means new updates are available
            print("New updates available for "+ str(newRow["URL"]))
            updateQuery(str(newRow["URL"]),str(newRow["hashKey"]),cursor)
            recipients=[ ' brian.honermann@amfar.org', 'alana.sharp@amfar.org']
            #recipients=[ 'pratima.kshetry@amfar.org']
            send_email("dbinfo@amfar.org ","KxKIBLRm",recipients,"test","NEW UPDATES AVAILABLE!! ")
            
            
def censusCountyDataUpdate(cursor):
    newRow={}
    newRow["URL"]='CENSUS'
    soup_url=BeautifulSoup(urllib.request.urlopen('https://www.census.gov/geo/reference/county-changes.html').read())
    result=soup_url.find("div", {"id":"middle-column"}).get_text().encode('utf-8')  #encode the text extracted . Extracted text must be encoded before using MD5Hash function
    m = hashlib.md5()    #get md5Hash value
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
    reqData=str(' '.join(map(str, data))).strip().encode('utf-8')  #encode the text extracted . Extracted text must be encoded before using MD5Hash function
    m = hashlib.md5()  #get md5Hash value
    m.update(reqData)
    newRow["hashKey"]=(str(m.hexdigest()))
    checkForData(newRow,cursor)

def sahieDataUpdate(cursor):
    newRow={}
    newRow["URL"]='SAHIE'
    soup_url=BeautifulSoup(urllib.request.urlopen('https://www.census.gov/did/www/sahie/data/20082014/index.html').read())
    reqData=str(soup_url.get_text()).encode('utf-8')  #encode the text extracted . Extracted text must be encoded before using MD5Hash function
    m = hashlib.md5()
    m.update(reqData)
    newRow["hashKey"]=(str(m.hexdigest()))
    checkForData(newRow,cursor)

def nsDuhUpdate(cursor):  
    ssl._create_default_https_context = ssl._create_unverified_context   #this url has SSL verification.
    newRow={}
    newRow["URL"]='NSDUH'
    req=urllib.request.urlopen("https://www.samhsa.gov/data/population-data-nsduh/reports?tab=38#tgr-tabs-34")
    charset=req.info().get_content_charset()
    content=req.read().decode(charset)
    test=content.encode('utf-8')  #encode the text extracted . Extracted text must be encoded before using MD5Hash function
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
    checkForData(newRow,cursor)

def samhsaDataUpdate(cursor):
    url="https://findtreatment.samhsa.gov/locator?sAddr=Washington%2C+DC%2C+United+States&submit=Go"
    response =  urllib.request.urlopen(url)
    print(response.getcode())
    i=response.info()
    print(i.keys())

def send_email(user, pwd, recipients, subject, body):
    import smtplib
    FROM = user
    TO = recipients if type(recipients) is list else [recipients]
    SUBJECT = subject
    TEXT = body
    # Prepare actual message
    message = """From: %s\nTo: %s\nSubject: %s\n\n%s
    """ % (FROM, ", ".join(TO), SUBJECT, TEXT)
    try:
        server = smtplib.SMTP('mail.amfar.org',25)
        server.ehlo()   ## identify ourselves, prompting server for supported features
        server.sendmail(FROM, TO, message)
        server.close()
        print ('successfully sent the mail')
    except Exception as e:
        print (str(e))
        print ("failed to send mail")
    
def main():
    cursor=db.cursor()
    count=0   #check if the table already exists in datbase.If not, create the corresponding table
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
    samhsaDataUpdate(cursor)
    db.commit()
    db.close()
    return 'Complete!'


if __name__ == '__main__':
    status = main()
    sys.exit(status)
