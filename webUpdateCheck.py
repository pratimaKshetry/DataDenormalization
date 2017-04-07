# -*- coding: utf-8 -*-
"""
Created on Thu Apr 03 09:27:01 2017
Author: Pratima Kshetry
"""
#check if the file has been updated or not
import sys
import urllib3
from bs4 import BeautifulSoup
import urllib

http = urllib3.PoolManager()

#count of txt files
countOfCounty=3236   #count of lines in  http://www2.census.gov/geo/docs/reference/codes/files/national_county.txt  file
MedicaidEnrollData="January – March 2016 Medicaid MBES Enrollment Report, posted December"  #count of lines in https://www.medicaid.gov/medicaid/program-information/medicaid-and-chip-enrollment-data/enrollment-mbes/index.html file
sahieDate="August 30, 2016".strip(" ")  #url: https://www.census.gov/did/www/sahie/data/20082014/index.html


def censusCountyDataUpdate():
    censusCountyData = http.request('GET','http://www2.census.gov/geo/docs/reference/codes/files/national_county.txt')
    url_data=str(censusCountyData.data)
    #txtN=txt.split("\\n",2)[1]
    data=url_data.split("\\n")
    count=countOfRows(data)
    print(count)
    if(count!=countOfCounty):
        print("File 'www2.census.gov/geo/docs/reference/codes/files/national_county.txt' has been Updated")

def MedicaidEnrollDataUpdate():  #Requires html parser in python
    soup_url = BeautifulSoup(urllib.request.urlopen('https://www.medicaid.gov/medicaid/program-information/medicaid-and-chip-enrollment-data/enrollment-mbes/index.html').read())
    result=soup_url.find(attrs={'class':'threeColumns'}).get_text()
    url_data=result.split("Quarterly Medicaid Enrollment and Expenditure Reports")[1].split("About the Reports")[0]
    data=url_data.split(" ")[0:10]
    reqData=str(' '.join(map(str, data))).strip()
    if(reqData!=str(MedicaidEnrollData)):
        print("############### Updates########################")

def sahieDataUpdate():
    soup_url=BeautifulSoup(urllib.request.urlopen('https://www.census.gov/did/www/sahie/data/20082014/index.html').read())
    result=soup_url.find("div", {"id":"reviseddate"}).get_text()
    url_data=result.split("Last Revised:")[1].strip()
    if(str(url_data)!=str(sahieDate)):
        print("############### Updates########################")

def nsDuhUpdate():  #Need to fix this
    soup_url=BeautifulSoup(urllib.request.urlopen('https://www.samhsa.gov/data/population-data-nsduh/reports?tab=38#tgr-tabs-34').read())
    #result=soup_url.find("div", {"id":"tgr-accordion-38"}).get_text()
    print(str(soup_url))
    
    
def aidsVuUpdate():  #dom element
    soup_url=BeautifulSoup(urllib.request.urlopen('http://aidsvu.org/resources/downloadable-maps-and-resources/').read())
    result=soup_url.find(attrs={'class':'downloadables-container'}).get_text()
    print(str(result))
        
def countOfRows(txt):
    count=0
    for line in txt:
        count=count+1
    return count
    

    
def main():
    #censusCountyDataUpdate()
    #MedicaidEnrollDataUpdate()
    #sahieDataUpdate()
    #NSDUHUpdate()
    aidsVuUpdate()
    

if __name__ == '__main__':
    status = main()
    sys.exit(status)

