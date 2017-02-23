"""
Created on Thu Dec 15 09:36:57 2016
@author: Pratima Kshetry
"""
import pymysql
#import MySQLdb
#import mysql.connector
import re
import sys

#***************Global Variable Declartions***************
AnchorNumber={}
AggTotal={}
Unknown={}
year={}
countries=[]
targetYears=['2014','2015']
db=pymysql.connect("localhost","root","admin","web")
#db=pymysql.connect("localhost","root","root","web")
#**********************************************************

#*********************Start DB Operations**********************************************************************************************************************
def fetchRowsByIndicator(cursor,targetYear,country,copYear,indicator):
    query="SELECT MECHID, INDICATOR,LABEL,COPCC,TYEAR,SUM(VALUE) as VALUE from COPMechTargets WHERE COPCC='"+country+"' and TYEAR='"+targetYear+"' and COPYY='" + copYear + "' and INDICATOR Like '"+indicator+"%' group by MECHID,INDICATOR,LABEL,TYEAR order by MECHID,INDICATOR,LABEL,TYEAR"
    #print( query)
    cursor.execute(query)
    rows=cursor.fetchall()
    return rows

def insertNewData(newRow):
    global db
    cursor1=db.cursor()
    query="Insert into targetFlatData(mech_id,copcc,tyear,indicator,service,label,disaggs,sex,age,datavalue) VALUES ('"+str(newRow["MECHID"])+"','"+str(newRow["COPCC"])+"','"+str(newRow["tYear"])+"','"+str(newRow["INDICATOR"])+"','"+str(newRow["SERVICE"])+"','"+str(newRow["LABEL"])+"','"+str(newRow["DISAGGS"])+"','"+str(newRow["SEX"])+"','"+str(newRow["AGE"])+"','"+str(newRow["VALUE"])+"')"
    print(query)
    cursor1.execute(query)
    db.commit()
#**********************************************************

#*********************Start Common Operations******************************************************************************************************************************

#Find the sum of age/sex variables
def findSum(gender,age,compare):
    sumVal=0
    leftAge=0
    for key in gender:
        if(key.find("-")!=-1):
            ageBoundary=key.split("-")
            leftAge=int(ageBoundary[0])
        else:
            match=re.search(r"(\d)+",key)
            if(match):
                leftAge=int(match.group())
        if(compare=="+"):  #Sum more than 15
            if(age<=leftAge):
                sumVal=sumVal+gender[key]
        if(compare=="-"):  #Sum less than 15
            if(age>leftAge):
                sumVal=sumVal+gender[key]
    return sumVal

def getIndicatorString(indicatorData):
    service=getServiceString(indicatorData)
    service="_"+service
    if service in indicatorData:
        ind=indicatorData.replace(service,"")
    else:
        ind=indicatorData
    return ind

def getServiceString(indicatorData):   ##Get service data from indicator
    if("DSD" in indicatorData):
        service="DSD"
    elif("TA" in indicatorData):
        service="TA"
    elif("NGI" in indicatorData):
        service="NGI"
    elif("NA" in indicatorData):
        service="NA"
    else:
        service=indicatorData
    return service

#Common Aggregate function for TX_CURR_DSD and TX_NEW_DSD ,This fucntion computes the total of aggregated values
def aggregateTXCURR_TXNEW(targetYear,country,indicator,label,valueData,mechid,TX_NEW_DSD_CURRLabelLookUp,TX_NEW_DSD_CURRAnchorNumber,TX_NEW_DSD_CURRAggTotal):   ###Calculate the sum of dataRows starting with "Aggregated" and compare aggregated age value with the individual age-band value
    key=mechid+":"+country+":"+targetYear+":"+indicator  #key for dictionary values
    val=0
    if(label.startswith("Number of")):
        TX_NEW_DSD_CURRLabelLookUp[key]=label
        TX_NEW_DSD_CURRAnchorNumber[key]=valueData
    else:
        if ((label.startswith("Aggregated") and ("15" in label))):###Get the aggregate data... Exclude the <1 male and female values when computing anchor total
            val=TX_NEW_DSD_CURRAggTotal.get(key,0)
            TX_NEW_DSD_CURRAggTotal[key]=val+valueData

#Common function for TX_CURR_DSD and TX_NEW_DSD,HTC,CARE_CURR,CARE_NEW. This function groups the age-group values for male and female group and also aggregated male/female group.
def aggregateByMechanism(MechAgg,dataRows):
    regex1=r"(\d)+-(\d)+"                   #0-5
    regex2=r"(\d)+\+"                       #15+
    regex3=r"(\d)+\+(\s)*Female"            #15+ Female
    regex4=r"(\d)+\+(\s)*Male"              #15+ Male
    regex5=r"\<(\d)+(\s)*Female"            #<15 Female
    regex6=r"\<(\d)+(\s)*Male"              #<15 mALE
    regex7=r"\<(\d)+"                        #<15+

    for row in dataRows:
        #Construct mech agg key
        mechKey=str(row['MECHID'])
        MechAggDict=MechAgg.get(mechKey,{}) #either fetch already hashed dict or empty dictionary
        Male=MechAggDict.get("Male",{})
        Female=MechAggDict.get("Female",{})
        AggFemale=MechAggDict.get("AggFemale",{})
        AggMale= MechAggDict.get("AggMale",{})

        strLabel=str(row['LABEL'])
        if ((strLabel.startswith("Age") or (strLabel.startswith("By Age/Sex")))):
            match=re.search(regex1,strLabel)
            if match:
                key=match.group()
            else:
                if (strLabel.find("+")!=-1):
                    match=re.search(regex2,strLabel)
                    if match:
                        key=match.group()
                else:
                    if (strLabel.find("<")!=-1):
                        match=re.search(regex7,strLabel)
                        if match:
                            key=match.group()
            if match:
                if(strLabel.find("Female")!=-1):
                    Female[key]=int(row['VALUE'])
                else:
                    if(strLabel.find("Male")!=-1):
                        Male[key]=int(row['VALUE'])
        elif (strLabel.startswith("Aggregated")):
            match=re.search(regex3,strLabel)
            if match:
                key=match.group()
            else:
                match=re.search(regex4,strLabel)
                if match:
                    key=match.group()
                else:
                    match=re.search(regex5,strLabel)
                    if match:
                        key=match.group()
                    else:
                        match=re.search(regex6,strLabel)
                        if match:
                            key=match.group()
            if match:
                if(strLabel.find("Female")!=-1):
                    AggFemale[key]=int(row['VALUE'])
                else:
                    if(strLabel.find("Male")!=-1):
                        AggMale[key]=int(row['VALUE'])
        MechAggDict["Male"]=Male
        MechAggDict["Female"]=Female
        MechAggDict["AggFemale"]=AggFemale
        MechAggDict["AggMale"]=AggMale
        MechAgg[mechKey]=MechAggDict

#Common aggregatebyMechanism func for TB_ART_SCREEN and TB_ART_DSD
def aggregateByMechanismTB_ART_SCREEN(MechAgg,dataRows):
    regex1=r"(\d)+-(\d)+"
    regex2=r"(\d)+\+"
    regex7=r"\<(\d)+"

    for row in dataRows:
        #Construct mech agg key
        mechKey=str(row['MECHID'])
        MechAggDict=MechAgg.get(mechKey,{}) #either fetch already hashed dict or empty dictionary
        Age15Plus=MechAggDict.get("Age15Plus",{}) #For c21d indicator case
        Age15Less=MechAggDict.get("Age15Less",{})
        AgeGroup=MechAggDict.get("AgeGroup",{})
        #NegativetResult=MechAggDict.get("Negative",{})
        strLabel=str(row['LABEL'])
        if (strLabel.startswith("Age:")):
            match=re.search(regex1,strLabel)
            if match:
                key=match.group()
                AgeGroup[key]=int(row['VALUE'])
            else:
                if (strLabel.find("+")!=-1):
                    match=re.search(regex2,strLabel)
                    if match:
                        key=match.group()
                        AgeGroup[key]=int(row['VALUE'])
                if (strLabel.find("<")!=-1):
                    match=re.search(regex7,strLabel)
                    if match:
                        key=match.group()
                        AgeGroup[key]=int(row['VALUE'])

        if (strLabel.startswith("Aggregated")):
            if (strLabel.find("+")!=-1):
                match=re.search(regex2,strLabel)
                if match:
                    key=match.group()
                    Age15Plus[key]=int(row['VALUE'])
                    print("Age15Plus[key]::"+str(Age15Plus[key]))
            else:
                if (strLabel.find("<")!=-1):
                    match=re.search(regex7,strLabel)
                    if match:
                        key=match.group()
                        Age15Less[key]=int(row['VALUE'])
                        #print("Age18Less[key]::"+str(Age18Less[key]))
        MechAggDict["Age15Plus"]=Age15Plus
        MechAggDict["Age15Less"]=Age15Less
        MechAggDict["AgeGroup"]=AgeGroup
        MechAgg[mechKey]=MechAggDict
#*********************End Common Operations************************************************************************************************************************************

#*********************Start HTC Operations*************************************************************************************************************************************
def aggregateHTC(targetYear,country,indicator,label,valueData,mechid,HTCLabelLookUp,HTCAnchorNumber,HTCAggTotal,Others,OthersTotal):   ###Calculate the sum of dataRows starting with "Aggregated" and compare aggregated age value with the individual age-band value
    key=mechid+":"+country+":"+targetYear+":"+indicator  #key for dictionary values
    val=0
    if(indicator.startswith("HTC")): #Need to make sure this is indeed HTC
        if(label.startswith("Number of")):
            HTCLabelLookUp[key]=label
            HTCAnchorNumber[key]=valueData
        else:
            if (label.startswith("Aggregated")):###Get the aggregate data
                val=HTCAggTotal.get(key,0)
                HTCAggTotal[key]=val+valueData
            elif ((label.startswith("By"))):
                OtherList=Others.get(key,[])
                oTotal=OthersTotal.get(key,0)
                OtherList.append(str(label)+"||"+str(valueData))
                Others[key]=OtherList
                oTotal=oTotal+valueData
                OthersTotal[key]=oTotal

def processHTC(cursor,targetYear,country,copYear):  ###Process all indicators starting with HTC
    #Dictionary to aggregate data
    HTCLabelLookUp={}
    HTCAnchorNumber={}
    HTCAggTotal={}
    HTCUnknown={}
    OthersUnknown={}
    MechAgg={}
    Others={}
    OthersTotal={}
    #Process all health indicators that are in the format of %htc%
    dataRows=fetchRowsByIndicator(cursor,targetYear,country,copYear,"HTC")
    for row in dataRows:
        aggregateHTC(targetYear,country,str(row['INDICATOR']),str(row['LABEL']),row['VALUE'],str(row['MECHID']),HTCLabelLookUp,HTCAnchorNumber,HTCAggTotal,Others,OthersTotal)

    aggregateByMechanism(MechAgg,dataRows)

    for key in HTCAnchorNumber:
        HTCUnknown[key]=HTCAnchorNumber[key]-HTCAggTotal.get(key,0)
        OthersUnknown[key]=HTCAnchorNumber[key]-OthersTotal.get(key,0)
    #split key to get required dta key is in format     key=mechid+":"+country+":"+targetYear+":"+indicator
    for key in HTCAnchorNumber:
        data=key.split(":")
        mechIDData=str(data[0])
        countryData=str(data[1])
        targetYearData=str(data[2])
        indicatorData=str(data[3])
        strInd=getIndicatorString(indicatorData)
        service=getServiceString(indicatorData)
        MechAggDict=MechAgg[mechIDData]
        Male= MechAggDict.get("Male",{})
        Female=MechAggDict.get("Female",{})
        AggMale= MechAggDict.get("AggMale",{})
        AggFemale=MechAggDict.get("AggFemale",{})
        htcUnknownVal=HTCUnknown.get(key,0)
        labelData=HTCLabelLookUp.get(key,"")
        OtherList=Others.get(key,[])
        newRow={}
        newRow["MECHID"]=mechIDData
        newRow["COPCC"]=countryData
        newRow["tYear"]=targetYearData
        newRow["INDICATOR"]=strInd
        newRow["SERVICE"]=service
        newRow["LABEL"]=labelData
         #newRow is a dictionary that contains copcc,copyy,age,sex,label,disaggs,value and unknown
        for mkey in Male:
            newRow["DISAGGS"]=" "
            newRow["SEX"]="MALE"
            newRow["AGE"]=mkey
            newRow["VALUE"]=Male.get(mkey,"")
            insertNewData(newRow)
        for fkey in Female:
            newRow["DISAGGS"]=" "
            newRow["SEX"]="FEMALE"
            newRow["AGE"]=fkey
            newRow["VALUE"]=Female.get(fkey,"")
            insertNewData(newRow)
        for keyMale in AggMale:
            unknownAggMale=0
            if "+" in keyMale:
                compare="+"
            else:
                compare="-"
            match=re.search(r"(\d)+",keyMale)
            if match:
                age=int(match.group())
                sumMale=findSum(Male,age,compare)
            unknownAggMale=int(AggMale.get(keyMale,0))-sumMale
            newRow["VALUE"]=unknownAggMale
            newRow["AGE"]=keyMale.split(" ")[0]+"Male UNKNOWN"
            newRow["SEX"]=keyMale.split(" ")[1]
            newRow["DISAGGS"]=""
            insertNewData(newRow)

        for keyFemale in AggFemale:
            if "+" in keyFemale:
                compare="+"
            else:
                compare="-"
            match=re.search(r"(\d)+",keyFemale)
            if match:
                age=int(match.group())
                sumFemale=findSum(Female,age,compare)
                unknownAggFemale=int(AggFemale.get(keyFemale,0))-sumFemale
            newRow["VALUE"]=unknownAggFemale
            newRow["AGE"]=keyFemale.split(" ")[0] + "Female UNKNOWN"
            newRow["SEX"]=keyFemale.split(" ")[1]
            newRow["DISAGGS"]=""
            insertNewData(newRow)

        for otherData in OtherList:
            otherDataSplit=otherData.split("||")
            newRow["DISAGGS"]=otherDataSplit[0]
            newRow["VALUE"]=otherDataSplit[1]
            newRow["AGE"]="N/A"
            newRow["SEX"]="N/A"
            insertNewData(newRow)
        newRow["DISAGGS"]="By Test Result Unknown"
        newRow["LABEL"]=labelData
        newRow["AGE"]="N/A"
        newRow["SEX"]="N/A"
        newRow["VALUE"]=str(OthersUnknown.get(key,0))
        insertNewData(newRow)
##Handle data input for anchor label and its respective column values
        newRow["LABEL"]=labelData
        newRow["DISAGGS"]="Anchor/Unknown"
        newRow["AGE"]="N/A"
        newRow["SEX"]="N/A"
        newRow["VALUE"]=str(htcUnknownVal)
        insertNewData(newRow)
#*********************End HTC Operations*************************************************************************************************************************************

#*********************Start BS Operations*************************************************************************************************************************************
def aggregateBS(targetYear,country,indicator,label,valueData,mechid,BSLabelLookUp,BSAnchorNumber,BSByTotal,BSBy,BSOtherAnchor,BSOtherTotal,BSOtherList):
    key=mechid+":"+country+":"+targetYear+":"+indicator
    if(indicator.startswith("BS")):
        if(label.startswith("Number of")):
            BSLabelLookUp[key]=label
            BSAnchorNumber[key]=valueData
        if(label.startswith("By:")):
            ByBSlist=BSBy.get(key,[]) #contain the dictionary of rows starting with "By" in a list
            ByBSlist.append(str(label)+"||"+str(valueData))
            BSBy[key]=ByBSlist
            val=BSByTotal.get(key,0)
            BSByTotal[key]=val+valueData
        if(label.startswith("By: Number of whole blood donations screened for HIV in an NBTS network laboratory")):
            BSOtherAnchor[key]=valueData
            BSOther=BSOtherList.get(key,[]) #contain the dictionary of rows starting with "By" in a list
            BSOther.append(str(label)+"||"+str(valueData))
            BSOtherList[key]=BSOther
            val=BSOtherTotal.get(key,0)
            BSOtherTotal[key]=val+valueData

def processBSCOLL(cursor,targetYear,country,copYear):
    BSLabelLookUp={}
    BSAnchorNumber={}
    BSByTotal={}
    BSUnknown={}
    BSBy={} #Dictionary of those rows starting with "By"
    BSOtherAnchor={}  #holds anchor value for 'By: Number of whole blood donations screened for HIV in an NBTS network laboratory'
    BSOtherTotal={}  #holds values for 'By: Number of whole blood donations screened for HIV in an NBTS network laboratory that are identified as reactive for HIV' 
    BSOtherList={}
    BSUnknownNotReactive={} #Holds unknown value for ('By: Number of whole blood donations screened for HIV in an NBTS network laboratory' (subtract) 'By: Number of whole blood donations screened for HIV in an NBTS network laboratory that are identified as reactive for HIV')
    dataRows=fetchRowsByIndicator(cursor,targetYear,country,copYear,"BS")
    for row in dataRows:
        aggregateBS(targetYear,country,str(row['INDICATOR']),str(row['LABEL']),row['VALUE'],str(row['MECHID']),BSLabelLookUp,BSAnchorNumber,BSByTotal,BSBy,BSOtherAnchor,BSOtherTotal,BSOtherList)

    for key in BSAnchorNumber:
        BSUnknown[key]=BSAnchorNumber[key]-BSByTotal.get(key,0) #Get the unknown value : Anchor Value-sum(values starting with 'By')
        if(int(BSOtherTotal.get(key,0))>0):
            BSUnknownNotReactive[key]=BSOtherAnchor[key]-BSOtherTotal.get(key,0)
    for key in BSAnchorNumber:
        data=key.split(":")
        mechIDData=data[0]
        countryData=data[1]
        targetYearData=str(data[2])
        indicatorData=str(data[3])
        service=indicatorData
        BSUnknownVal=BSUnknown.get(key,0)
        labelData=BSLabelLookUp.get(key,"")
        ByBSlist=BSBy.get(key,[])
        
        newRow={}
        newRow["MECHID"]=mechIDData
        newRow["COPCC"]=countryData
        newRow["tYear"]=targetYearData
        newRow["INDICATOR"]=indicatorData
        newRow["SERVICE"]=service
        newRow["LABEL"]=labelData

        for byData in ByBSlist:
            ByDataSplit=byData.split("||")  #format: By: Number of whole blood donations screened for HIV in an NBTS network laboratory|| 1234
            newRow["DISAGGS"]=ByDataSplit[0]
            newRow["VALUE"]=ByDataSplit[1]
            newRow["AGE"]="ALL"
            newRow["SEX"]="ALL"
            insertNewData(newRow)
        newRow["LABEL"]=labelData
        newRow["DISAGGS"]="Anchor/Unknown"
        newRow["AGE"]="ALL"
        newRow["SEX"]="ALL"
        newRow["VALUE"]=str(BSUnknownVal)
        insertNewData(newRow)
        
        newRow["LABEL"]="By: Number of whole blood donations screened for HIV in an NBTS network laboratory"
        newRow["DISAGGS"]="Number of whole blood donations screened for HIV in an NBTS network laboratory identified as NOT reactive for HIV"
        newRow["AGE"]="ALL"
        newRow["SEX"]="ALL"
        newRow["VALUE"]=str(BSUnknownNotReactive.get(key,0))
        insertNewData(newRow)
#*************************End BS Operations*************************************************************************************************************************************

#*************************Start AC Operations*************************************************************************************************************************************
def processAC(cursor,targetYear,country,copYear):
    dataRows=fetchRowsByIndicator(cursor,targetYear,country,copYear,"AC_")
    for row in dataRows:
        newRow={}
        newRow["MECHID"]=str(row["MECHID"])
        newRow["DISAGGS"]=""
        newRow["INDICATOR"]=str(row["INDICATOR"])
        newRow["SERVICE"]=str(row["INDICATOR"])
        newRow["AGE"]="ALL"
        newRow["SEX"]="ALL"
        newRow["VALUE"]=str(row["VALUE"])
        newRow["tYear"]=str(row["TYEAR"])
        newRow["LABEL"]=str(row["LABEL"])
        newRow["COPCC"]=str(row["COPCC"])
        insertNewData(newRow)
#*************************End AC Operations*************************************************************************************************************************************

#*************************Start C21D Operations*************************************************************************************************************************************
def aggregateC21D(targetYear,country,indicator,label,VALUEData,mechid,C21DLabelLookUp,C21DAnchorNumber,C21DAggTotal,allAges,byAge,byAgeTotal,bySex,sexTotal):
    key=mechid+":"+country+":"+targetYear+":"+indicator
    if(label.startswith("Number of")):
        C21DLabelLookUp[key]=label
        C21DAnchorNumber[key]=VALUEData
    if(label.startswith("By Age/Sex:")):
        val=C21DAggTotal.get(key,0)
        C21DAggTotal[key]=val+VALUEData
        allList=allAges.get(key,[])
        allList.append(str(label)+"||"+str(VALUEData))
        allAges[key]=allList
    if(label.startswith("By Age:")):
        val=byAgeTotal.get(key,0)
        byAgeTotal[key]=val+VALUEData
        allAge=byAge.get(key,[])
        allAge.append(str(label)+"||"+str(VALUEData))
        byAge[key]=allAge
    if(label.startswith("By Sex:")):
        val=sexTotal.get(key,0)
        sexTotal[key]=val+VALUEData
        allSex=bySex.get(key,[])
        allSex.append(str(label)+"||"+str(VALUEData))
        bySex[key]=allSex
#Process C21D Indicators
def processC21D(cursor,targetYear,country,copYear):
    C21DLabelLookUp={}
    C21DAnchorNumber={}
    C21DAggTotal={} #Keys are 15+ male,15- Female, <15 Male, <15 Female, 15+,<15,Male, Female
    C21DUnknown={}  #Anchor unknown value for (AnchorNumber- sum of all ages)
    allAges={}
    byAge={}
    byAgeTotal={}
    byAgeUnknown={}
    bySex={}
    sexTotal={}
    bySexUnknown={}
    dataRows=fetchRowsByIndicator(cursor,targetYear,country,copYear,"C2.1.D")
    for row in dataRows:
        aggregateC21D(targetYear,country,str(row['INDICATOR']),str(row['LABEL']),row['VALUE'],str(row['MECHID']),C21DLabelLookUp,C21DAnchorNumber,C21DAggTotal,allAges,byAge,byAgeTotal,bySex,sexTotal)
    
    for key in C21DAnchorNumber:
        C21DUnknown[key]=C21DAnchorNumber[key]-C21DAggTotal.get(key,0)
        if(byAgeTotal.get(key,0)>0):
            byAgeUnknown[key]=C21DAnchorNumber[key]-byAgeTotal.get(key,0)
        if(sexTotal.get(key,0)>0):
            bySexUnknown[key]=C21DAnchorNumber[key]-sexTotal.get(key,0)
    for key in C21DAnchorNumber:
        data=key.split(":")
        mechIDData=str(data[0])
        allList=allAges.get(key,[])
        onlyAge=byAge.get(key,[])
        onlySex=bySex.get(key,[])
        newRow={}
        newRow["MECHID"]=mechIDData
        newRow["COPCC"]=str(data[1])
        newRow["tYear"]=str(data[2])
        newRow["INDICATOR"]=getIndicatorString(str(data[3]))
        newRow["SERVICE"]=getServiceString(str(data[3]))
        newRow["LABEL"]=C21DLabelLookUp.get(key,"")
        for allKey in allList:
            newRow["DISAGGS"]=""
            newRow["VALUE"]=allKey.split("||")[1]    ##(allkey="By Age/Sex: 15+ Male|| 1234 where "By Age/Sex: 15+ Male" is label and "1234" is its corresponding value)
            newRow["AGE"]=allKey.split("||")[0].split(":")[1].split(" ")[1]
            if("Male" in allKey):
                newRow["SEX"]="MALE"
            else:
                newRow["SEX"]="FEMALE"
            insertNewData(newRow)
        for allKey in onlyAge:
            newRow["DISAGGS"]=""
            newRow["VALUE"]=allKey.split("||")[1]    ##(allkey="By Age: 15+|| 1234 where "By Age: 15+" is label and "1234" is its corresponding value)
            newRow["AGE"]=allKey.split("||")[0].split(":")[1]
            newRow["SEX"]="ALL"
            insertNewData(newRow)
        for allKey in onlySex:
            newRow["DISAGGS"]=""
            newRow["VALUE"]=allKey.split("||")[1]    ##(allkey="By Sex: Male|| 1234 where "By Age: 15+" is label and "1234" is its corresponding value)
            newRow["AGE"]="ALL"
            newRow["SEX"]=allKey.split("||")[0].split(":")[1]
            insertNewData(newRow)
        newRow["DISAGGS"]="Anchor Age/Sex Unknown"
        newRow["VALUE"]=str(C21DUnknown.get(key,0))
        newRow["SEX"]="ALL"
        newRow["AGE"]="ALL"
        insertNewData(newRow)
        
        newRow["DISAGGS"]="Anchor Age Unknown"
        newRow["VALUE"]=str(byAgeUnknown.get(key,0))
        newRow["SEX"]="ALL"
        newRow["AGE"]="ALL"
        insertNewData(newRow)
        
        newRow["DISAGGS"]="Anchor SEX Unknown"
        newRow["VALUE"]=str(bySexUnknown.get(key,0))
        newRow["SEX"]="ALL"
        newRow["AGE"]="ALL"
        insertNewData(newRow)

#*************************Start C21D Operations*************************************************************************************************************************************

#*************************Start OVC Operations*************************************************************************************************************************************
def aggregateOVC(targetYear,country,indicator,label,valueData,mechid,OVCLabelLookUp,OVCAnchorNumber,OVCAgeTotal,OVCSexTotal,Age,Sex):
    key=mechid+":"+country+":"+targetYear+":"+indicator  #key for dictionary values
    if(indicator.startswith("OVC_")): #Need to make sure this is indeed HTC
        if(label.startswith("Number of")):
            OVCLabelLookUp[key]=label
            OVCAnchorNumber[key]=valueData
        if (label.startswith("Age:")):###Get the aggregate data
                ageList=Age.get(key,[])
                print("ageList:"+str(ageList))
                ageTotal=OVCAgeTotal.get(key,0)
                ageList.append(str(label)+"||"+str(valueData))
                print("ageList:"+str(ageList))
                Age[key]=ageList
                ageTotal=ageTotal+valueData
                OVCAgeTotal[key]=ageTotal
        if (label.startswith("Sex:")):###Get the aggregate data
                sexList=Sex.get(key,[])

                sexTotal=OVCSexTotal.get(key,0)
                sexList.append(str(label)+"||"+str(valueData))
                Sex[key]=sexList
                print("sexList:"+str(sexList))
                sexTotal=sexTotal+valueData
                OVCSexTotal[key]=sexTotal

def processOVC(cursor,targetYear,country,copYear):
    OVCLabelLookUp={}
    OVCAnchorNumber={}
    OVCAgeTotal={}
    OVCSexTotal={}
    OVCAgeUnknown={}
    OVCSexUnknown={}
    Age={}
    Sex={}
    #Process all health indicators that are in the format of %htc%
    dataRows=fetchRowsByIndicator(cursor,targetYear,country,copYear,"OVC_")
    for row in dataRows:
        aggregateOVC(targetYear,country,str(row['INDICATOR']),str(row['LABEL']),row['VALUE'],str(row['MECHID']),OVCLabelLookUp,OVCAnchorNumber,OVCAgeTotal,OVCSexTotal,Age,Sex)
    #aggregateByMechanism(MechAgg,dataRows)

    for key in OVCAnchorNumber:
        OVCAgeUnknown[key]=OVCAnchorNumber[key]-OVCAgeTotal.get(key,0)
        OVCSexUnknown[key]=OVCAnchorNumber[key]-OVCSexTotal.get(key,0)

    for key in OVCAnchorNumber:
        data=key.split(":")
        mechIDData=str(data[0])
        print("MechID:"+mechIDData)

        countryData=str(data[1])
        print("cOUNTRY:"+countryData)
        targetYearData=str(data[2])
        print("targetYear:"+targetYearData)
        indicatorData=str(data[3])
        print("indicator:"+indicatorData)
        ind=indicatorData.split("_")[0:2]                  #Splits indicator
        strInd=str(ind[0])+"_"+str(ind[1])
        print("indicator:"+strInd)
        service=str(indicatorData.split("_")[-1:][0])       #Splits service from indicator
        print("service:"+service)
        ageList=Age.get(key,[])
        sexList=Sex.get(key,[])
        #print("ageList:"+str(ageList))
        OVCUnknownAge=OVCAgeUnknown.get(key,0)
        print("OVCUnknownAge:"+str(OVCUnknownAge))
        OVCUnknownSex=OVCSexUnknown.get(key,0)
        print("OVCUnknownSex:"+str(OVCUnknownSex))
        labelData=OVCLabelLookUp.get(key,"")
        newRow={}
        newRow["MECHID"]=mechIDData
        newRow["COPCC"]=countryData
        newRow["tYear"]=targetYearData
        newRow["INDICATOR"]=strInd
        newRow["SERVICE"]=service
        newRow["LABEL"]=labelData
        print("label:"+newRow["LABEL"])
#Processing for Age labels
        for agekey in ageList:
            ageSplit=agekey.split("||")
            newRow["VALUE"]=ageSplit[1]
            age=ageSplit[0].split(":")[1]
            newRow["AGE"]=age
            newRow["DISAGGS"]=" "
            newRow["SEX"]="ALL"
            insertNewData(newRow)
        newRow["DISAGGS"]="Anchor AGE /Unknown"
        newRow["AGE"]="ALL"
        newRow["SEX"]="ALL"
        newRow["VALUE"]=OVCAgeUnknown.get(key,0)
        insertNewData(newRow)

        for sexkey in sexList:
            sexSplit=sexkey.split("||")
            newRow["VALUE"]=sexSplit[1]
            sex=sexSplit[0].split(":")[1]
            newRow["SEX"]=sex
            newRow["AGE"]="ALL"
            insertNewData(newRow)

        newRow["DISAGGS"]=labelData
        newRow["AGE"]="ALL"
        newRow["SEX"]="ALL"
        newRow["VALUE"]=OVCAnchorNumber[key]
        insertNewData(newRow)
#*************************End OVC Operations*************************************************************************************************************************************

#*************************Start Care_Curr Operations**********************************************************************************************************************************
def aggregateCareCurr(targetYear,country,indicator,label,valueData,mechid,CareCurrLabelLookUp,CareCurrAnchorNumber,CareCurrAggTotal):
    key=mechid+":"+country+":"+targetYear+":"+indicator  #key for dictionary values
    val=0
    if(indicator.startswith("CARE_CURR")): #Need to make sure this is indeed CAR_CURR
        if(label.startswith("Number of")):
            CareCurrLabelLookUp[key]=label
            CareCurrAnchorNumber[key]=valueData
        else:
            if (label.startswith("Aggregated")):###Get the aggregate data
                val=CareCurrAggTotal.get(key,0)
                CareCurrAggTotal[key]=val+valueData
                #print("Others data :"+str(val))

def processCareCurr(cursor,targetYear,country,copYear):
    #Dictionary to aggregate data
    CareCurrLabelLookUp={}
    CareCurrAnchorNumber={}
    CareCurrAggTotal={}
    CareCurrUnknown={}
    MechAgg={}

    #Process all health indicators that are in the format of %htc%
    dataRows=fetchRowsByIndicator(cursor,targetYear,country,copYear,"CARE_CURR")
    for row in dataRows:
        aggregateCareCurr(targetYear,country,str(row['INDICATOR']),str(row['LABEL']),row['VALUE'],str(row['MECHID']),CareCurrLabelLookUp,CareCurrAnchorNumber,CareCurrAggTotal)

    aggregateByMechanism(MechAgg,dataRows)

    for key in CareCurrAnchorNumber:
        CareCurrUnknown[key]=CareCurrAnchorNumber[key]-CareCurrAggTotal.get(key,0)

    for key in CareCurrAnchorNumber:
        data=key.split(":")
        mechIDData=str(data[0])
        countryData=str(data[1])
        targetYearData=str(data[2])
        indicatorData=str(data[3])
        ind=indicatorData.split("_")[0:2]                  #Splits indicator
        strInd=str(ind[0])+"_"+str(ind[1])
        service=str(indicatorData.split("_")[-1:][0])       #Splits service from indicator
        MechAggDict=MechAgg[mechIDData]
        Male= MechAggDict.get("Male",{})
        Female=MechAggDict.get("Female",{})
        AggMale= MechAggDict.get("AggMale",{})
        AggFemale=MechAggDict.get("AggFemale",{})
        CareCurrUnknownVal=CareCurrUnknown.get(key,0)
        labelData=CareCurrLabelLookUp.get(key,"")

        newRow={}
        newRow["MECHID"]=mechIDData
        newRow["COPCC"]=countryData
        newRow["tYear"]=targetYearData
        newRow["INDICATOR"]=strInd
        newRow["SERVICE"]=service
        newRow["LABEL"]=labelData
         #newRow is a dictionary that contains copcc,copyy,age,sex,label,disaggs,value and unknown
        for mkey in Male:

            newRow["DISAGGS"]=" "
            newRow["SEX"]="MALE"
            newRow["AGE"]=mkey
            newRow["VALUE"]=Male.get(mkey,"")
            insertNewData(newRow)
        for fkey in Female:
            #newRow["LABEL"]=labelData
            newRow["DISAGGS"]=" "
            newRow["SEX"]="FEMALE"
            newRow["AGE"]=fkey
            newRow["VALUE"]=Female.get(fkey,"")
            insertNewData(newRow)
        for keyMale in AggMale:
            unknownAggMale=0
            if "+" in keyMale:
                compare="+"
            else:
                compare="-"
            match=re.search(r"(\d)+",keyMale)
            if match:
                age=int(match.group())
                sumMale=findSum(Male,age,compare)
            unknownAggMale=int(AggMale.get(keyMale,0))-sumMale
            newRow["VALUE"]=unknownAggMale
            newRow["AGE"]=keyMale.split(" ")[0]+"Male UNKNOWN"
            newRow["SEX"]=keyMale.split(" ")[1]
            newRow["DISAGGS"]=""
            insertNewData(newRow)

        for keyFemale in AggFemale:
            if "+" in keyFemale:
                compare="+"
            else:
                compare="-"
            match=re.search(r"(\d)+",keyFemale)
            if match:
                age=int(match.group())
                sumFemale=findSum(Female,age,compare)
                unknownAggFemale=int(AggFemale.get(keyFemale,0))-sumFemale
            newRow["VALUE"]=unknownAggFemale
            newRow["AGE"]=keyFemale.split(" ")[0]+"Female UNKNOWN"
            newRow["SEX"]=keyFemale.split(" ")[1]
            newRow["DISAGGS"]=""
            insertNewData(newRow)

##Handle data input for anchor label and its respective column values
        newRow["LABEL"]=labelData
        newRow["DISAGGS"]="Anchor/Unknown"
        newRow["AGE"]="ALL"
        newRow["SEX"]="ALL"
        newRow["VALUE"]=str(CareCurrUnknownVal)
        insertNewData(newRow)

        newRow["LABEL"]=labelData
        newRow["DISAGGS"]=labelData
        newRow["AGE"]="ALL"
        newRow["SEX"]="ALL"
        newRow["VALUE"]=str(CareCurrAnchorNumber[key])
        insertNewData(newRow)
#*************************End Care_Curr Operations*************************************************************************************************************************************

#*************************Start Care_New Operations*************************************************************************************************************************************
def aggregateCareNew(targetYear,country,indicator,label,valueData,mechid,CareCurrLabelLookUp,CareCurrAnchorNumber,CareCurrAggTotal):
    key=mechid+":"+country+":"+targetYear+":"+indicator  #key for dictionary values
    val=0
    if(indicator.startswith("CARE_NEW")): #Need to make sure this is indeed HTC
        if(label.startswith("Number of")):
            CareCurrLabelLookUp[key]=label
            CareCurrAnchorNumber[key]=valueData
        else:
            if (label.startswith("Aggregated")):###Get the aggregate data
                val=CareCurrAggTotal.get(key,0)
                CareCurrAggTotal[key]=val+valueData
                #print("Others data :"+str(val))

def processCareNew(cursor,targetYear,country,copYear):
    #Dictionary to aggregate data
    CareCurrLabelLookUp={}
    CareCurrAnchorNumber={}
    CareCurrAggTotal={}
    CareCurrUnknown={}
    MechAgg={}

    #Process all health indicators that are in the format of %htc%
    dataRows=fetchRowsByIndicator(cursor,targetYear,country,copYear,"CARE_NEW")
    for row in dataRows:
        aggregateCareNew(targetYear,country,str(row['INDICATOR']),str(row['LABEL']),row['VALUE'],str(row['MECHID']),CareCurrLabelLookUp,CareCurrAnchorNumber,CareCurrAggTotal)

    aggregateByMechanism(MechAgg,dataRows)

    for key in CareCurrAnchorNumber:
        CareCurrUnknown[key]=CareCurrAnchorNumber[key]-CareCurrAggTotal.get(key,0)

    for key in CareCurrAnchorNumber:
        data=key.split(":")
        mechIDData=str(data[0])
        countryData=str(data[1])
        targetYearData=str(data[2])
        indicatorData=str(data[3])
        ind=indicatorData.split("_")[0:2]                  #Splits indicator
        strInd=str(ind[0])+"_"+str(ind[1])
        service=str(indicatorData.split("_")[-1:][0])       #Splits service from indicator
        MechAggDict=MechAgg[mechIDData]
        Male= MechAggDict.get("Male",{})
        Female=MechAggDict.get("Female",{})
        AggMale= MechAggDict.get("AggMale",{})
        AggFemale=MechAggDict.get("AggFemale",{})
        CareCurrUnknownVal=CareCurrUnknown.get(key,0)
        labelData=CareCurrLabelLookUp.get(key,"")

        newRow={}
        newRow["MECHID"]=mechIDData
        newRow["COPCC"]=countryData
        newRow["tYear"]=targetYearData
        newRow["INDICATOR"]=strInd
        newRow["SERVICE"]=service
        newRow["LABEL"]=labelData
         #newRow is a dictionary that contains copcc,copyy,age,sex,label,disaggs,value and unknown
        for mkey in Male:

            newRow["DISAGGS"]=" "
            newRow["SEX"]="MALE"
            newRow["AGE"]=mkey
            newRow["VALUE"]=Male.get(mkey,"")
            insertNewData(newRow)
        for fkey in Female:
            #newRow["LABEL"]=labelData
            newRow["DISAGGS"]=" "
            newRow["SEX"]="FEMALE"
            newRow["AGE"]=fkey
            newRow["VALUE"]=Female.get(fkey,"")
            insertNewData(newRow)
        for keyMale in AggMale:
            unknownAggMale=0
            if "+" in keyMale:
                compare="+"
            else:
                compare="-"
            match=re.search(r"(\d)+",keyMale)
            if match:
                age=int(match.group())
                sumMale=findSum(Male,age,compare)
            unknownAggMale=int(AggMale.get(keyMale,0))-sumMale
            newRow["VALUE"]=unknownAggMale
            newRow["AGE"]=keyMale.split(" ")[0]+"Male UNKNOWN"
            newRow["SEX"]=keyMale.split(" ")[1]
            newRow["DISAGGS"]=""
            insertNewData(newRow)

        for keyFemale in AggFemale:
            if "+" in keyFemale:
                compare="+"
            else:
                compare="-"
            match=re.search(r"(\d)+",keyFemale)
            if match:
                age=int(match.group())
                sumFemale=findSum(Female,age,compare)
                unknownAggFemale=int(AggFemale.get(keyFemale,0))-sumFemale
            newRow["VALUE"]=unknownAggFemale
            newRow["AGE"]=keyFemale.split(" ")[0]+"Female UNKNOWN"
            newRow["SEX"]=keyFemale.split(" ")[1]
            newRow["DISAGGS"]=""
            insertNewData(newRow)

##Handle data input for anchor label and its respective column values
        newRow["LABEL"]=labelData
        newRow["DISAGGS"]="Anchor/Unknown"
        newRow["AGE"]="N/A"
        newRow["SEX"]="N/A"
        newRow["VALUE"]=str(CareCurrUnknownVal)
        insertNewData(newRow)

        newRow["LABEL"]=labelData
        newRow["DISAGGS"]=labelData
        newRow["AGE"]="ALL"
        newRow["SEX"]="ALL"
        newRow["VALUE"]=str(CareCurrAnchorNumber[key])
        insertNewData(newRow)
#*************************End Care_New Operations*************************************************************************************************************************************

#*************************Start C24 Operations*************************************************************************************************************************************
def aggregateC24(targetYear,country,indicator,label,valueData,mechid,C24LabelLookUp,C24AnchorNumber,C24AggTotal,Others):
    key=mechid+":"+country+":"+targetYear+":"+indicator  #key for dictionary values
    if(indicator.startswith("C2.4")): #Need to make sure this is indeed HTC
        if("Number of HIV-positive individuals receiving a minimum of one clinical service" in label):
            C24LabelLookUp[key]=label
            C24AnchorNumber[key]=valueData
        else:
            othersList=Others.get(key,[])
            othersList.append(str(label)+"||"+str(valueData))
            C24AggTotal[key]=valueData
            Others[key]=othersList

def processC24(cursor,targetYear,country,copYear):
    Others={}
    C24LabelLookUp={}
    C24AggTotal={}
    C24AnchorNumber={}
    C24Unknown={}

    dataRows=fetchRowsByIndicator(cursor,targetYear,country,copYear,"C2.4")
    for row in dataRows:
        aggregateC24(targetYear,country,str(row['INDICATOR']),str(row['LABEL']),row['VALUE'],str(row['MECHID']),C24LabelLookUp,C24AnchorNumber,C24AggTotal,Others)

    for key in C24AnchorNumber:
        C24Unknown[key]=C24AnchorNumber[key]-C24AggTotal.get(key,0)

    for key in C24AnchorNumber:
        data=key.split(":")
        mechIDData=str(data[0])
        countryData=str(data[1])
        targetYearData=str(data[2])
        indicatorData=str(data[3])
        ind=indicatorData.split("_")[0:1]                  #Splits indicator
        strInd=str(ind[0])
        service=str(indicatorData.split("_")[-1:][0])       #Splits service from indicator
        #C24UnknownVal=C24Unknown.get(key,0)
        labelData=C24LabelLookUp.get(key,"")
        othersList=Others.get(key,[])
        newRow={}
        newRow["MECHID"]=mechIDData
        newRow["COPCC"]=countryData
        newRow["tYear"]=targetYearData
        newRow["INDICATOR"]=strInd
        newRow["SERVICE"]=service
        newRow["LABEL"]=labelData
        newRow["AGE"]="ALL"
        newRow["SEX"]="ALL"
        for others in othersList:
            dataSplit=others.split("||")
            newRow["VALUE"]=dataSplit[1]
            newRow["DISAGGS"]=dataSplit[0]
            insertNewData(newRow)
        newRow["LABEL"]=labelData
        newRow["DISAGGS"]=labelData
        newRow["AGE"]="ALL"
        newRow["SEX"]="ALL"
        newRow["VALUE"]=C24AnchorNumber.get(key,0)
        insertNewData(newRow)
        newRow["DISAGGS"]="Anchor C24 Unknown"
        newRow["VALUE"]=C24Unknown.get(key,0)
        insertNewData(newRow)
#*************************End C24 Operations*************************************************************************************************************************************

#*************************End C25 Operations*************************************************************************************************************************************
def aggregateC25(targetYear,country,indicator,label,valueData,mechid,C25LabelLookUp,C25AnchorNumber,C25AggTotal,Others):
    key=mechid+":"+country+":"+targetYear+":"+indicator  #key for dictionary values
    if(indicator.startswith("C2.5")): #Need to make sure this is indeed HTC
        if("Number of HIV-positive individuals receiving a minimum of one clinical service" in label):
            C25LabelLookUp[key]=label
            C25AnchorNumber[key]=valueData
        else:
            othersList=Others.get(key,[])
            othersList.append(str(label)+"||"+str(valueData))
            C25AggTotal[key]=valueData
            Others[key]=othersList

def processC25(cursor,targetYear,country,copYear):
    Others={}
    C25LabelLookUp={}
    C25AggTotal={}
    C25AnchorNumber={}
    C25Unknown={}

    dataRows=fetchRowsByIndicator(cursor,targetYear,country,copYear,"C2.5")
    for row in dataRows:
        aggregateC25(targetYear,country,str(row['INDICATOR']),str(row['LABEL']),row['VALUE'],str(row['MECHID']),C25LabelLookUp,C25AnchorNumber,C25AggTotal,Others)

    for key in C25AnchorNumber:
        C25Unknown[key]=C25AnchorNumber[key]-C25AggTotal.get(key,0)

    for key in C25AnchorNumber:
        data=key.split(":")
        mechIDData=str(data[0])
        countryData=str(data[1])
        targetYearData=str(data[2])
        indicatorData=str(data[3])
        ind=indicatorData.split("_")[0:1]                  #Splits indicator
        strInd=str(ind[0])
        service=str(indicatorData.split("_")[-1:][0])       #Splits service from indicator
        #C24UnknownVal=C24Unknown.get(key,0)
        labelData=C25LabelLookUp.get(key,"")
        othersList=Others.get(key,[])
        newRow={}
        newRow["MECHID"]=mechIDData
        newRow["COPCC"]=countryData
        newRow["tYear"]=targetYearData
        newRow["INDICATOR"]=strInd
        newRow["SERVICE"]=service
        newRow["LABEL"]=labelData
        newRow["AGE"]="ALL"
        newRow["SEX"]="ALL"
        for others in othersList:
            dataSplit=others.split("||")
            newRow["VALUE"]=dataSplit[1]
            newRow["DISAGGS"]=dataSplit[0]
            insertNewData(newRow)
        newRow["LABEL"]=labelData
        newRow["DISAGGS"]=labelData
        newRow["AGE"]="ALL"
        newRow["SEX"]="ALL"
        newRow["VALUE"]=C25AnchorNumber.get(key,0)
        insertNewData(newRow)
        newRow["DISAGGS"]="Anchor C25 Unknown"
        newRow["VALUE"]=C25Unknown.get(key,0)
        insertNewData(newRow)
#*************************End C25 Operations*************************************************************************************************************************************

#*************************Start FNTHER Operations*************************************************************************************************************************************
def aggregateFNTHER(targetYear,country,indicator,label,valueData,mechid,FNTHERLabelLookUp,FNTHERAnchorNumber,FNTHERAggTotal,allAges,FNTHERLabelOther,FNTHEROther):
    key=mechid+":"+country+":"+targetYear+":"+indicator  #key for dictionary values
    if("Number of clinically malnourished PLHIV who received therapeutic and/or supplementary food during the reporting period" in label):
        FNTHERLabelLookUp[key]=label
        FNTHERAnchorNumber[key]=valueData
    if(label.startswith("Age:")):
        allList=allAges.get(key,[])
        allList.append(str(label)+"||"+str(valueData))
        val=FNTHERAggTotal.get(key,0)
        allAges[key]=allList
        FNTHERAggTotal[key]=val+valueData
    if("Number of PLHIV who were nutritionally assessed and found to be clinically undernourished" in label): #to hold the "clinically undernourished" number
        FNTHERLabelOther[key]=label
        FNTHEROther[key]=valueData

def processFNTHER(cursor,targetYear,country,copYear):   #indicators like FN_THER
    #dictionary to aggregate data
    FNTHERLabelLookUp={}
    FNTHERAnchorNumber={}
    FNTHERAggTotal={} #Keys are <18,18+
    FNTHERUnknown={}
    allAges={}
    FNTHERLabelOther={} #to hold "Number of PLHIV who were nutritionally assessed and found to be clinically undernourished" label
    FNTHEROther={} #to hold "Number of PLHIV who were nutritionally assessed and found to be clinically undernourished" value data
    FNTHEROtherUnknown={} #to hold the unknown number of clinically undernourished ppl who did not receive any therapeutic treatment
    MechAgg={}
    #process
    dataRows=fetchRowsByIndicator(cursor,targetYear,country,copYear,"FN_THER")
    for row in dataRows:
        aggregateFNTHER(targetYear,country,str(row['INDICATOR']),str(row['LABEL']),row['VALUE'],str(row['MECHID']),FNTHERLabelLookUp,FNTHERAnchorNumber,FNTHERAggTotal,allAges,FNTHERLabelOther,FNTHEROther)
    aggregateByMechanismFNTHER(MechAgg,dataRows)

    for key in FNTHERAnchorNumber:
        FNTHERUnknown[key]=FNTHERAnchorNumber[key]-FNTHERAggTotal.get(key,0)
        FNTHEROtherUnknown[key]=FNTHEROther.get(key,0)-FNTHERAnchorNumber[key]

    for key in FNTHERAnchorNumber:
        data=key.split(":")
        mechIDData=str(data[0])
        strInd=getIndicatorString(str(data[3]))                  #Splits indicator
        service=getServiceString(str(data[3]))
        MechAggDict=MechAgg[mechIDData]
        AgeGroup= MechAggDict.get("AgeGroup",{})
        FNTHERUnknownVal=FNTHERUnknown.get(key,0)
        labelData=FNTHERLabelLookUp.get(key,"")
        allList=allAges.get(key,[])
        Age18Plus=MechAggDict.get("Age18Plus",{})
        Age18Less=MechAggDict.get("Age18Less",{})
        Age18PlusUnknown=0
        Age18LessUnknown=0

        newRow={}
        newRow["MECHID"]=str(data[0])
        newRow["COPCC"]=str(data[1])
        newRow["tYear"]=str(data[2])
        newRow["INDICATOR"]=strInd
        newRow["SERVICE"]=service
        newRow["LABEL"]=labelData
        newRow["SEX"]="ALL"
        for allkey in allList:
            ageSplit=allkey.split("||")
            newRow["VALUE"]=ageSplit[1]
            age=ageSplit[0].split(":")[1]
            newRow["AGE"]=age
            newRow["DISAGGS"]=" "
            insertNewData(newRow)

        for ageKey in Age18Plus: #Evaluating uknowns only
            if "+" in ageKey:
                compare="+"
                match=re.search(r"(\d)+",ageKey)
                if match:
                    age=int(match.group())
                    sum18Plus=findSum(AgeGroup,age,compare)
                    Age18PlusUnknown=int(Age18Plus.get(ageKey,0))-sum18Plus
                    newRow["AGE"]="18+ Unknown"
                    newRow["DISAGGS"]=" "
                    newRow["VALUE"]=Age18PlusUnknown
                    insertNewData(newRow)

        for ageKey in Age18Less: #Evaluating uknowns only
            compare="-"
            match=re.search(r"(\d)+",ageKey)
            if match:
                age=int(match.group())
                sum18Lesss=findSum(AgeGroup,age,compare)
                Age18LessUnknown=int(Age18Less.get(ageKey,0))-sum18Lesss
                newRow["AGE"]="18< Unknown"
                newRow["DISAGGS"]=" "
                newRow["VALUE"]=Age18LessUnknown
                insertNewData(newRow)

        newRow["DISAGGS"]="Anchor/Unknown"
        newRow["AGE"]="ALL"
        newRow["VALUE"]=str(FNTHERUnknownVal)
        insertNewData(newRow)

        newRow["LABEL"]=FNTHERLabelOther.get(key,"") #Label=Number of PLHIV who were nutritionally assessed and found to be clinically undernourished and receive any therapeutic and/or supplementary food "
        newRow["DISAGGS"]=FNTHERLabelOther.get(key,"")
        newRow["AGE"]="ALL"
        newRow["VALUE"]=str(FNTHEROther.get(key,0))
        insertNewData(newRow)

        newRow["LABEL"]=FNTHERLabelOther.get(key,"")
        newRow["DISAGGS"]="Number of PLHIV who were nutritionally assessed and found to be clinically undernourished but did not receive any therapeutic and/or supplementary food "
        newRow["AGE"]="ALL"
        newRow["VALUE"]=str(FNTHEROtherUnknown.get(key,0))
        insertNewData(newRow)

def aggregateByMechanismFNTHER(MechAgg,dataRows):
    regex1=r"(\d)+-(\d)+"
    regex2=r"(\d)+\+"
    regex7=r"\<(\d)+"

    for row in dataRows:
        #Construct mech agg key
        mechKey=str(row['MECHID'])
        MechAggDict=MechAgg.get(mechKey,{}) #either fetch already hashed dict or empty dictionary
        Age18Plus=MechAggDict.get("Age18Plus",{}) #For c21d indicator case
        Age18Less=MechAggDict.get("Age18Less",{})
        AgeGroup=MechAggDict.get("AgeGroup",{})
        #NegativetResult=MechAggDict.get("Negative",{})
        strLabel=str(row['LABEL'])
        if (strLabel.startswith("Age:")):
            match=re.search(regex1,strLabel)
            if match:
                key=match.group()
                AgeGroup[key]=int(row['VALUE'])
            else:
                if (strLabel.find("+")!=-1):
                    match=re.search(regex2,strLabel)
                    if match:
                        key=match.group()
                        AgeGroup[key]=int(row['VALUE'])
                if (strLabel.find("<")!=-1):
                    match=re.search(regex7,strLabel)
                    if match:
                        key=match.group()
                        AgeGroup[key]=int(row['VALUE'])

        if (strLabel.startswith("Aggregated")):
            if (strLabel.find("+")!=-1):
                match=re.search(regex2,strLabel)
                if match:
                    key=match.group()
                    Age18Plus[key]=int(row['VALUE'])
                    print("Age18Plus[key]::"+str(Age18Plus[key]))
            else:
                if (strLabel.find("<")!=-1):
                    match=re.search(regex7,strLabel)
                    if match:
                        key=match.group()
                        Age18Less[key]=int(row['VALUE'])
                        #print("Age18Less[key]::"+str(Age18Less[key]))
        MechAggDict["Age18Plus"]=Age18Plus
        MechAggDict["Age18Less"]=Age18Less
        MechAggDict["AgeGroup"]=AgeGroup
        MechAgg[mechKey]=MechAggDict

#*************************End FNTHER Operations*************************************************************************************************************************************

#*************************Start TB_ART Operations*************************************************************************************************************************************
#NOTE::TB_ART_DSD and TB_SCREEN_DSD have age value aggregated by same age bar, that is 15+ and <15

def aggregateTBART(targetYear,country,indicator,label,valueData,mechid,TBARTLabelLookUp,TBARTAnchorNumber,TBARTAggTotal,allAges,TBARTOther,TBARTLabelOther):
    key=mechid+":"+country+":"+targetYear+":"+indicator  #key for dictionary values
    if("The number of registered new and relapse TB cases with documented HIV-positive status whoare on ART during TB treatment during the reporting period" in label):
        TBARTLabelLookUp[key]=label
        TBARTAnchorNumber[key]=valueData
    if(label.startswith("Age:")):
        ageList=allAges.get(key,[])
        ageList.append(str(label)+"||"+str(valueData))
        val=TBARTAggTotal.get(key,0)
        allAges[key]=ageList
        TBARTAggTotal[key]=val+valueData
    if("The number of registered new and relapse TB cases with documented HIV-positive status during TB treatment during the reporting period" in label): #to hold the "clinically undernourished" number
        TBARTLabelOther[key]=label  #Holds label of The number of registered new and relapse TB cases with documented HIV-positive status during TB treatment during the reporting periods
        TBARTOther[key]=valueData  #Holds value of The number of registered new and relapse TB cases with documented HIV-positive status during TB treatment during the reporting period

def processTBART(cursor,targetYear,country,copYear): #process TB_ART_DSD
    TBARTLabelLookUp={}
    TBARTAnchorNumber={}
    TBARTAggTotal={} #Keys are 15+ male,15- Female, <15 Male, <15 Female, 15+,<15,Male, Female
    TBARTUnknown={}
    MechAgg={}
    allAges={}
    TBARTOther={}
    TBARTLabelOther={}
    TBARTOtherUnknown={}
    #process
    dataRows=fetchRowsByIndicator(cursor,targetYear,country,copYear,"TB_ART_DSD")
    for row in dataRows:
        aggregateTBART(targetYear,country,str(row['INDICATOR']),str(row['LABEL']),row['VALUE'],str(row['MECHID']),TBARTLabelLookUp,TBARTAnchorNumber,TBARTAggTotal,allAges,TBARTOther,TBARTLabelOther)
    aggregateByMechanismTB_ART_SCREEN(MechAgg,dataRows)

    for key in TBARTAnchorNumber:
        TBARTUnknown[key]=TBARTAnchorNumber[key]-TBARTAggTotal.get(key,0)
        TBARTOtherUnknown[key]=TBARTOther.get(key,0)-TBARTAnchorNumber[key]

    for key in TBARTAnchorNumber:
        data=key.split(":")
        mechIDData=str(data[0])
        MechAggDict=MechAgg[mechIDData]
        AgeGroup= MechAggDict.get("AgeGroup",{})

        TBARTUnknownVal=TBARTUnknown.get(key,0)
        allList=allAges.get(key,[])
        Age15Plus=MechAggDict.get("Age15Plus",{})
        Age15Less=MechAggDict.get("Age15Less",{})
        Age15PlusUnknown=0  #collector to accumulate values of age group 15+
        Age15LessUnknown=0
        newRow={}
        newRow["MECHID"]=str(data[0])
        newRow["COPCC"]=str(data[1])
        newRow["tYear"]=str(data[2])
        newRow["INDICATOR"]=getIndicatorString(str(data[3]))
        newRow["SERVICE"]=getServiceString(str(data[3]))
        newRow["LABEL"]=TBARTLabelLookUp.get(key,"")
        newRow["SEX"]="ALL"

        for allKey in allList:
            ageSplit=allKey.split("||")
            newRow["VALUE"]=ageSplit[1]
            age=ageSplit[0].split(":")[1]
            newRow["AGE"]=age
            newRow["DISAGGS"]=" "
            insertNewData(newRow)

        for ageKey in Age15Plus:  #Evaluating uknowns only
            if "+" in ageKey:
                compare="+"
            else:
                compare="-"
            match=re.search(r"(\d)+",ageKey)
            if match:
                age=int(match.group())
                sum15Plus=findSum(AgeGroup,age,compare)
            Age15PlusUnknown=int(Age15Plus.get(ageKey,0))-sum15Plus
            newRow["DISAGGS"]=" "
            newRow["AGE"]="15+ UNKNOWN"
            newRow["VALUE"]=Age15PlusUnknown
            insertNewData(newRow)

        for agekey in Age15Less:   #Evaluating uknowns only
            compare="-"
            match=re.search(r"(\d)+",agekey)
            if match:
                age=int(match.group())
                sum15Less=findSum(AgeGroup,age,compare)
            Age15LessUnknown=int(Age15Less.get(agekey,0))-sum15Less
            newRow["DISAGGS"]=" "
            newRow["AGE"]="15< UNKNOWN"
            newRow["VALUE"]=Age15LessUnknown
            insertNewData(newRow)

        newRow["LABEL"]=TBARTLabelLookUp.get(key,"")
        newRow["DISAGGS"]="Anchor/Unknown"
        newRow["AGE"]="ALL"
        newRow["VALUE"]=str(TBARTUnknownVal)
        insertNewData(newRow)

        newRow["LABEL"]=TBARTLabelLookUp.get(key,"")
        newRow["DISAGGS"]=TBARTLabelOther.get(key,"")
        newRow["AGE"]="ALL"
        newRow["VALUE"]=str(TBARTOther.get(key,0))
        insertNewData(newRow)

        newRow["LABEL"]=TBARTLabelLookUp.get(key,"")
        newRow["DISAGGS"]="The number of registered new and relapse TB cases with documented HIV-positive status who are NOT on ART during TB treatment during the reporting period "
        newRow["AGE"]="ALL"
        newRow["VALUE"]=str(TBARTOtherUnknown.get(key,0))
        insertNewData(newRow)
#*************************End TB_ART Operations*************************************************************************************************************************************

#*************************Start TB_SCREEN_DSD Operations*************************************************************************************************************************************
def aggregateTBSCREEN(targetYear,country,indicator,label,valueData,mechid,TBSCREENLabelLookUp,TBSCREENAnchorNumber,TBSCREENAggTotal,allAges):
    key=mechid+":"+country+":"+targetYear+":"+indicator  #key for dictionary values
    if("Number " in label):
        TBSCREENLabelLookUp[key]=label
        TBSCREENAnchorNumber[key]=valueData
    if(label.startswith("Age:")):
        allList=allAges.get(key,[])
        allList.append(str(label)+"||"+str(valueData))
        val=TBSCREENAggTotal.get(key,0)
        allAges[key]=allList
        TBSCREENAggTotal[key]=val+valueData

def processTBSCREEN(cursor,targetYear,country,copYear):   #indicators like FN_THER
    #dictionary to aggregate data
    TBSCREENLabelLookUp={}
    TBSCREENAnchorNumber={}
    TBSCREENAggTotal={} #Keys are <18,18+
    TBSCREENUnknown={}
    allAges={}
    MechAgg={}
    #process
    dataRows=fetchRowsByIndicator(cursor,targetYear,country,copYear,"TB_SCREEN")
    for row in dataRows:
        aggregateTBSCREEN(targetYear,country,str(row['INDICATOR']),str(row['LABEL']),row['VALUE'],str(row['MECHID']),TBSCREENLabelLookUp,TBSCREENAnchorNumber,TBSCREENAggTotal,allAges)
    aggregateByMechanismTB_ART_SCREEN(MechAgg,dataRows)

    for key in TBSCREENAnchorNumber:
        TBSCREENUnknown[key]=TBSCREENAnchorNumber[key]-TBSCREENAggTotal.get(key,0)

    for key in TBSCREENAnchorNumber:
        data=key.split(":")
        mechIDData=str(data[0])
        MechAggDict=MechAgg[mechIDData]
        AgeGroup= MechAggDict.get("AgeGroup",{})
        TBSCREENUnknownVal=TBSCREENUnknown.get(key,0)
        labelData=TBSCREENLabelLookUp.get(key,"")
        allList=allAges.get(key,[])
        Age15Plus=MechAggDict.get("Age15Plus",{})
        Age15Less=MechAggDict.get("Age15Less",{})
        Age15PlusUnknown=0
        Age15LessUnknown=0

        newRow={}
        newRow["MECHID"]=str(data[0])
        newRow["COPCC"]=str(data[1])
        newRow["tYear"]=str(data[2])
        newRow["INDICATOR"]=getIndicatorString(str(data[3]))
        newRow["SERVICE"]=getServiceString(str(data[3]))
        newRow["LABEL"]=labelData
        newRow["SEX"]="ALL"
        for allkey in allList:
            ageSplit=allkey.split("||")
            newRow["VALUE"]=ageSplit[1]
            age=ageSplit[0].split(":")[1]
            newRow["AGE"]=age
            newRow["DISAGGS"]=" "
            insertNewData(newRow)

        for ageKey in Age15Plus: #Evaluating uknowns only
            if "+" in ageKey:
                compare="+"
                match=re.search(r"(\d)+",ageKey)
                if match:
                    age=int(match.group())
                    sum15Plus=findSum(AgeGroup,age,compare)
                    Age15PlusUnknown=int(Age15Plus.get(ageKey,0))-sum15Plus
                    newRow["AGE"]="15+ Unknown"
                    newRow["DISAGGS"]=" "
                    newRow["VALUE"]=Age15PlusUnknown
                    insertNewData(newRow)

        for ageKey in Age15Less: #Evaluating uknowns only
            compare="-"
            match=re.search(r"(\d)+",ageKey)
            if match:
                age=int(match.group())
                sum15Lesss=findSum(AgeGroup,age,compare)
                Age15LessUnknown=int(Age15Less.get(ageKey,0))-sum15Lesss
                newRow["AGE"]="15< Unknown"
                newRow["DISAGGS"]=" "
                newRow["VALUE"]=Age15LessUnknown
                insertNewData(newRow)

        newRow["DISAGGS"]="Anchor/Unknown"
        newRow["AGE"]="ALL"
        newRow["VALUE"]=str(TBSCREENUnknownVal)
        insertNewData(newRow)
#*************************End TB_SCREEN_DSD Operations*************************************************************************************************************************************

#*************************Start TX_CURR_DSD Operations*************************************************************************************************************************************
def processTXCURR(cursor,targetYear,country,copYear):
    TX_NEW_DSD_CURRLabelLookUp={}
    TX_NEW_DSD_CURRAnchorNumber={}
    TX_NEW_DSD_CURRAggTotal={}
    TX_NEW_DSD_CURRUnknown={}
    MechAgg={}

    dataRows=fetchRowsByIndicator(cursor,targetYear,country,copYear,"TX_CURR_")
    for row in dataRows:
        aggregateTXCURR_TXNEW(targetYear,country,str(row['INDICATOR']),str(row['LABEL']),row['VALUE'],str(row['MECHID']),TX_NEW_DSD_CURRLabelLookUp,TX_NEW_DSD_CURRAnchorNumber,TX_NEW_DSD_CURRAggTotal)
    aggregateByMechanism(MechAgg,dataRows)

    for key in TX_NEW_DSD_CURRAnchorNumber:
        TX_NEW_DSD_CURRUnknown[key]=TX_NEW_DSD_CURRAnchorNumber[key]-TX_NEW_DSD_CURRAggTotal.get(key,0)

    if (len(TX_NEW_DSD_CURRAnchorNumber)==0):
        return

    for key in TX_NEW_DSD_CURRAnchorNumber:
        data=key.split(":")   #where data=mechid+":"+country+":"+targetYear+":"+indicator
        mechIDData=str(data[0])

        MechAggDict=MechAgg[mechIDData]
        Male= MechAggDict.get("Male",{})
        Female=MechAggDict.get("Female",{})
        AggMale= MechAggDict.get("AggMale",{})
        AggFemale=MechAggDict.get("AggFemale",{})
        TXCURRUnknownVal=TX_NEW_DSD_CURRUnknown.get(key,0)
        labelData=TX_NEW_DSD_CURRLabelLookUp.get(key,"")
        newRow={}
        newRow["MECHID"]=mechIDData
        newRow["COPCC"]=str(data[1])
        newRow["tYear"]=str(data[2])
        newRow["INDICATOR"]=getIndicatorString(str(data[3]))
        newRow["SERVICE"]=getServiceString(str(data[3]))
        newRow["LABEL"]=labelData
        newRow["DISAGGS"]=" "

    for mkey in Male:
        newRow["SEX"]="MALE"
        newRow["AGE"]=mkey
        newRow["VALUE"]=Male.get(mkey,"")
        insertNewData(newRow)
    for fkey in Female:
        newRow["SEX"]="FEMALE"
        newRow["AGE"]=fkey
        newRow["VALUE"]=Female.get(fkey,"")
        insertNewData(newRow)

    for keyMale in AggMale:
        unknownAggMale=0
        if "+" in keyMale:
            compare="+"
        else:
            compare="-"
        match=re.search(r"(\d)+",keyMale)
        if match:
            age=int(match.group())
            sumMale=findSum(Male,age,compare)
        unknownAggMale=int(AggMale.get(keyMale,0))-sumMale
        if("15" in keyMale):
            newRow["VALUE"]=unknownAggMale
            newRow["AGE"]=keyMale.split(" ")[0]
            newRow["SEX"]="MALE UNKNOWN"
            insertNewData(newRow)

    for keyFemale in AggFemale:
        if "+" in keyFemale:
            compare="+"
        else:
            compare="-"
        match=re.search(r"(\d)+",keyFemale)
        if match:
            age=int(match.group())
            sumFemale=findSum(Female,age,compare)
            unknownAggFemale=int(AggFemale.get(keyFemale,0))-sumFemale
        if("15" in keyFemale):
            newRow["VALUE"]=unknownAggFemale
            newRow["AGE"]=keyFemale.split(" ")[0]
            newRow["SEX"]="FEMALE UNKNOWN"
            insertNewData(newRow)

    newRow["LABEL"]=labelData
    newRow["DISAGGS"]="Anchor/Unknown"
    newRow["AGE"]="ALL"
    newRow["SEX"]="ALL"
    newRow["VALUE"]=str(TXCURRUnknownVal)
    insertNewData(newRow)
#*************************End TX_CURR_DSD Operations*************************************************************************************************************************************

#*************************Start TX_DIST Operations*************************************************************************************************************************************
def aggregateTXDIST(targetYear,country,indicator,label,VALUEData,mechid,TXDISTLabelLookUp,TXDISTAnchorNumber,TXDISTAggTotal):
    key=mechid+":"+country+":"+targetYear+":"+indicator
    if(label.startswith("Total")):
        TXDISTLabelLookUp[key]=label
        TXDISTAnchorNumber[key]=VALUEData
    if(label.startswith("Number")):
        TXDISTAggTotal[key]=VALUEData

def processTX_DIST(cursor,targetYear,country,copYear):  # indicators starting with TX_DIST
    TXDISTLabelLookUp={}
    TXDISTAnchorNumber={}
    TXDISTUnknown={}
    TXDISTAggTotal={}
    dataRows=fetchRowsByIndicator(cursor,targetYear,country,copYear,"TX_DIST")
    for row in dataRows:
        aggregateTXDIST(targetYear,country,str(row['INDICATOR']),str(row['LABEL']),row['VALUE'],str(row['MECHID']),TXDISTLabelLookUp,TXDISTAnchorNumber,TXDISTAggTotal)

    for key in TXDISTAnchorNumber:
        TXDISTUnknown[key]=TXDISTAnchorNumber[key]-TXDISTAggTotal.get(key,0)

    if (len(TXDISTAnchorNumber)==0):
        return

    for key in TXDISTAnchorNumber:
        data=key.split(":")
        mechIDData=str(data[0])
        newRow={}
        newRow["MECHID"]=mechIDData
        newRow["COPCC"]=str(data[1])
        newRow["tYear"]=str(data[2])
        newRow["INDICATOR"]=getIndicatorString(str(data[3]))
        newRow["SERVICE"]=getServiceString(str(data[3]))
        newRow["LABEL"]=TXDISTLabelLookUp.get(key,"")
        newRow["VALUE"]=TXDISTUnknown.get(key,0)
        newRow["AGE"]=""
        newRow["SEX"]=""
        newRow["DISAGGS"]="Number of Districts with NO documented routine supportive supervision visits to 75% of HIV care and treatment sites supported by the District"
        insertNewData(newRow)
        newRow["LABEL"]=TXDISTLabelLookUp.get(key,"")
        newRow["DISAGGS"]="Number of Districts with documented routine supportive supervision visits to 75% of HIV care and treatment sites supported by the District"
        newRow["AGE"]=""
        newRow["SEX"]=""
        newRow["VALUE"]=TXDISTAggTotal.get(key,0)
        insertNewData(newRow)
#*************************End TX_dist Operations*************************************************************************************************************************************

#*************************Start TX_NEW_DSD Operations*************************************************************************************************************************************
def processTX_NEW_DSD(cursor,targetYear,country,copYear):
    TX_NEW_DSD_CURRLabelLookUp={}
    TX_NEW_DSD_CURRAnchorNumber={}
    TX_NEW_DSD_CURRAggTotal={}
    TX_NEW_DSD_CURRUnknown={}
    MechAgg={}

    dataRows=fetchRowsByIndicator(cursor,targetYear,country,copYear,"TX_NEW_")
    for row in dataRows:
        aggregateTXCURR_TXNEW(targetYear,country,str(row['INDICATOR']),str(row['LABEL']),row['VALUE'],str(row['MECHID']),TX_NEW_DSD_CURRLabelLookUp,TX_NEW_DSD_CURRAnchorNumber,TX_NEW_DSD_CURRAggTotal)

    aggregateByMechanism(MechAgg,dataRows)

    for key in TX_NEW_DSD_CURRAnchorNumber:
        TX_NEW_DSD_CURRUnknown[key]=TX_NEW_DSD_CURRAnchorNumber[key]-TX_NEW_DSD_CURRAggTotal.get(key,0)

    if (len(TX_NEW_DSD_CURRAnchorNumber)==0):
        return

    for key in TX_NEW_DSD_CURRAnchorNumber:
        data=key.split(":")   #where data=mechid+":"+country+":"+targetYear+":"+indicator
        mechIDData=str(data[0])
        MechAggDict=MechAgg[mechIDData]
        Male= MechAggDict.get("Male",{})
        Female=MechAggDict.get("Female",{})
        AggMale= MechAggDict.get("AggMale",{})
        AggFemale=MechAggDict.get("AggFemale",{})
        labelData=TX_NEW_DSD_CURRLabelLookUp.get(key,"")
        newRow={}
        newRow["MECHID"]=mechIDData
        newRow["COPCC"]=str(data[1])
        newRow["tYear"]=str(data[2])
        newRow["INDICATOR"]=getIndicatorString(str(data[3]))
        newRow["SERVICE"]=getServiceString(str(data[3]))
        newRow["LABEL"]=labelData
        newRow["DISAGGS"]=" "

    for mkey in Male:
        newRow["SEX"]="MALE"
        newRow["AGE"]=mkey
        newRow["VALUE"]=Male.get(mkey,"")
        insertNewData(newRow)
    for fkey in Female:
        newRow["SEX"]="FEMALE"
        newRow["AGE"]=fkey
        newRow["VALUE"]=Female.get(fkey,"")
        insertNewData(newRow)

    for keyMale in AggMale:
        unknownAggMale=0
        if "+" in keyMale:
            compare="+"
        else:
            compare="-"
        match=re.search(r"(\d)+",keyMale)
        if match:
            age=int(match.group())
            sumMale=findSum(Male,age,compare)
        unknownAggMale=int(AggMale.get(keyMale,0))-sumMale
        if("15" in keyMale):
            newRow["VALUE"]=unknownAggMale
            newRow["AGE"]=keyMale.split(" ")[0]
            newRow["SEX"]="MALE UNKNOWN"
            insertNewData(newRow)

    for keyFemale in AggFemale:
        if "+" in keyFemale:
            compare="+"
        else:
            compare="-"
        match=re.search(r"(\d)+",keyFemale)
        if match:
            age=int(match.group())
            sumFemale=findSum(Female,age,compare)
            unknownAggFemale=int(AggFemale.get(keyFemale,0))-sumFemale
        if("15" in keyFemale):
            newRow["VALUE"]=unknownAggFemale
            newRow["AGE"]=keyFemale.split(" ")[0]
            newRow["SEX"]="FEMALE UNKNOWN"
            insertNewData(newRow)
    newRow["LABEL"]=labelData
    newRow["DISAGGS"]="Anchor/Unknown"
    newRow["AGE"]="ALL"
    newRow["SEX"]="ALL"
    newRow["VALUE"]=str(TX_NEW_DSD_CURRUnknown.get(key,0))
    insertNewData(newRow)
#*************************End TX_NEW_DSD Operations*************************************************************************************************************************************

#*************************START TX_RET_DSD Operations*************************************************************************************************************************************
def aggregateTX_RET_DSD(targetYear,country,indicator,label,valueData,mechid,TX_RET_DSDLabelLookUp,TX_RET_DSDAnchorNumber,TX_RET_DSDAggTotal):
    key=mechid+":"+country+":"+targetYear+":"+indicator
    val=0
    if(label.startswith("Number of")):
        TX_RET_DSDLabelLookUp[key]=label
        TX_RET_DSDAnchorNumber[key]=valueData
    else:
        if (label.startswith("Aggregated")):###Get the aggregate data
            val=TX_RET_DSDAggTotal.get(key,0)
            TX_RET_DSDAggTotal[key]=val+valueData

def processTX_RET_DSD(cursor,targetYear,country,copYear):  ###Process all indicators starting with HTC
    #Dictionary to aggregate data
    TX_RET_DSDLabelLookUp={}
    TX_RET_DSDAnchorNumber={}
    TX_RET_DSDAggTotal={}
    TX_RET_DSDUnknown={}
    MechAgg={}
    #Process all health indicators that are in the format of TX_RET_DSD
    dataRows=fetchRowsByIndicator(cursor,targetYear,country,copYear,"TX_RET_DSD")
    for row in dataRows:
        aggregateTX_RET_DSD(targetYear,country,str(row['INDICATOR']),str(row['LABEL']),row['VALUE'],str(row['MECHID']),TX_RET_DSDLabelLookUp,TX_RET_DSDAnchorNumber,TX_RET_DSDAggTotal)

    aggregateByMechanismTX_RET_DSD(MechAgg,dataRows)

    for key in TX_RET_DSDAnchorNumber:
        TX_RET_DSDUnknown[key]=TX_RET_DSDAnchorNumber[key]-TX_RET_DSDAggTotal.get(key,0)

    for key in TX_RET_DSDAnchorNumber:
        data=key.split(":")
        mechIDData=str(data[0])
        MechAggDict=MechAgg[mechIDData]
        Denominator= MechAggDict.get("Denominator",{})
        Numerator=MechAggDict.get("Numerator",{})
        AggDenominator= MechAggDict.get("AggDenominator",{})
        AggNumerator=MechAggDict.get("AggNumerator",{})

        newRow={}
        newRow["MECHID"]=mechIDData
        newRow["COPCC"]=str(data[1])
        newRow["tYear"]=str(data[2])
        newRow["INDICATOR"]=getIndicatorString(str(data[3]))
        newRow["SERVICE"]=getServiceString(str(data[3]))
        newRow["LABEL"]=TX_RET_DSDLabelLookUp.get(key,"")
        newRow["SEX"]=""
        for Dkey in Denominator:
            newRow["DISAGGS"]=" "
            newRow["AGE"]=Dkey
            newRow["VALUE"]=Denominator.get(Dkey,"")
            insertNewData(newRow)
        for Fkey in Numerator:
            newRow["DISAGGS"]=" "
            newRow["AGE"]=Fkey
            newRow["VALUE"]=Numerator.get(Fkey,"")
            insertNewData(newRow)
        for keyDen in AggDenominator:
            unknownAggDenominator=0
            if "+" in keyDen:
                compare="+"
            else:
                compare="-"
            match=re.search(r"(\d)+",keyDen)
            if match:
                age=int(match.group())
                sumDenominator=findSum(Denominator,age,compare)
            unknownAggDenominator=int(AggDenominator.get(keyDen,0))-sumDenominator
            newRow["VALUE"]=unknownAggDenominator
            newRow["AGE"]=keyDen.split(" ")[0]
            newRow["DISAGGS"]="AGGREGATE UNKNOWN DENOMINATOR"
            insertNewData(newRow)
        for keyNum in AggNumerator:
            if "+" in keyNum:
                compare="+"
            else:
                compare="-"
            match=re.search(r"(\d)+",keyNum)
            if match:
                age=int(match.group())
                sumNumerator=findSum(Numerator,age,compare)
                unknownNumerator=int(AggNumerator.get(keyNum,0))-sumNumerator
            newRow["VALUE"]=unknownNumerator
            newRow["AGE"]=keyNum.split(" ")[0]
            newRow["DISAGGS"]="Aggregate NUMERATOR UNKNOWN"
            insertNewData(newRow)

def aggregateByMechanismTX_RET_DSD(MechAgg,dataRows):
    regex1=r"(\d)+-(\d)+"                        #0-7
    regex2=r"(\d)+\+"                           #15+
    regex3=r"(\d)+\+(\s)*(\(Denominator)"        #15+ (Denominator
    regex4=r"(\d)+\+(\s)*(\(Numerator)"           #15+ (Numerator
    regex5=r"\<(\d)+(\s)*(\(Denominator)"         #<15 (Denpminator
    regex6=r"\<(\d)+(\s)*(\(Numerator)"           #<15 Numerator
    #regex7=r"\<(\d)+"
    for row in dataRows:
        #Construct mech agg key
        mechKey=str(row['MECHID'])
        MechAggDict=MechAgg.get(mechKey,{}) #either fetch already hashed dict or empty dictionary
        Denominator=MechAggDict.get("Denominator",{})
        Numerator=MechAggDict.get("Numerator",{})
        AggDenominator=MechAggDict.get("AggDenominator",{})
        AggNumerator= MechAggDict.get("AggNumerator",{})

        strLabel=str(row['LABEL'])
        if (strLabel.startswith("Age")):
            match=re.search(regex1,strLabel)
            if match:
                key=match.group()
            else:
                if("+" in strLabel):
                    match=re.search(regex2,strLabel)
                    if match:
                        key=match.group()
            if match:
                if("Denominator" in strLabel):
                    Denominator[key]=int(row['VALUE'])
                else:
                    if("Numerator" in strLabel):
                        Numerator[key]=int(row['VALUE'])
        elif(strLabel.startswith("Aggregated")):
            match=re.search(regex3,strLabel)
            if match:
                key=match.group()
            else:
                match=re.search(regex4,strLabel)
                if match:
                    key=match.group()
                else:
                    match=re.search(regex5,strLabel)
                    if match:
                        key=match.group()
                    else:
                        match=re.search(regex6,strLabel)
            if match:
                if("Denominator" in strLabel):
                    AggDenominator[key]=int(row['VALUE'])
                    print("AggDenominator[key]:"+str(AggDenominator[key]))
                else:
                    if("Numerator" in strLabel):
                        AggNumerator[key]=int(row['VALUE'])
        MechAggDict["Denominator"]=Denominator
        MechAggDict["Numerator"]=Numerator
        MechAggDict["AggDenominator"]=AggDenominator
        MechAggDict["AggNumerator"]=AggNumerator
        MechAgg[mechKey]=MechAggDict
#*************************START TX_RET_DSD Operations*************************************************************************************************************************************

#*************************START PP_PREV_DSD Operations*************************************************************************************************************************************
def aggregatePP_PREV(targetYear,country,indicator,label,valueData,mechid,PPREVLabelLookUp,PPREVAnchorNumber,PPREVAggTotal,PPREVOtherAnchor):
    key=mechid+":"+country+":"+targetYear+":"+indicator
    val=0
    if(label.startswith("Number of")):
        PPREVLabelLookUp[key]=label
        PPREVAnchorNumber[key]=valueData
    if(label.startswith("Age")):
        val=PPREVAggTotal.get(key,0)
        PPREVAggTotal[key]=val+valueData
    if(label.startswith("Total")):
        PPREVOtherAnchor[key]=valueData  #PPREVOtherAnchor holds label description for "Total number of people in the target population"

def processPP_PREV(cursor,targetYear,country,copYear):
    PPREVLabelLookUp={}
    PPREVAnchorNumber={}
    PPREVOtherAnchor={}
    PPREVAggTotal={}
    PPREVUnknown={}
    PPREVUnknownTarget={} #Holds unknown value for "Total number of people in the target population (subtract) Number of target pop who completed HIV prevention intervention
    MechAgg={}
    dataRows=fetchRowsByIndicator(cursor,targetYear,country,copYear,"PP_PREV")
    for row in dataRows:
        aggregatePP_PREV(targetYear,country,str(row['INDICATOR']),str(row['LABEL']),row['VALUE'],str(row['MECHID']),PPREVLabelLookUp,PPREVAnchorNumber,PPREVAggTotal,PPREVOtherAnchor)
    aggregateByMechanism(MechAgg,dataRows)

    for key in PPREVAnchorNumber:
        PPREVUnknown[key]=PPREVAnchorNumber[key]-PPREVAggTotal.get(key,0)
        PPREVUnknownTarget[key]=PPREVOtherAnchor.get(key,0)-PPREVAnchorNumber.get(key,0)

    for key in PPREVAnchorNumber:
        data=key.split(":")
        mechIDData=str(data[0])
        newRow={}
        newRow["MECHID"]=mechIDData
        newRow["COPCC"]=str(data[1])
        newRow["tYear"]=str(data[2])
        newRow["INDICATOR"]=getIndicatorString(str(data[3]))
        newRow["SERVICE"]=getServiceString(str(data[3]))
        newRow["LABEL"]=PPREVLabelLookUp.get(key,"")
        newRow["DISAGGS"]=""
        MechAggDict=MechAgg[mechIDData]
        Male= MechAggDict.get("Male",{})
        Female=MechAggDict.get("Female",{})
        for mkey in Male:
            newRow["SEX"]="MALE"
            newRow["AGE"]=mkey
            newRow["VALUE"]=Male.get(mkey,"")
            insertNewData(newRow)
        for fkey in Female:
            newRow["SEX"]="FEMALE"
            newRow["AGE"]=fkey
            newRow["VALUE"]=Female.get(fkey,"")
            insertNewData(newRow)
        newRow["DISAGGS"]="Anchor/Unknown"
        newRow["LABEL"]=PPREVLabelLookUp.get(key,"")
        newRow["AGE"]="ALL"
        newRow["SEX"]="ALL"
        newRow["VALUE"]=str(PPREVUnknown.get(key,0))
        insertNewData(newRow)

        newRow["DISAGGS"]="Number of the target population who DID NOT complete a standardized HIV prevention intervention including the minimum components during the reporting period."
        newRow["LABEL"]=PPREVLabelLookUp.get(key,"")
        newRow["AGE"]="ALL"
        newRow["SEX"]="ALL"
        newRow["VALUE"]=str(PPREVUnknownTarget.get(key,0))
        insertNewData(newRow)
#*************************END PP_PREV_DSD Operations*************************************************************************************************************************************

#*************************START VMMC_CIRC Operations*************************************************************************************************************************************
def aggregateVMMC_CIRC(targetYear,country,indicator,label,valueData,mechid,VMMC_CIRCLabelLookUp,VMMC_CIRCAnchorNumber,VMMC_CIRCAggTotal,allAges,OtherLabels,byCircLabel,byCircTotal,byFollowUPlabel,byFollowUpTotal,byHIVStatusLabel,byHIVStatusTotal):
    key=mechid+":"+country+":"+targetYear+":"+indicator
    val=0
    if(label.startswith("Number of")):
        VMMC_CIRCLabelLookUp[key]=label
        VMMC_CIRCAnchorNumber[key]=valueData
    if(label.startswith("By Age:")):
        val=VMMC_CIRCAggTotal.get(key,0)
        allList=allAges.get(key,[])
        allList.append(str(label)+"||"+str(valueData))
        allAges[key]=allList
        VMMC_CIRCAggTotal[key]=val+valueData
        OtherLabels[key]=label
    if(label.startswith("By circumcision")):
        val=byCircTotal.get(key,0)
        byCircumcision=byCircLabel.get(key,[])
        byCircumcision.append(str(label)+"||"+str(valueData))  #data will be held in format of "By circumcision technique: Device-based VMMC|| 1223"
        byCircLabel[key]=byCircumcision
        byCircTotal[key]=val+valueData
    if(label.startswith("By follow")):
        val=byFollowUpTotal.get(key,0)
        byFollow=byFollowUPlabel.get(key,[])
        byFollow.append(str(label)+"||"+str(valueData))  #data will be held in format of "By circumcision technique: Device-based VMMC|| 1223"
        byFollowUPlabel[key]=byFollow
        byFollowUpTotal[key]=val+valueData

    if(label.startswith("By HIV status")):
        val=byHIVStatusTotal.get(key,0)
        byHIV=byHIVStatusLabel.get(key,[])
        byHIV.append(str(label)+"||"+str(valueData))  #data will be held in format of "By circumcision technique: Device-based VMMC|| 1223"
        byHIVStatusLabel[key]=byHIV
        byHIVStatusTotal[key]=val+valueData

def processVMMC_CIRC(cursor,targetYear,country,copYear):
    VMMC_CIRCAnchorNumber={}
    VMMC_CIRCLabelLookUp={}
    VMMC_CIRCAggTotal={}
    VMMC_CIRCUnknown={}
    allAges={} #container to hold all Age related values
    OtherLabels={} #labels like "By age:"..
    allCircumcision={} #ALL Labels starting with "By Circumcision Technique"
    byCircLabel={}
    byCircTotal={}
    byCircUnknown={} #hold unknown value for Circumcision Technique
    byFollowUPlabel={}
    byFollowUpTotal={}
    byFollowUpUnknown={}
    byHIVStatusLabel={}
    byHIVStatusTotal={}
    byHIVStatusUnknown={}
    dataRows=fetchRowsByIndicator(cursor,targetYear,country,copYear,"VMMC_CIRC")
    for row in dataRows:
        aggregateVMMC_CIRC(targetYear,country,str(row['INDICATOR']),str(row['LABEL']),row['VALUE'],str(row['MECHID']),VMMC_CIRCLabelLookUp,VMMC_CIRCAnchorNumber,VMMC_CIRCAggTotal,allAges,OtherLabels,byCircLabel,byCircTotal,byFollowUPlabel,byFollowUpTotal,byHIVStatusLabel,byHIVStatusTotal)

    for key in VMMC_CIRCAnchorNumber:
        VMMC_CIRCUnknown[key]=VMMC_CIRCAnchorNumber[key]-VMMC_CIRCAggTotal.get(key,0)   #Unkwnon age disaggregates for VMMC_CIRC

        if(int(byCircTotal.get(key,0))>0):  #Check if the "By Circumcision Technique" value is greater than zero else it will end up with value of
            byCircUnknown[key]=VMMC_CIRCAnchorNumber[key]-byCircTotal[key]  #unknown value for "By circumcision technique unknown VMMC_CIRCAnchorNumber which is error.
        if(int(byFollowUpTotal.get(key,0))>0):
            byFollowUpUnknown[key]=VMMC_CIRCAnchorNumber[key]-byFollowUpTotal[key]
        if(int(byHIVStatusTotal.get(key,0))>0):
            byHIVStatusUnknown[key]=VMMC_CIRCAnchorNumber[key]-byHIVStatusTotal[key]

    for key in VMMC_CIRCAnchorNumber:
        data=key.split(":")
        mechIDData=str(data[0])
        newRow={}
        newRow["MECHID"]=mechIDData
        newRow["COPCC"]=str(data[1])
        newRow["tYear"]=str(data[2])
        newRow["INDICATOR"]=getIndicatorString(str(data[3]))
        newRow["SERVICE"]=getServiceString(str(data[3]))
        newRow["LABEL"]=VMMC_CIRCLabelLookUp.get(key,"")
        allList=allAges.get(key,[])
        allCircumcision=byCircLabel.get(key,[])
        allFollowUp=byFollowUPlabel.get(key,[])
        allHIVStatus=byHIVStatusLabel.get(key,[])
        for allKey in allList:
            newRow["DISAGGS"]=""
            newRow["VALUE"]=allKey.split("||")[1]
            newRow["AGE"]=allKey.split("||")[0].split(":")[1]
            newRow["SEX"]="ALL"
            insertNewData(newRow)
        for allKey in allCircumcision:
            newRow["DISAGGS"]=allKey.split("||")[0]
            newRow["VALUE"]=allKey.split("||")[1]
            newRow["SEX"]="ALL"
            newRow["AGE"]="ALL"
            insertNewData(newRow)
        for allKey in allFollowUp:
            newRow["DISAGGS"]=allKey.split("||")[0]
            newRow["VALUE"]=allKey.split("||")[1]
            newRow["SEX"]="ALL"
            newRow["AGE"]="ALL"
            insertNewData(newRow)
        for allKey in allHIVStatus:
            newRow["DISAGGS"]=allKey.split("||")[0]
            newRow["VALUE"]=allKey.split("||")[1]
            newRow["SEX"]="ALL"
            newRow["AGE"]="ALL"
            insertNewData(newRow)

        newRow["DISAGGS"]="By Circumcision Technique: UNKNOWN"
        newRow["VALUE"]=str(byCircUnknown.get(key))
        newRow["SEX"]="ALL"
        newRow["AGE"]="ALL"
        insertNewData(newRow)

        newRow["DISAGGS"]="By Follow Up Status: UNKNOWN"
        newRow["VALUE"]=str(byFollowUpUnknown.get(key))
        newRow["SEX"]="ALL"
        newRow["AGE"]="ALL"
        insertNewData(newRow)

        newRow["DISAGGS"]="By HIV Status: UNKNOWN"
        newRow["VALUE"]=str(byHIVStatusUnknown.get(key))
        newRow["SEX"]="ALL"
        newRow["AGE"]="ALL"
        insertNewData(newRow)

        newRow["COPCC"]=str(data[1])
        newRow["tYear"]=str(data[2])
        newRow["INDICATOR"]=getIndicatorString(str(data[3]))
        newRow["SERVICE"]=getServiceString(str(data[3]))
        newRow["DISAGGS"]="AGE/Unknown"
        newRow["LABEL"]=VMMC_CIRCLabelLookUp.get(key,"")
        newRow["AGE"]="ALL"
        newRow["SEX"]="ALL"
        newRow["VALUE"]=str(VMMC_CIRCUnknown.get(key,0))
        insertNewData(newRow)
#*************************END VMMC_CIRC Operations*************************************************************************************************************************************

#*************************START GEND_ Operations*************************************************************************************************************************************
def aggregateGEND(targetYear,country,indicator,label,valueData,mechid,GENDLabelLookUp,GENDAnchorNumber,GENDAggTotal,allAges,sexTotal,bySex,serviceTotal,byService,byPEP,byPEPAnchor,PEPval):
    key=mechid+":"+country+":"+targetYear+":"+indicator
    val=0
    if(label.startswith("Number of")):
        GENDLabelLookUp[key]=label
        GENDAnchorNumber[key]=valueData
    if(label.startswith("Age")):
        val=GENDAggTotal.get(key,0)
        allList=allAges.get(key,[])
        allList.append(str(label)+"||"+str(valueData))
        allAges[key]=allList
        GENDAggTotal[key]=val+valueData
    if(label.startswith("Sex:")):
        val=sexTotal.get(key,0)
        allSexList=bySex.get(key,[])
        allSexList.append(str(label)+"||"+str(valueData))
        bySex[key]=allSexList
    if(label.startswith("By type of service")):
        val=serviceTotal.get(key,0)
        allService=byService.get(key,[])
        allService.append(str(label)+"||"+str(valueData))
        byService[key]=allService
    if(label.startswith("By PEP service provision (related to sexual violence services provided)")):
        val=byPEP.get(key,0)
        PEPval[key]=val
        allPEP=byPEP.get(key,[])
        allPEP.append(str(label)+"||"+str(valueData))
        byPEP[key]=allPEP
    if(label.startswith("By type of service: Sexual Violence (Post-Rape Care)")):
        byPEPAnchor[key]=valueData

def processGEND(cursor,targetYear,country,copYear):  #indicators like GEND_GBV
    GENDLabelLookUp={}
    GENDAnchorNumber={}
    GENDAggTotal={}
    allAges={}
    AgeUnknown={}
    sexTotal={}
    bySex={}
    sexUnknown={}
    serviceTotal={}
    byService={}
    serviceUnknown={}
    byPEP={}
    byPEPAnchor={}
    PEPval={}
    PEPUnknown={}
    dataRows=fetchRowsByIndicator(cursor,targetYear,country,copYear,"GEND_")
    for row in dataRows:
        aggregateGEND(targetYear,country,str(row['INDICATOR']),str(row['LABEL']),row['VALUE'],str(row['MECHID']),GENDLabelLookUp,GENDAnchorNumber,GENDAggTotal,allAges,sexTotal,bySex,serviceTotal,byService,byPEP,byPEPAnchor,PEPval)
        
    for key in GENDAnchorNumber:
        AgeUnknown[key]=GENDAnchorNumber[key]-GENDAggTotal.get(key,0)   #Unkwnon age disaggregates for GEND_ like indicators
        if(int(sexTotal.get(key,0))>0):
            sexUnknown[key]=GENDAnchorNumber[key]-sexTotal.get(key,0)
        if(int(serviceTotal.get(key,0))>0):
            serviceUnknown[key]=GENDAnchorNumber[key]-sexTotal.get(key,0)
        if(int(PEPval.get(key,0))>0):
            PEPUnknown[key]=byPEPAnchor[key]-PEPval[key]
    
    for key in GENDAnchorNumber:
        data=key.split(":")
        mechIDData=str(data[0])
        newRow={}
        newRow["MECHID"]=mechIDData
        newRow["COPCC"]=str(data[1])
        newRow["tYear"]=str(data[2])
        newRow["INDICATOR"]=getIndicatorString(str(data[3]))
        newRow["SERVICE"]=getServiceString(str(data[3]))
        newRow["LABEL"]=GENDLabelLookUp.get(key,"")
        allList=allAges.get(key,[])
        allSex=bySex.get(key,[])
        allService=byService.get(key,[])
        PEP=byPEP.get(key,[])
        for allKey in allList:
            newRow["DISAGGS"]=""
            newRow["VALUE"]=allKey.split("||")[1]    ##(allkey="Age: 0-9 || 1234 where "Age: 0-9" is label and "1234" is its corresponding value)
            newRow["AGE"]=allKey.split("||")[0].split(":")[1]
            newRow["SEX"]="ALL"
            insertNewData(newRow)
        for allKey in allSex:
            newRow["DISAGGS"]=""
            newRow["VALUE"]=allKey.split("||")[1]    ##(allkey="Sex: Female|| 1234 where "Sex: Female" is label and "1234" is its corresponding value)
            newRow["AGE"]="ALL"
            newRow["SEX"]=allKey.split("||")[0].split(":")[1]
            insertNewData(newRow)
        for allKey in allService:
            newRow["DISAGGS"]=allKey.split("||")[0]
            newRow["VALUE"]=allKey.split("||")[1]    ##(allkey="By type of service: Sexual Violence (Post-Rape Care) || 1234 where "By type of service: Sexual Violence (Post-Rape Care)" is label and "1234" is its corresponding value)
            newRow["AGE"]="ALL"
            newRow["SEX"]="ALL"
            insertNewData(newRow)
        for allKey in PEP:
            newRow["DISAGGS"]=allKey.split("||")[0]
            newRow["VALUE"]=allKey.split("||")[1]    ##(allkey="By PEP service provision (related to sexual violence services provided) || 1234 where "By PEP service provision (related to sexual violence services provided)" is label and "1234" is its corresponding value)
            newRow["AGE"]="ALL"
            newRow["SEX"]="ALL"
            insertNewData(newRow)

        newRow["DISAGGS"]="Age/Unknown"
        newRow["LABEL"]=GENDLabelLookUp.get(key,"")
        newRow["AGE"]="ALL"
        newRow["SEX"]="ALL"
        newRow["VALUE"]=str(AgeUnknown.get(key,0))
        insertNewData(newRow)

        newRow["DISAGGS"]="SEX/Unknown"
        newRow["LABEL"]=GENDLabelLookUp.get(key,"")
        newRow["AGE"]="ALL"
        newRow["SEX"]="ALL"
        newRow["VALUE"]=str(sexUnknown.get(key,0))
        insertNewData(newRow)
        
        newRow["DISAGGS"]="SERVICE Unknown"
        newRow["LABEL"]=GENDLabelLookUp.get(key,"")
        newRow["AGE"]="ALL"
        newRow["SEX"]="ALL"
        newRow["VALUE"]=str(serviceUnknown.get(key,0))
        insertNewData(newRow)

        newRow["DISAGGS"]="PEP Unknown"
        newRow["LABEL"]="By type of service: Sexual Violence (Post-Rape Care)"
        newRow["AGE"]="ALL"
        newRow["SEX"]="ALL"
        newRow["VALUE"]=str(serviceUnknown.get(key,0))
        insertNewData(newRow)

#*************************END GEND_ Operations*************************************************************************************************************************************

def main():
    global countries
    global targetYears
    global LabelLookUp
    global AggTotal
    global AnchorNumber
    global db
    cursor=db.cursor(pymysql.cursors.DictCursor)
    #cursor=db.cursor(MySQLdb.cursors.DictCursor)
    copYear='2014' # Cop Year to process
    #Fetch distinct countries to process
    cursor.execute("Select distinct COPCC from COPMechTargets")
    countryRows=cursor.fetchall()
    #count=0
    for countryRow in countryRows:
        temp=str(countryRow['COPCC'])
        temp=temp.replace("'","")
        countries.append(temp)

    for tgYear in targetYears:
        for country in countries:
            processDataMechTargets(cursor,tgYear,country,copYear)

    db.commit()
    db.close()
    return 'Complete!'

def processDataMechTargets(cursor,targetYear,country,copYear):
    processHTC(cursor,targetYear,country,copYear)#HTC
    processHTC(cursor,targetYear,country,copYear)#Testing
    processBSCOLL(cursor,targetYear,country,copYear)#BS_COLL
    processAC(cursor,targetYear,country,copYear) #AC_ indicators
    processC21D(cursor,targetYear,country,copYear)
    processOVC(cursor,targetYear,country,copYear)
    processCareNew(cursor,targetYear,country,copYear)#CARE_NEW
    processCareCurr(cursor,targetYear,country,copYear)
    processC24(cursor,targetYear,country,copYear)
    processC25(cursor,targetYear,country,copYear)
    processFNTHER(cursor,targetYear,country,copYear)#indicators like FN_THER
    processTBART(cursor,targetYear,country,copYear)
    processTBSCREEN(cursor,targetYear,country,copYear)
    processTXCURR(cursor,targetYear,country,copYear)
    processTX_DIST(cursor,targetYear,country,copYear)
    processTX_DIST(cursor,targetYear,country,copYear)
    processTX_NEW_DSD(cursor,targetYear,country,copYear)
    processTX_NEW_DSD(cursor,targetYear,country,copYear)
    processTX_RET_DSD(cursor,targetYear,country,copYear)
    processTX_RET_DSD(cursor,'2014','Nigeria',copYear)
    processPP_PREV(cursor,targetYear,country,copYear)
    processVMMC_CIRC(cursor,targetYear,country,copYear)
    processGEND(cursor,targetYear,country,copYear)
if __name__ == '__main__':
    status = main()
    sys.exit(status)


