# importing all library which needed here

from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import re
from sqlalchemy import create_engine
import traceback
import time


# global list for different-different use case
listOfCompanyName = []
tableName=['Quarterly','Profit_loss','unwantedTable2','unwantedTable3','unwantedTable4','unwantedTable5','Balance_Sheet',
           'Cash_Flows','Ratios','Shareholding_Pattern','unwanted10','unwanted11']
listOfData = []
headerList = []
distinctBetweenCompanyCategory = []
isinCodeWhichIsCommingFromFile=[]
CompanyNameWhichWillHelpUsToMakeUrl=[]
isinCodeForTable=[]


# This function is responsible to routing the finance data along with different-2 table name

def financeTableOnConditions(financeDataFrame):
    ingestDataToPostGres_DB(financeDataFrame, tableName[tableCount]+"_Finance")


# This methode is responsible to push data into Postgres-db
def ingestDataToPostGres_DB(dataTodb, tableName):
     try:
            # print('final data',dataTodb)
            #engine = create_engine('postgresql://gritcapital:grit1234@findb-1.c6v9409usa68.us-east-2.rds.amazonaws.com/postgres')

            #postgresql: // postgres: database @ localhost:5432 / postgres
            engine = create_engine('postgresql://postgres:database@localhost:5432/postgres')
            for i in range(1, dataTodb.shape[0]):
                listOfCompanyName.insert(i, listOfCompanyName[0])
                isinCodeForTable.insert(i,isinCodeForTable[0])
            print("this is a shape of data which", dataTodb.shape[0])
            print("This is a Year list: " , len(headerList[0][1:]))
            print("Count  of CompanyList", len(listOfCompanyName))
            print("Count  of isinCodeForTable", len(isinCodeForTable))
            # print("Tbale rows ",listOfCompanyName)
            # listOfCompanyName.remove(0)
            dataTodb['CompanyName'] = listOfCompanyName
            dataTodb['isin']=isinCodeForTable
            print(headerList[0][1:])
            dataTodb['year']=headerList[0][1:]

            #listOfCompanyName.clear()
            dataTodb.to_sql(tableName, engine, if_exists='append',index=False)
            print("Inserted data successfully [to-Postgres_DB]")

     except:
         print("issue with this table solving it Table:"+tableName)
         try:
             FullTableAsDataset = pd.read_sql_query("SELECT * FROM \""+tableName+"\"", con=engine)
             finalDataframeAftreAlter = FullTableAsDataset.append(dataTodb)
            # engine = create_engine('postgresql://postgres:database@localhost:5432/postgres')
             finalDataframeAftreAlter.to_sql(tableName, engine, if_exists='replace',index=False)
             print("Inserted data successfully [to-Postgres_DB]")
             print('====================================================================================')
         except:
           print("Erorr: from alterTable")
           traceback.print_exc()






def sendDataFrameToImportToDb(mainDf):
    mainDf = mainDf.set_index(0)
    # newHeader = mainDf.index.tolist()
    finalDf = mainDf.T
    #print(finalDf)


    #headerList.clear()
    #  print(finalDf)
    if 'Revenue' in finalDf or distinctBetweenCompanyCategory[0] == 'GoToDb':
        # print('yes it is there')
        distinctBetweenCompanyCategory.insert(0, 'GoToDb')
        #headerList.clear()
        print("This is finance company so i am routing this data to another tables")
        financeTableOnConditions(finalDf)

    else:
        ingestDataToPostGres_DB(finalDf, tableName[tableCount])
        headerList.clear()


def getListOfDataInaTabulerform(row):
    cells = row.find_all('td')
    str_cells = str(cells)
    cleantext = BeautifulSoup(str_cells, "lxml").get_text()
    if (cleantext == '[]'):
        pass
    else:
        finalData = re.sub('[^A-Za-z0-9-. ]+', '',
            cleantext.replace('+', '')
                .replace('[', '').replace(']', '').replace('Notes', '').replace('Income', '')
                .replace('Profit', '').replace('in Rs', '')
                .replace('Payout', '')
                .replace('Capital', '').replace('Capital', '').replace('Liabilities', '')
                .replace('Assets', '').replace('before tax', 'before_tax').replace('Cash from', '').
                replace('Activity', '').replace('Cash Flow', '').replace('Debtor', '')
                .replace('Margin', '').replace('NPA', '')
                .replace('Turnover', '')).split(' ')
        while ("" in finalData):
            finalData.remove("")
        listOfData.append(finalData)


def getDateTimeOfThisTable(gettingRawRowsHere):
    for row in gettingRawRowsHere:
        try:
            cells = row.find_all('th')
            str_cells = str(cells)
            header = BeautifulSoup(str_cells, "lxml").get_text()
            # print(header)
            if header == '[]':
                header = re.sub('[^A-Za-z0-9., ]+', ' ', header.replace(',', '').replace('[', '').replace(']', ''))
                flage = True
                pass
            else:
                flage = False
                # header things need to enable with some logic do not remove from here.
                #print('================================header==============================')
                header1 = re.sub('[^A-Za-z0-9., ]+', ' ',
                    header.replace('[', '').replace(']', '')).lstrip().rstrip().split(
                    ',')
                headerList.append(header1)
                # print(headerList)
        except:
            pass


class WebData:

    def InitiWebBaseData(self):
        try:
            url="https://www.screener.in/company/"+CompanyNameWhichWillHelpUsToMakeUrl[diffrenetCompanyCount]+"/consolidated/"
            print("Search in excel: "+CompanyNameWhichWillHelpUsToMakeUrl[diffrenetCompanyCount]," ISIN: "
                ,isinCodeWhichIsCommingFromFile[diffrenetCompanyCount],"\n")
            html = urlopen(url)
            soup = BeautifulSoup(html, 'lxml')
            type(soup)
            title = soup.title
            # print("Company name: ", title)
            text = soup.get_text()

            rowsOfSpecifictData = soup.find_all('table')[tableCount]
            listOfCompanyName.insert(0, soup.find("h1").get_text())
            isinCodeForTable.insert(0, isinCodeWhichIsCommingFromFile[diffrenetCompanyCount])
            # print("company name",soup.find("h1").get_text())
            getDateTimeOfThisTable(rowsOfSpecifictData)
            nameList = rowsOfSpecifictData.findAll("tr")
            for rows in nameList:
                getListOfDataInaTabulerform(rows)
            list2 = filter(None, listOfData)
            df = pd.DataFrame(list2)
            # print(df)
            listOfData.clear()
            sendDataFrameToImportToDb(df)
        except:
            traceback.print_exc()
            print("ERROR: Check your code")


# Start point of this program
print("\nApplication has started:-")
RawData=pd.read_csv("C:/Users/mglocadmin/Desktop/ProjectReletedData/EQUITY_L.csv")
isinCodeWhichIsCommingFromFile=RawData[' ISIN NUMBER'].tolist().copy()
CompanyNameWhichWillHelpUsToMakeUrl=RawData['SYMBOL'].tolist().copy()
refVar = WebData()
for diffrenetCompanyCount in range(0,len(isinCodeWhichIsCommingFromFile)):
 try:
    time.sleep(2)
    distinctBetweenCompanyCategory.insert(0, 'Don not go')
    for tableCount in range(0, 10):
       # print('\n Waiting...')

        if (tableCount == 2) or (tableCount == 3) or (tableCount == 4) or (tableCount == 5):
            print(tableCount, "passed this table number")
        else:
            listOfCompanyName.clear()
            headerList.clear()
            isinCodeForTable.clear()
            refVar.InitiWebBaseData()



 except BaseException as e:
    traceback.print_exc()
