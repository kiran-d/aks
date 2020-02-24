import logging
import azure.functions as func
import pymssql
import json
import pyodbc

#cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};""Server=tcp:otisoftcbmreportserveremea.database.windows.net,1433;""Database=EMEA_OFT_ReportDB;""Uid=CBMOFT_Admin@otisoftcbmreportserveremea;""Pwd=AdUser@8294;""Encrypt=yes;""TrustServerCertificate=no;""Connection Timeout=30;")
cnxn = pyodbc.connect("Server=tcp:otisoftcbmreportserveremea.database.windows.net,1433;""Database=EMEA_OFT_ReportDB;""Uid=CBMOFT_Admin@otisoftcbmreportserveremea;""Pwd=AdUser@8294;""Encrypt=yes;""TrustServerCertificate=no;""Connection Timeout=30;")
#cnxn = pymssql.connect(host='reportmssqlcbmstagenaa6mbuxd6ljklwc.database.windows.net', user='otisadmin', password='0tis7heGr3at!', database='reportdbcbmstagenaa6mbuxd6ljklwc')

def getJsonData(limit):
    
    query_string = "select top "+limit+" *  FROM [dbo].[rp_Daily_Predictions]"
    cur = cnxn.cursor()
    cur = cur.execute(query_string)
    
    row_headers=[x[0] for x in cur.description] #this will extract row headers

    resultData = cur.fetchall()
    json_data=[]
    for result in resultData:
        json_data.append(dict(zip(row_headers,result)))


    return json.dumps(json_data, indent=4, sort_keys=True, default=str)


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    limit = req.params.get('limit')
    if not limit:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            limit = req_body.get('limit') 

    if limit:
        resp = getJsonData(limit)
        return func.HttpResponse(resp)  
    else:
        return func.HttpResponse(
             "Please pass a limit on the query string or in the request body",
             status_code=400
        )

