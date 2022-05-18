import cx_Oracle
import win32com.client as win32
import datetime

dsnStr = cx_Oracle.makedsn("e-scan", "1521", service_name="irbis")

con = cx_Oracle.connect(user="rprt", password="rprt", dsn=dsnStr)
cursor = con.cursor()

r = cursor.execute("""SELECT lasttime FROM reports.hadoop_pgw_timedownload""")
data = r.fetchone()[0]
delta = datetime.datetime.now() - data
if str(delta)[0] > '2':
    date_string = data.strftime('%Y-%m-%d %H:%M:%S')
    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.To = 'Aliya.Abulhairova@tattelecom.ru'
    mail.Subject = 'ERROR!!!'
    mail.Body = 'ERROR!!!Downloading files to Hadoop'
    mail.HTMLBody = "ERROR!!! Last upload at " + date_string
    mail.Send()
