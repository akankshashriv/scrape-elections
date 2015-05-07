import csv, datetime, MySQLdb

URL_TEMPLATE = """http://eciresults.ap.nic.in/ConstituencywiseSTATE_CDCONST_CD.htm?ac=CONST_CD"""

urlf = csv.writer(open("urls.csv", 'wr'), dialect='excel', lineterminator='\n')
readcodes = csv.reader(open("codes.csv", "rb"))
readcodes.next()

SQL_DB = 'resultdayanalysis'
SQL_HOST = 'localhost'
SQL_USER = 'root'
SQL_PASSWD = 'setsquare09'

try:
    CONN = MySQLdb.connect(host=SQL_HOST, user=SQL_USER, passwd=SQL_PASSWD, db=SQL_DB)
    cur = CONN.cursor()
    CONN.set_character_set('utf8')
    cur.execute('SET NAMES utf8;') 
    cur.execute('SET CHARACTER SET utf8;')
    cur.execute('SET character_set_connection=utf8;')
    
except MySQLdb.Error, e:
    print "Error %d: %s" % (e.args[0], e.args[1])
    sys.exit(1)

sqlcomm = "SELECT A.constituency_code, B.state_code, A.id from constituency A inner join state B on A.state_id=B.id;"
cur.execute(sqlcomm)
results = cur.fetchall()

for row in results:
    print row
    urlf.writerow([URL_TEMPLATE.replace('STATE_CD',str(row[1])).replace('CONST_CD',str(row[0])), row[2], row[1], row[0], datetime.datetime.now()])
