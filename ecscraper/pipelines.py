import MySQLdb
import datetime, sys, csv

class EcscraperPipeline(object):


    def __init__(self):
        SQL_DB = 'resultdayanalysis'
        SQL_HOST = 'localhost'
        SQL_USER = 'root'
        SQL_PASSWD = 'setsquare09'
        
        try:
            self.CONN = MySQLdb.connect(host=SQL_HOST, user=SQL_USER, passwd=SQL_PASSWD, db=SQL_DB)
            self.cur = self.CONN.cursor()
            self.CONN.set_character_set('utf8')
            self.cur.execute('SET NAMES utf8;') 
            self.cur.execute('SET CHARACTER SET utf8;')
            self.cur.execute('SET character_set_connection=utf8;')
        
        except MySQLdb.Error, e:
                try:
                    errfile = open("errorcount.txt", 'r')
                    err = int(errfile.readline())
                    err = err +1
                except:
                    err = 1
                errfile = open("errorcount.txt", 'w')
                errfile.write(str(err))
                exc_type, exc_value, exc_traceback = sys.exc_info()
                writer=csv.writer(open("ErrorLog.csv", 'a'), dialect='excel', lineterminator='\n')
                writer.writerow([exc_type, exc_value, exc_traceback])
                sys.exit(1)
    

    def incInser(self):
        try:
            insfile = open("insertcount.txt", 'r')
            ins = int(insfile.readline())
            ins = ins+1
        except:
            ins = 0
        
        insfile = open("insertcount.txt", 'w')
        insfile.write(str(ins))
    

    def incErr(self, type, val, traceback, url):
        try:
            errfile = open("errorcount.txt", 'r')
            err = int(errfile.readline())
            err = err+1
        except:
            err = 0
        errfile = open("errorcount.txt", 'w')
        errfile.write(str(err))
        writer=csv.writer(open("ErrorLog.csv", 'a'), dialect='excel', lineterminator='\n')
        writer.writerow([type, val, traceback, url])
        

    def checkVal(self,value,column,table):
        sqlcomm = "SELECT * from %s where %s = '%s'"% (table, column, value)
        self.cur.execute(sqlcomm)
        results = self.cur.fetchall()
        if len(results)==0:
            return 0
        else:
            id = results[0][0]
            return id

    def checkResults(self,cand_id, party_id, const_id, time, votes, url):
        sqlcomm1 = "SELECT * from results where candidate_id = %s and constituency_id = %s"% (cand_id, const_id)
        self.cur.execute(sqlcomm1)
        results = self.cur.fetchall()
        
        if len(results)!=0:
            self.sqlcomm2 = "UPDATE results SET active = 0 where candidate_id = %s and constituency_id = %s"% \
                       (cand_id, const_id)
            try:
                self.cur.execute(self.sqlcomm2)
                self.CONN.commit()
            except:
                self.CONN.rollback()
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.incErr(exc_type, exc_value, exc_traceback, url)


        sqlcomm3 =  "INSERT INTO results(candidate_id, constituency_id, votes, time_start, active) VALUES(%d,%d,%d,'%s', 1);"% (cand_id, const_id, int(votes), time)
        
        try: 
            self.cur.execute(sqlcomm3)
            self.CONN.commit()
            self.incInser()
        except:
            self.CONN.rollback()
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.incErr(exc_type, exc_value, exc_traceback, url)

        self.cur.execute(sqlcomm1)
        resultsnew = self.cur.fetchall()
        

    def process_item(self, item, spider):
        
        print "In here!"
        
        # GET CONSITUENCY ID FROM STATE_CD AND CONSTITUENCY_CD
        print item['state_cd']
        state_id = self.checkVal(item['state_cd'], 'state_code', 'state')
        sqlcomm = "SELECT * from constituency where constituency_code = '%s' and state_id = %d"% (item['constituency_cd'],state_id)
        self.cur.execute(sqlcomm)
        results = self.cur.fetchall()
        if len(results)==0:
            self.incErr('Constituency id missing', 'Id not found from state and const codes', '', item['url'])
        else:
            constituency_id = results[0][0]

        # UPDATE CONSTITUENCY COUNTING STATUS IN CONSTITUENCY TABLE
        print item['status']
        self.sqlcomm = "UPDATE constituency SET result_status = '%s' where id = %s"% (item['status'], constituency_id)
        try:
            self.cur.execute(sqlcomm)
            self.CONN.commit()
        except:
            self.CONN.rollback()
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.incErr(exc_type, exc_value, exc_traceback, item['url'])

	for each in item['allrows']:
            
            # GET OR MAKE NEW PARTY ID (INSERT PARTY)
            party_id = self.checkVal(each[1],'name','party')
            if party_id == 0:
                sqlcomm = "INSERT INTO party(name, coalition_id) VALUES ('%s', 3);"% (each[1].title())
                try: 
                    self.cur.execute(sqlcomm)
                    self.CONN.commit()
                    self.incInser()
                except:
                    self.CONN.rollback()
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    self.incErr(exc_type, exc_value, exc_traceback, url)

                party_id = self.checkVal(each[1],'name','party')
                writer = csv.writer(open("Logs/Insert_party.csv", 'a'), dialect='excel', lineterminator='\n')
                writer.writerow([party_id, each[1], item['url']])



            # GET OR MAKE CONSTITUENCY ID (INSERT NEW CONSTITUENCY)
            candidate_id = self.checkVal(each[0],'fullname','candidate')
            if candidate_id == 0:
                sqlcomm = "SELECT * from candidate_constituency where constituency_id = '%s' and  party_id= '%s'"% (constituency_id, party_id)
                self.cur.execute(sqlcomm)
                results = self.cur.fetchall()
                if len(results)==0:
                     sqlcomm = "INSERT INTO candidate(fullname) VALUES ('%s');"% (each[0].title())
                     
                     try: 
                         self.cur.execute(sqlcomm)
                         self.CONN.commit()
                         self.incInser()
                         candidate_id = self.checkVal(each[0],'fullname','candidate')
                     except:
                         self.CONN.rollback()
                         exc_type, exc_value, exc_traceback = sys.exc_info()
                         self.incErr(exc_type, exc_value, exc_traceback, item['url'])
                    
                     sqlcomm2 = "INSERT INTO candidate_constituency(candidate_id, constituency_id, party_id) VALUES (%d, %d, %d );" % (candidate_id, constituency_id, party_id)
                     try: 
                         self.cur.execute(sqlcomm)
                         self.CONN.commit()
                     except:
                         self.CONN.rollback()
                         exc_type, exc_value, exc_traceback = sys.exc_info()
                         self.incErr(exc_type, exc_value, exc_traceback, item['url'])
                    
                else:
                    candidate_id = results[0][0]
                    sqlcomm2 = "UPDATE candidate SET fullname = '%s' where id = %s"% (each[0].title(), candidate_id)
                    try:
                        self.cur.execute(sqlcomm2)
                        self.CONN.commit()
                    except:
                        self.CONN.rollback()
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        self.incErr(exc_type, exc_value, exc_traceback, item['url'])

                writer = csv.writer(open("Logs/Insert_candidate.csv", 'a'), dialect='excel', lineterminator='\n')
                writer.writerow([candidate_id, party_id, constituency_id, item['url']])

            self.checkResults(candidate_id, party_id, constituency_id, item['timenow'], each[2], item['url'])
        
        return item
        
