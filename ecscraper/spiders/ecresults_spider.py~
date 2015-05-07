from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.selector import Selector
from ecscraper.items import ECResults
from scrapy.http import Request
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
import csv, os, sys
import datetime
from bs4 import BeautifulSoup 

class ECSpider(CrawlSpider):
    name = "ec"
    allowed_domains = ["eciresults.ap.nic.in"]
    handle_httpstatus_list = [404, 302, 503]
    ser,i,e, tot = 0,0,0, 0
    time_start = datetime.datetime.now()

    def start_requests(self):
        try: 
            serial = open("crawl_serial.txt", 'r')
            self.ser = int(serial.readline())
            self.ser = self.ser+1
        except:
            self.ser = 0
        
        print "crawl serial", self.ser

        serial = open("crawl_serial.txt",'w')
        serial.write(str(self.ser))
        
        dirn = "Logs//Serial_" + str(self.ser)+"//"
        d = os.path.dirname(dirn)

        try:
            os.makedirs(d)
        except OSError:
            if not os.path.isdir(d):
                self.e=self.e+1
                exc_type, exc_value, exc_traceback = sys.exc_info()
                writer=csv.writer(open("ErrorLog.csv", 'a'), dialect='excel', lineterminator='\n')
                writer.writerow([exc_type, exc_value, exc_traceback])
                raise

        dispatcher.connect(self.spider_quit, signals.spider_closed)

        with open('urls.csv', 'rb') as urlf:
            reader = csv.reader(urlf)
            for row in reader:
               try:
                   request = Request(row[0].strip(), self.parse_const)
                   self.tot = self.tot +1
                   request.meta['constituency_id'] = row[1]
                   request.meta['state_cd'] = row[2]
                   request.meta['constituency_cd'] = row[3]
                   yield request
               except:
                   exc_type, exc_value, exc_traceback = sys.exc_info()
                   writer=csv.writer(open("ErrorLog.csv", 'a'), dialect='excel', lineterminator='\n')
                   writer.writerow([exc_type, exc_value, exc_traceback, row[0]])
                   


    def handle_404(self,response):
        constituency_id = response.meta['constituency_id']
        writer=csv.writer(open("HTMLParseErrorLog.csv", 'a'), dialect='excel', lineterminator='\n')
        writer.writerow(['404', 'Page error handled', '',response.url])



    def parse_const(self,response):
        
        if response.status in self.handle_httpstatus_list or response.url == "http://eciresults.ap.nic.in/error.htm": 
            return self.handle_404(response)

        else:
            url = response.url
            print url
            sel = Selector(response)
            
            constresult = ECResults()
            try:

                constresult['url']=url
                constresult['constituency_cd'] = response.meta['constituency_cd']
                constresult['state_cd'] = response/meta['state_cd']
                constresult['constituency_id'] = response.meta['constituency_id']
                constresult['state']=sel.xpath("//*[@id='div1']/table[1]/tr[1]/td/text()").extract()[0].split("-")[0].strip()
                constresult['constituency']=sel.xpath("//*[@id='div1']/table[1]/tr[1]/td/text()").extract()[0].split("-")[1].strip()
                constresult['updated']=sel.xpath("//*[@id='div1']/table[2]/tr/td[1]/text()").extract()[0]
                declared=sel.xpath("//*[@id='div1']/table[1]/tr[2]/td/text()").extract()[0]
                constresult['timenow']= str(datetime.datetime.now())
                allrows =sel.xpath("//tr[@style='font-size:12px;']").extract()
            
                if 'Declared' in declared: constresult['status'] = 'Declared'
                else: constresult['status'] = 'Counting'
                constresult['allrows'] = []
                
                for each in allrows:
                    soup = BeautifulSoup(each)
                    col = soup.findAll('td')
                    candidate = col[0].get_text().strip()
                    party = col[1].get_text().strip()
                    votes = int(col[2].get_text().strip())
                    constresult['allrows'].append([candidate,party,votes])
                
                
                self.i=self.i+1
                return constresult

            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                writer=csv.writer(open("HTMLParseErrorLog.csv", 'a'), dialect='excel', lineterminator='\n')
                writer.writerow([exc_type, exc_value, exc_traceback,url])
            


    def spider_quit(self, spider):
        print "reaches here"
        time_end = datetime.datetime.now()

        try: 
            errfile = open("errorcount.txt", 'r')
            err = int(errfile.readline())
        except:
            err = 0
            
        try: 
            insfile = open("insertcount.txt", 'r')
            ins = int(insfile.readline())
        except:
            ins = 0
            
        new = 0
        try:
            reader = csv.reader(open("Logs/Insert_candidate.csv", "rb"))
            c = sum(1 for row in reader)
            new = new+c
        except:
            new = new + 0
                                
        try:
            reader = csv.reader(open("Logs/Insert_party.csv", "rb"))
            c = sum(1 for row in reader)
            new = new+c
        except:
            new = new + 0

        writer = csv.writer(open("Logs/CrawlerInstances.csv", 'a'), dialect='excel', lineterminator='\n')
        writer.writerow([self.ser, str(self.time_start), str(time_end-self.time_start),self.i,err+self.e,ins, new, self.tot])
        
        try:
            os.rename("Insert_candidate.csv", "Logs/Serial_"+ str(self.ser) + "/Insert_candidate.csv")
            os.rename("Insert_party.csv", "Logs/Serial_"+ str(self.ser) + "/Insert_party.csv")
            os.rename("ErrorLog.csv", "Logs/Serial_"+ str(self.ser) + "/ErrorLog.csv")
            os.rename("HTMLParseErrorLog.csv", "Logs/Serial_"+ str(self.ser) + "/ErrorLog.csv")            
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.fileerror(exc_type, exc_value, exc_traceback)
            
        try:
            os.remove("errorcount.txt")
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.fileerror(exc_type, exc_value, exc_traceback)
            
        try:
            os.remove("insertcount.txt")
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.fileerror(exc_type, exc_value, exc_traceback)

            
        try:
            os.remove("HTMLParseErrorLog.csv")
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.fileerror(exc_type, exc_value, exc_traceback)

        try:
            os.remove("ErrorLog.csv")
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.fileerror(exc_type, exc_value, exc_traceback)
            
    def fileerror(self, exc_type, exc_value, exc_traceback):
        writer=csv.writer(open("Excpetions_File.csv", 'a'), dialect='excel', lineterminator='\n')
        writer.writerow([self.ser, exc_type, exc_value, exc_traceback, "This file must have not been created by the program."])
