# Scrapy settings for ecscraper project
#

import sys
import MySQLdb

BOT_NAME = 'ecscraper'

SPIDER_MODULES = ['ecscraper.spiders']
NEWSPIDER_MODULE = 'ecscraper.spiders'

#change these to handle Politeness
DOWNLOAD_dELAY = 0

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 0
AUTOTHROTTLE_MAX_DELAY = 60.0

#set the following true for debug mode
AUTOTHROTTLE_DEBUG = False


# SQL DATABASE SETTING
SQL_DB = 'resultdayanalysis'
#SQL_TABLE = 'constituency'
SQL_HOST = 'localhost'
SQL_USER = 'root'
SQL_PASSWD = 'setsquare09'
 
# connect to the MySQL server
try:
    CONN = MySQLdb.connect(host=SQL_HOST,
                         user=SQL_USER,
                         passwd=SQL_PASSWD,
                         db=SQL_DB)
except MySQLdb.Error, e:
    print "Error %d: %s" % (e.args[0], e.args[1])
    sys.exit(1)

ITEM_PIPELINES = {
    'ecscraper.pipelines.EcscraperPipeline'
}
