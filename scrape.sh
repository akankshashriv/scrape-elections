#!/bin/bash
scrapy crawl ec -o scraped_data.csv -t csv

#crontab edit, add: 0 0  * * * sh /media/akanksha/Data/Projects/ecscraper/scrape.sh
