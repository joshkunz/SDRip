from cStringIO import StringIO
from pprint import pprint
from time import sleep
from lxml import html, etree
import requests
import sqlite3
import datetime
import mimetypes
import hashlib
import sys
import os

########## Config Options ###########

# NAME_SCHEME: Filename of saved images
# valid_options: id, name, nameascii, accesstime, author
NAME_SCHEME = "{id} - {nameascii}"

# DOWNLOAD_DIR: Where to store the downloaded files
DOWNLOAD_DIR = "/Users/Joshkunz/Pictures/SimpleDesktop"

# PAGE_RANGE: Pages to download, 38 is the max as of Jan 23, 2012
PAGE_RANGE = (1, 2)

# QUIT_THRESHOLD: Quit after QUIT_THRESHOLD existing images found
# count resets every time a new image is found
QUIT_THRESHOLD = 3

# REPEAT: Number of times to retry downloads
# SLEEP_TIME: Sleep between repeats, in seconds
REPEAT = 5
SLEEP_TIME = 60 

######## end of the config #########

# Root url of the "browse" pages
DESKTOP_URL = "http://simpledesktops.com/browse/%d"

#Top level url for correcting relative url's
TOP_URL = "http://simpledesktops.com%s"

# xpath to find links to the detail pages
PAGE_XPATH = "//div[contains(@class, 'edge') and @class!='edge browse-ad']/div/a/@href"

# xpath to find the container on the detail page
DETAIL_CONTAINER_XPATH = "//div/div[@class='edge']/div[@class='desktop']"

#Setup Tables
db = sqlite3.connect("images.db")
cursor = db.cursor()

cursor.execute("""
               CREATE TABLE IF NOT EXISTS images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name text,
                hash text,
                author text,
                dlurl text,
                accessed text)
               """)

os.chdir(DOWNLOAD_DIR)

def repeat(url):
	repeats = 0
	page = None
	for attempt in range(REPEAT):
		try:
			page = requests.get(url)
			break #page fetched, stop repeating
		except (requests.exceptions.ConnectionError, 
				requests.exceptions.HTTPError), e:
			print "Connection error", e, "retrying..."
	return page

def fetch(url, parse_page=True):
	page = repeat(url)
	sleep_time = 60
	while page is None:
		print "Repeats failed, sleeping for %s min(s)"% SLEEP_TIME/2
		sleep(SLEEP_TIME)
		SLEEP_TIME *= 2
	if parse_page:
		return page, html.document_fromstring(page.text)
	else:
		return page
    
def hash(file_obj, block_size=128):
    h = hashlib.sha1()
    file_obj.seek(0,0)
    data = file_obj.read(block_size)
    while data:
        h.update(data)
        data = file_obj.read(block_size)
    file_obj.seek(0,0)
    return h.hexdigest()

def parse_detail_page(detail_link):
	global exists_count
	detail_page, detail_pageP = fetch(TOP_URL% link)
	detail_container = detail_pageP.xpath(DETAIL_CONTAINER_XPATH)[0]
        
	down_link = TOP_URL% detail_container.xpath("./h2/a/@href")[0]
	name = detail_container.xpath("./h2/a/text()")[0]
	author = detail_container.xpath("./span/a/text()")[0]
	accessed = datetime.datetime.now().isoformat()
	
	print u"Downloading", name,
	f = fetch(down_link, parse_page=False)
	image_file = StringIO(f.content)
	fhash = hash(image_file)
	
	# Check to see if the image is a duplicate
	cursor.execute("SELECT id FROM images WHERE hash=?", (fhash,))
	if cursor.fetchone():
		print u'already exists, skipping...'
		exists_count += 1
		if QUIT_THRESHOLD and exists_count >= QUIT_THRESHOLD:
			print "Quit Threshold reached, exiting..."
			sys.exit(0)
	else:
		exists_count = 0
		print 
		cursor.execute("""
			INSERT INTO images
			(name, author, dlurl, accessed, hash)
			values (?,?,?,?,?)""",
			(name, author, down_link, accessed, fhash))
		db.commit()
	
		page_id = cursor.execute("SELECT last_insert_rowid()").fetchone()[0]
		
		doctored_name = NAME_SCHEME.format(name=name,
			nameascii=name.encode('ascii', 'replace').replace(os.sep, ""),
			author=author, id=page_id, accesstime=accessed)
		
		final_name = doctored_name \
			+mimetypes.guess_extension(f.headers["Content-Type"])
	
		of = open(os.path.join(DOWNLOAD_DIR, final_name), 'wb')
		of.write(image_file.read())
		of.close()
    
exists_count = 0
for page_num in range(min(PAGE_RANGE), max(PAGE_RANGE)+1):
	print "Downloading Page %s..."% page_num
	page, pageP = fetch(DESKTOP_URL% page_num)
	
	detail_links = pageP.xpath(PAGE_XPATH)
	print len(detail_links), "images found..."
	for link in detail_links:
		parse_detail_page(link)

print "Done"
        
        
