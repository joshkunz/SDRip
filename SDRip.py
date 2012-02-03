import urllib2
from lxml import html
import os
from pprint import pprint
import gzip
from cStringIO import StringIO
import sqlite3
import datetime
import mimetypes
import hashlib
from time import sleep
import sys

#How many times to retry downloads
REPEAT = 5
#Quit after QUIT_THRESHOLD attempts (0 for no checking)
QUIT_THRESHOLD = 5
PAGE_RANGE = (1, 2) # 38 is the max as of Jan, 23, 2012
DOWNLOAD_DIR = "/Users/Joshkunz/Pictures/SimpleDesktop"
#DOWNLOAD_DIR = "/Users/Joshkunz/Desktop/SimpleDesktops"
DESKTOP_URL = "http://simpledesktops.com/browse/%d"
TOP_URL = "http://simpledesktops.com%s"
# valid scheme names: id, name, nameascii, accesstime, author
name_scheme = "{id} - {nameascii}"

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
    while page is None:
        if repeats > REPEAT: break
        try:
            page = urllib2.urlopen(url)
        except (urllib2.URLError, urllib2.HTTPError), e:
            print "Connection error", e, "retrying..."
    return page

def fetch(url):
    "fetch_page handling gzip, encoded files"
    page = repeat(url)
    sleep_time = 60
    while page is None:
        print "Repeats failed, sleeping for %s min(s)"% sleep_time/2
        sleep(sleep_time)
        sleep_time *= 2
    content = page.read()
    if "Content-Encoding" in page.info() and page.info()["Content-Encoding"] == "gzip":
        f_obj = StringIO(content)
        f_obj_gzip = gzip.GzipFile(fileobj=f_obj, mode='r')
        content = f_obj_gzip.read()
    return page, html.document_fromstring(content)
    
def hash(file_obj, block_size=128):
    h = hashlib.sha1()
    file_obj.seek(0,0)
    data = file_obj.read(block_size)
    while data:
        h.update(data)
        data = file_obj.read(block_size)
    file_obj.seek(0,0)
    return h.hexdigest()
    
exists_count = 0
for page_num in range(min(PAGE_RANGE), max(PAGE_RANGE)+1):
    print "Downloading Page %s..."% page_num
    page, pageP = fetch(DESKTOP_URL% page_num)

    detail_links = pageP.xpath("//div[contains(@class, 'edge') and @class!='edge browse-ad']/div/a/@href")
    print len(detail_links), "images found..."
    for link in detail_links:
        detail_page, detail_pageP = fetch(TOP_URL% link)
        detail_container = detail_pageP.xpath("//div/div[@class='edge']/div[@class='desktop']")[0]
        
        down_link = TOP_URL% detail_container.xpath("./h2/a/@href")[0]
        name = detail_container.xpath("./h2/a/text()")[0]
        author = detail_container.xpath("./span/a/text()")[0]
        accessed = datetime.datetime.now().isoformat()
        
        print u"Downloading", name,
        f = urllib2.urlopen(down_link)
        image_file = StringIO(f.read())
        fhash = hash(image_file)
        
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
        
            doctored_name = name_scheme.format(name=name,
                                           nameascii=name.encode('ascii',
                                                                 'replace').replace(os.sep, ""),
                                           author=author,
                                           id=page_id,
                                           accesstime=accessed)
            final_name = doctored_name \
                     +mimetypes.guess_extension(f.info()["Content-Type"])
            of = open(os.path.join(DOWNLOAD_DIR, final_name), 'wb')
            of.write(image_file.read())
            of.close()
        
print "Done"
        
        