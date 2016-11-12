#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import threading
import os
from urllib.request import urlretrieve, urlopen
import xml.etree.ElementTree as ET
import exifread
import sqlite3

WALDO_RECRUTING_URL = 'http://s3.amazonaws.com/waldo-recruiting'


def get_db():
    db = sqlite3.connect("files.db")
    return db


def setup_databse():
    db = get_db()
    sql = ''.join([
        'drop table if exists photos;',
        'create table photos (',
        'id integer primary key autoincrement,',
        'filename text not null,',
        'exif text not null);'])

    db.cursor().executescript(sql)
    db.commit()
    db.close()


def add_exif_to_db(file_name, exif_str):
    db = get_db()
    db.execute('insert into photos (filename, exif) values(?, ?);',
               [file_name, exif_str])
    db.commit()
    db.close()


def get_image_list_from_XML(xml_url):
    print("Getting xml from url")
    raw_file = urlopen(xml_url)
    data = raw_file.read()
    raw_file.close()
    tree = ET.fromstring(data)
    element_list = tree.findall(''.join([
        '{http://s3.amazonaws.com/doc/2006-03-01/}',
        'Contents/{http://s3.amazonaws.com/doc/2006-03-01/}Key']))
    url_list = [xml_url + '/' + e.text for e in element_list]
    return url_list


def process_exif_for_file(file_name):
    f = open(file_name, 'rb')
    tags = exifread.process_file(f)
    exif_data = '\n'.join([
        '%s = %s' % (key, value) for key, value in tags.items()])
    add_exif_to_db(file_name, exif_data)


def download_img(img_url, tries=3):
    print('Downloading: %s' % img_url)
    if not os.path.exists('img_tmp/'):
        os.makedirs('img_tmp/')
    file_name = img_url.split('/')[-1]
    file_name = 'img_tmp/' + file_name

    # handle error
    try:
        urlretrieve(img_url, file_name)
        print('File downloaded: %s' % file_name)
        # read exif
        process_exif_for_file(file_name)
    except Exception as e:
        if tries == 0:
            try:
                os.remove(file_name)
            except OSError:
                pass
            return
        print("Failed to download file: %s, %s retrying" % (img_url, e))
        tries -= 1
        download_img(img_url, tries)


def execute_threads():
    # starting the process
    url_list = get_image_list_from_XML(WALDO_RECRUTING_URL)
    print("Starting download %i files" % len(url_list))

    # request to the S3 and retrieve the photos
    threads = [threading.Thread(target=download_img,
                                args=(url,)) for url in url_list]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    print("done")

if __name__ == '__main__':
    setup_databse()
    execute_threads()
