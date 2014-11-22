# This file is part of federal_data.

# federal_data is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Foobar is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Foobar.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import print_function
import json
import requests
import threading
from Queue import Queue
from bs4 import BeautifulSoup
import sys, os, re
from util import utilities as util

class Agency(object):

    # https://gist.github.com/chandlerprall/1017266
    class ThreadedDownload(object):

        class Downloader(threading.Thread):
            def __init__(self, queue):
                threading.Thread.__init__(self)
                self.queue = queue

            def run(self):
                while self.queue.empty() == False:
                    url = self.queue.get()
                    response = url.download()
                    if response == False and url.url_tried < url.url_tries:
                        self.queue.put(url)
                    self.queue.task_done()

        class URLTarget(object):
            def __init__(self, url, destination, url_tries):
                self.url = url
                self.destination = destination
                self.url_tries = url_tries
                self.url_tried = 0
                self.success = False

            def download(self):
                self.url_tried = self.url_tried + 1

                try:
                    if os.path.exists(self.destination): # file already downloaded
                        self.success = True
                        return self.success

                    remote_file = requests.get(self.url, stream=True)
                    with open(self.destination, 'wb') as f:
                        print("Downloading: %s" % self.url)
                        for chunk in remote_file.iter_content(chunk_size=1024):
                            f.write(chunk)
                            f.flush()
                    self.success = True
                    
                except remote_file.status_code != requests.codes.ok:
                    print("Error: %s", self.url)
                    remote_file.raise_for_status()
                return self.success


        def __init__(self, urls=[], folder='.', thread_count=5, url_tries=3):
            if os.path.exists(folder) == False:
                util.mkdir(folder, overwrite=False)

            self.queue = Queue(0) # Infinite sized queue
            if folder[-1] != os.path.sep:
                folder = folder + os.path.sep
            self.folder = folder
            self.thread_count = thread_count

            # Prepopulate queue with any values we were given
            for url in urls:
                destination = os.path.join(self.folder, url.split('/')[-1])
                self.queue.put(Agency.ThreadedDownload.URLTarget(url, destination, url_tries))

        def addTarget(self, url, url_tries=3):
            destination = os.path.join(self.folder, url.split('/')[-1])
            self.queue.put(Agency.ThreadedDownload.URLTarget(url, destination, url_tries))

        def run(self):
            for i in range(self.thread_count):
                thread = Agency.ThreadedDownload.Downloader(self.queue)
                thread.start()
            if self.queue.qsize() > 0:
                self.queue.join()
 

    def __init__(self, name, website, folder):
        self.name = name
        self.website = website
        self.folder = folder

    def get_website_content(self, urlext = u""):
        """get urls from website"""
        # this can be more memory efficient; keep links, dump rest immediately
        if urlext:
            url = self.website + urlext
        else:
            url = self.website
        html_content = requests.get(url).content
        for link in BeautifulSoup(html_content).find_all('a', href=True):
            yield link
        
    def update(self):
        """update local data relative to new stuff online

Updating local data first runs check_meta() so as to verify what we have, and thus what need be downloaded."""
        pass

    def check_meta(self):
        """check meta data associated with federal agency

Cross check available years, geographies, and links to ensure we have up to date information. check_meta() signals green light to update(), to begin. Should include
        1. yelling at you if url(s) is(are) not specified correctly
        2. warning you if meta_data has changed and download() is necessary
"""
        pass

    def create_meta(self):
        print('centre college')

    def update_meta(self):
        pass

    def transform(self):
        """transform Agency's data into something writable to csv"""
        pass

    def download(self):
        """download Agency's available data"""
        pass

    def write(self):
        """write Agency's downloaded and transformed data to one csv file"""
        pass

    def read(self):
        """read Agency's downloaded data one line of one file at a time"""
        pass

    def __repr__(self):
        return '<%s: %s>' % (self.name, self.url)
