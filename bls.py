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
import os
import csv
import json
from agency import Agency
import re
# from struct import Struct
# from sys import exit
# from auxiliary import states, ae_headers
# from utilities import *

class BLS(Agency):
    """A class to handle downloading, reading and writing BLS (cew) data"""

    def __init__(self, folder):
        Agency.__init__(self, 'BLS', 'http://www.bls.gov/', folder)
        self.meta_file = os.path.join(self.folder, 'meta.json')


    def check_meta(self):
        self.url_regex = r'/cew/data/files/([0-9]{4})/csv/([0-9]{4})_'
        self.url_regex_industry = self.url_regex + r'qtrly_by_industry.zip'
        self.url_regex_naics10 = self.url_regex + r'qtrly_naics10_totals.zip'
        self.regex = re.compile(self.url_regex_industry)
        self.regex2 = re.compile(self.url_regex_naics10)
        for link in self.get_website_content(u"cew/datatoc.htm"):
                yr = self.regex.match(link['href'])
                yr2 = self.regex2.match(link['href'])
                if yr:
                    yield yr
                elif yr2:
                    yield yr2

    def create_meta(self):
        """create meta file """
        mBLS = {'years': {'server': [], 'local': []}}
        if os.path.exists(self.meta_file) == False:
            pass
            # create file
        # update meta

    def update_meta(self):
        """update meta file to all available data online"""
        # read file and get local years

        # read website and get server years
        bls_years = [r.group(1) for r in self.check_meta()]

        # union bls_years, mBLS['years']['remote']
        # update file

    def download(self):
        """download Agency's available data"""
        urls = [self.website + r.group(0) for r in self.check_meta()]
        downloader = self.ThreadedDownload(urls, self.folder, 5, 3)
        print('Downloading %s files' % len(urls))
        downloader.run()

    def update(self):
        """find Agency's available data and download that which doesn't exist locally"""
        # check_local()
        # download() what doesn't exist locally
        # csvfile = '/Users/easy-e/Downloads/bls/1997.q1-q4.by_industry/1997.q1-q4 10 (Total, all industries).csv'
        # with open(csvfile, 'rU') as csvf:
        #     data = csv.DictReader(csvf)
        # print(data.fieldnames)

    def check_local(self):
        # might be too silly to be its own function; put in update()?
        """cross check local files against a call to update_meta()"""
        print('checks some local stuff')
        # read self.folder
        # cross check years available; update_meta()
        # output needed data

        
