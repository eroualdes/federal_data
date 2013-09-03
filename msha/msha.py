#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function
from os.path import join, basename, isfile, splitext, abspath
from os import makedirs, listdir
from struct import Struct
from sys import exit
from pymongo import MongoClient
from urllib2 import urlopen, URLError, HTTPError
import csv
import re
from aux.auxiliary import states, ae_headers
from aux.utilities import *

class MSHA:
    """A class to handle downloading, reading, and writing MSHA data."""

    types = {'c': 'coal', 'o': 'other', 'cc': 'coal_contractor', 
             'oc': 'other_contractor', 'ci': 'coal_injury', 
             'oi': 'other_injury', 'cic': 'coal_injury_contractor', 
             'oic': 'other_injury_contractor', 'cn': 'coal_narrative', 
             'on': 'other_narrative', 'ccn': 'coal_contractor_narrative',
             'ocn': 'other_contractor_narrative', 'm': 'master_index'}
    website = 'http://www.msha.gov/STATS/PART50/P50Y2K/AETABLE.HTM'
    baseURL = 'http://www.msha.gov/STATS/PART50/P50Y2K/A&I/'

    def __init__(self, type, years, download_directory):
        if type not in self.types.values():
            print('Don\'t understand type %s' % type)
        if max(years) > 2013 or min(years) < 1983:
            print('Years out of range; check website %s for all years available.' % self.website)

        self.dir = abspath(download_directory)
        self.possible_subunits = None
        self.url = None
        if type == self.types['c']:
            self.s = '%d/cade%d.exe'
        if type == self.types['o']:
            self.s = '%d/made%d.exe'
        if type == self.types['cc']:
            self.s = '%d/ctad%d.exe'
        if type == self.types['oc']:
            self.s = '%d/mtad%d.exe'
        if type == self.types['ci']:
            self.s = '%d/caim%d.exe'
        if type == self.types['oi']:
            self.s = '%d/maim%d.exe'
        if type == self.types['cic']:
            self.s = '%d/ccti%d.exe'
        if type == self.types['oic']:
            self.s = '%d/mcti%d.exe'
        if type == self.types['cn']:
            self.s = '%d/canm%d.exe'
        if type == self.types['on']:
            self.s = '%d/manm%d.exe'
        if type == self.types['ccn']:
            self.s = '%d/cctn%d.exe'
        if type == self.types['ocn']:
            self.s = '%d/mctn%d.exe'
        if type == self.types['m']:
            self.url = ['http://www.msha.gov/STATS/PART50/P50Y2K/MIF/mif.exe']

        if not self.url:
            self.url  = [self.baseURL + self.s % (y, y) for y in years]

        self.row1_keys = ['constant', 'type_file', 'year_file', 'cycle_number', 'update_date', 'filler'] # 6

        if type in [self.types[x] for x in ['c', 'o', 'cc', 'oc']]:
            self.subunit_keys = ['subunit_%d_code', 'document_number_q%d', 'number_employees_q%d', 'employee_hours_q%d', 'tons_production_q%d', 'document_number_q%d', 'number_employees_q%d', 'employee_hours_q%d', 'tons_production_q%d', 'document_number_q%d', 'number_employees_q%d', 'employee_hours_q%d', 'tons_production_q%d', 'document_number_q%d', 'number_employees_q%d', 'employee_hours_q%d', 'tons_production_q%d'] # 17
            self.row_keys = ['mine_id', 'contractor', 'filler01', 'inspection_office', 'state_code', 'county_code', 'sic', 'filler02', 'canvass', 'mine_type', 'status_code', 'status_date', 'seam_height', 'filler03', 'prior_status_code', 'travel_area', 'mailing_control', 'company_name', 'filler04', 'mine_name', 'filler05', 'address', 'filler06', 'city', 'filler07', 'state_abbreviation', 'zip_code', 'filler08', 'county_name', 'injury_flag_q1', 'injury_count_q1', 'injury_flag_q2', 'injury_count_q2', 'injury_flag_q3', 'injury_count_q3', 'injury_flag_q4', 'injury_count_q4', 'work_group', 'update_addition_year', 'update_addition_number', 'update_change_year', 'update_change_number', 'number_subunits'] # 43

        if type in [self.types[x] for x in ['c', 'o']]:
            self.fieldwidths1 = (7, 7, 14, 4, 3, 8, 833) 
            self.fieldwidths = (7, 7, 3, 4, 2, 3, 5, 1, 1, 2, 1, 8, 4, 2, 1, 3, 1, 30, 18, 30, 18, 30, 12, 13, 9, 2, 5, 4, 24, 1, 3, 1, 3, 1, 3, 1, 3, 2, 4, 3, 4, 3, 1, 2, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 2, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 2, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 2, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 25)
            self.row1_keys = ['constant'] + self.row1_keys
            self.possible_subunits = 4

        if type in [self.types[x] for x in ['cc', 'oc']]:
            self.fieldwidths1 = (14, 14, 4, 3, 8, 1543)
            self.fieldwidths = (7, 7, 3, 4, 2, 3, 5, 1, 1, 2, 1, 8, 4, 2, 1, 3, 1, 30, 18, 30, 18, 30, 12, 13, 9, 2, 5, 4, 24, 1, 3, 1, 3, 1, 3, 1, 3, 2, 4, 3, 4, 3, 1, 2, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8,10, 2, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 2, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 2, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 2, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 2, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 2, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 2, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 2, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 5, 8, 10, 25)
            self.possible_subunits = 9

        if type in [self.types[x] for x in ['ci', 'oi', 'cic', 'oic']]:
            self.fieldwidths1 = (28, 14, 4, 3, 8, 198)
            self.fieldwidths = (7, 7, 2, 2, 2, 4, 5, 4, 2, 3, 5, 1, 1, 2, 2, 3, 1, 2, 4, 11, 14, 4, 2, 2, 3, 12, 20, 1, 8, 2, 4, 4, 4, 3, 5, 3, 3, 3, 3, 2, 4, 4, 4, 1, 8, 12, 2, 8, 4, 3, 4, 3, 26)
            self.row_keys = ['mine_id', 'contractor', 'subunit', 'month_accident', 'day_accident', 'time_accident', 'filler01', 'inspection_office', 'state_code', 'county_code', 'sic', 'filler02', 'canvass', 'underground_location', 'underground_mining_method', 'trade_name_equipment', 'filler03', 'mining_machine', 'filler04', 'equipment_model_number', 'filler05', 'shift_time', 'accident', 'accident_type', 'injuries_reported', 'document_number', 'filler06', 'sex', 'filler07', 'age', 'total_mine_experience', 'total_experience_this_mine', 'regular_job_experience', 'regular_job_title', 'filler08', 'mine_worker_activity', 'source_injury', 'nature_injury', 'part_body', 'degree_injury', 'days_away_work', 'restricted_work_activity', 'days_lost_work', 'permanently_transferred', 'date_returned_work', 'close_case_injury_document_number', 'msha_accident_code', 'date_investigation_started', 'update_addition_year', 'update_addition_number', 'update_change_year', 'update_change_number', 'filler09']

        if type in [self.types[x] for x in ['cc', 'oc', 'cic', 'oic']]:
            self.a = self.row_keys.index('mine_id')
            self.b = self.row_keys.index('contractor')
            self.row_keys[self.a], self.row_keys[self.b] = self.row_keys[self.b], self.row_keys[self.a]

        if type in [self.types[x] for x in ['cn', 'on', 'ccn', 'ocn']]:
            self.row1_keys.insert(1, 'filler0')
            self.fieldwidths1 = (17, 1, 14, 4, 3, 8, 355)
            self.fieldwidths = (12, 1, 1, 3, 1) + (48,)*8
            self.row_keys = ['document_number', 'type_indicator', 'completion_code', 'narrative_character_count', 'number_narrative_descriptions'] + ['narrative_description_%d' % i for i in range(1, 9)]
        
        if type in [self.types[x] for x in ['m']]:
            self.row1_keys.remove('year_file')
            self.fieldwidths1 = (7, 3, 4, 8, 154)
            self.fieldwidths = (7, 25, 23, 20, 28, 2, 3, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1, 5, 1, 1, 2, 1, 8, 6, 1, 7, 1, 1, 1, 3)
            self.row_keys = ['mine_id', 'company_name', 'filler01', 'entity_name', 'filler02', 'state_code', 'county_code', 'primary_sic', 'filler03', 'secondary_sic1', 'filler04', 'secondary_sic2', 'filler05', 'secondary_sic3', 'filler06', 'secondary_sic4', 'filler07', 'secondary_sic5', 'filler08', 'operation_class', 'filler09', 'status_code', 'status_date', 'latitude', 'filler10', 'longitude', 'filler11', 'number_shops', 'number_plants', 'number_pits']
            

    def genKeys(self):
        keys = self.row_keys
        if self.possible_subunits:
            for u in range(1, self.possible_subunits+1):
                y = (u,)
                k = map(lambda z: 'su%d_' % u + z, self.subunit_keys)
                for q in range(1, 5): # quarters within subunits
                    y += (q,)*4
                keys += [i % j for i, j in zip(k, y)]
        return(keys)

    def download(self, overwrite = False):
        files = []
        mkdir(self.dir, overwrite = overwrite)
        for url in self.url:
            file_name = url.split('/')[-1]
            F = join(self.dir, file_name)
            files.append(F)
            try:
                u = urlopen(url)
                with open(F, 'wb') as f:
                    meta = u.info()
                    file_size = int(meta.getheaders("Content-Length")[0])
                    print("Downloading: %s Bytes: %s" % (file_name, file_size))

                    file_size_dl = 0
                    block_sz = 8192
                    while True:
                        buffer = u.read(block_sz)
                        if not buffer:
                            break

                        file_size_dl += len(buffer)
                        f.write(buffer)
                        status = r"%10d [%3.2f%%]" % (file_size_dl, file_size_dl * 100./file_size)
                        status = status + chr(8)*(len(status)+1)
                        print(status,end='')

            #handle errors
            except HTTPError, e:
                print('HTTP Error:', e.code, url)
            except URLError, e:
                print('URL Error:', e.reason, url)
        return(files)


    def read(self, f):
        with open(f) as F:
            # get first line information
            line_length = sum(self.fieldwidths1)
            fmtstring = ''.join('%ds' % x for x in self.fieldwidths1)
            parse = Struct(fmtstring).unpack_from        
            p = [x.strip() for x in parse(F.readline().ljust(line_length))]
            line_one = dict(zip(self.row1_keys, p))

            # get rest of the data
            line_length = sum(self.fieldwidths)
            fmtstring = ''.join('%ds' % x for x in self.fieldwidths)
            parse = Struct(fmtstring).unpack_from
            for line in F:
                p = [x.strip() for x in parse(line.ljust(line_length))]
                d = dict(zip(self.genKeys(), p))
                yield line_one, d


    def write(self, write_file):
        with open(join(self.dir, write_file), 'w') as csvfile:
            fields = ['year_file', 'update_date'] + self.genKeys()
            w = csv.DictWriter(csvfile, fields)
            w.writeheader()
            for f in listdir(self.dir):
                F = join(self.dir, f)
                if isfile(F) and 'exe' not in F and 'csv' not in F and not f.startswith('.'):
                    r = re.match(r"(\w{3})(\d{4})", f)
                    if r:
                        year_file = r.group(2)
                    for row in self.read(F):
                        if 'year_file' in row[0]:
                            row[1].update({'year_file': row[0]['year_file'],
                                           'update_date': row[0]['update_date']})
                        else:

                            row[1].update({'year_file': year_file, 
                                           'update_date': row[0]['update_date']})
                        w.writerow(row[1])
        return


if __name__ == '__main__':

    # Notes
    # since I don't know how to make python extract the self-extracting files
    # first download files calling MSHA.download() as below;  exit script
    # go extract files yourself
    # call script again to read / write files

    # for the master index, be sure to extract files into the directory
    # specified when initializing the class instance

    # to see what types can initialize class MSHA
    # print(MSHA.types.values())

    # example
    base = '/Users/easy-e/Downloads/msha'
    y = range(1983, 2014)
    t = MSHA.types.values()
    d = [join(base, x) for x in t]

    for i in range(0, len(t)):
        c = MSHA(t[i], y, d[i])
        # c.download()
        c.write(t[i] + '.csv')

