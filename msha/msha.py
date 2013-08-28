#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function
from os.path import join, basename, isfile, splitext
from os import makedirs, listdir
from struct import Struct
from sys import exit
from pymongo import MongoClient
from urllib2 import urlopen, URLError, HTTPError
import csv
from aux.auxiliary import states, ae_headers
from aux.utilities import *

website = 'http://www.msha.gov/STATS/PART50/P50Y2K/AETABLE.HTM'

# only address/employment files so far


class MSHA:
    """A class to handle downloading, reading, and writing MSHA data."""

    def __init__(self, type):
        if type == 'coal':
        elif type == 'other':
        elif type == 'coal_contractor':
        elif type == 'other_contractor':
        else:
            print('Don\'t understand type %s' % type)


def downloadAE(directory, years, coal = True, other = False, 
                      coal_contractors = False,  other_contractors = False,
                      overwrite = False):
    """Download sefl-extracting MSHA data files, for specified years, into folder directory.  

    Args:
        directory (string): full path to folder for which the data files should go
        years (list): a list of years for which the data is downloaded (min 1983, max 2013)
        coal (boolean): True means download coal data; default = True
        other (boolean): True means download metal/nonmetal data; default = False
        coal_contractors (boolean): True means download coal contractors data; default = False
        other_contractors (boolean): True means download metal/nonmetal contractors data; default = False
        overwrite (boolean): True will overwrite everything in directory; default = False

    Returns:
        files (list): paths to the downloaded files
    """

    # check years within appropriate time span
    if max(years) > 2013 or min(years) < 1983:
        exit('Years out of range; check website %s for all years available.' % website)

    files = []
    # url: MSHA coal files
    baseURL = 'http://www.msha.gov/STATS/PART50/P50Y2K/A&I/'
    mshaCOAL = [baseURL + '%d/cade%d.exe' % (y, y) for y in years]
    mshaOTHER = [baseURL + '%d/made%d.exe' % (y, y) for y in years]
    mshaCOAL_CON = [baseURL + '%d/ctad%d.exe' % (y, y) for y in years]
    mshaOTHER_CON = [baseURL + '%d/mtad%d.exe' % (y, y) for y in years]

    downloads = []
    if coal == True:
        downloads += mshaCOAL
    elif other == True:
        downloads += mshaOTHER
    elif coal_contractors == True:
        downloads += mshaCOAL_CON
    elif other_contractors == True:
        downloads += mshaOTHER_CON

    if not downloads:
        exit('You specify something to download.')

    mkdir(directory, overwrite = overwrite)
    for url in downloads:
        file_name = url.split('/')[-1]
        F = join(directory, file_name)
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
                    print(status, end = '')
                
        #handle errors
        except HTTPError, e:
            print('HTTP Error:', e.code, url)
        except URLError, e:
            print('URL Error:', e.reason, url)

    return(files)


def readAE(f):
    """read MSHA address/employment data from their fixed width files and yield each row of the file as a dict object similar to DictReader.

    Files found at the following link: http://www.msha.gov/STATS/PART50/P50Y2K/AETABLE.HTM
    Documentation of said files found here: http://www.msha.gov/STATS/PART50/P50Y2K/P50Y2KHB.PDF

    Args:
        f (string): a path to an ASCII text file

    Returns:
        an iterator of 2-tuples; the first element in the tuple is a DictReader-like object for the first row of the file and the second element holds each row after that
    """

    fieldwidths1 = (7, 7, 14, 4, 3, 8, 833)
    fieldwidths = (7, 7, 3, 4, 2, 3, 5, 1, 1, 2, 1, 8, 4, 2, 1, 3, 1, 30, 18, 30, 18, 30, 12, 13, 9, 2, 5, 4, 24, 1, 3, 1, 3, 1, 3, 1, 3, 2, 4, 3, 4, 3, 1, 2, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 2, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 2, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 2, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 12, 5, 8, 10, 25)

    print('Reading file: %s' % f)
    with open(f) as F:
        # get first line information
        fmtstring = ''.join('%ds' % x for x in fieldwidths1)
        parse = Struct(fmtstring).unpack_from        
        p = [x.strip() for x in parse(F.readline().ljust(876))]

        line_one = {}
        line_one['constant'] = p[0]
        line_one['constant'] = p[1]
        line_one['type_file'] = p[2]         # type of file
        line_one['year_file'] = int(p[3]) # year of file
        line_one['update_cycle'] = p[4]
        line_one['update_date'] = p[5]
        line_one['filler'] = p[6]
        
        # get rest of the data
        fmtstring = ''.join('%ds' % x for x in fieldwidths)
        parse = Struct(fmtstring).unpack_from        
        for line in F:
            d = {}
            p = [x.strip() for x in parse(line.ljust(876))]

            # map each column to a pair (key, value)
            d['mine_id'] = p[0]
            d['contractor'] = p[1]
            d['filler01'] = p[2]
            d['inspection_office'] = p[3]
            d['state_code'] = p[4]
            d['county_code'] = p[5]
            d['sic'] = p[6]
            d['filler02'] = p[7]
            d['canvass'] = p[8] # Canvass or Class
            d['mine_type'] = p[9]
            d['status_code'] = p[10]
            d['status_date'] = p[11]
            d['seam_height'] = num(p[12])
            d['filler03'] = p[13]
            d['prior_status_code'] = p[14]
            d['travel_area'] = p[15]
            d['mailing_control'] = p[16]
            d['company_name'] = p[17]
            d['filler04'] = p[18]
            d['mine_name'] = p[19] # Mine or Plant Name
            d['filler05'] = p[20]
            d['address'] = p[21]     # street or PO Box Number
            d['filler06'] = p[22]
            d['city'] = p[23]
            d['filler07'] = p[24]
            d['state_abbreviation'] = p[25]
            d['zip_code'] = p[26]
            d['filler08'] = p[27]
            d['county_name'] = p[28]
            d['injury_flag_q1'] = p[29]
            d['injury_count_q1'] = p[30]
            d['injury_flag_q2'] = p[31]
            d['injury_count_q2'] = p[32]
            d['injury_flag_q3'] = p[33]
            d['injury_count_q3'] = p[34]
            d['injury_flag_q4'] = p[35]
            d['injury_count_q4'] = p[36]
            d['work_group'] = p[37]
            d['update_addition_year'] = p[38]
            d['update_addition_number'] = p[39]
            d['update_change_year'] = p[40]
            d['update_change_number'] = p[41]
            d['number_subunits'] = p[42] # Number of Subunits
           
            # subunits
            d['subunit_1'] = {}
            d['subunit_1']['subunit_1_code'] = p[43]
            d['subunit_1']['document_number_q1'] = p[44]
            d['subunit_1']['number_employees_q1'] = p[45]
            d['subunit_1']['employee_hours_q1'] = p[46]
            d['subunit_1']['tons_production_q1'] = p[47]
            d['subunit_1']['document_number_q2'] = p[48]
            d['subunit_1']['number_employees_q2'] = p[49]
            d['subunit_1']['employee_hours_q2'] = p[50]
            d['subunit_1']['tons_production_q2'] = p[51]
            d['subunit_1']['document_number_q3'] = p[52]
            d['subunit_1']['number_employees_q3'] = p[53]
            d['subunit_1']['employee_hours_q3'] = p[54]
            d['subunit_1']['tons_production_q3'] = p[55]
            d['subunit_1']['document_number_q4'] = p[56]
            d['subunit_1']['number_employees_q4'] = p[57]
            d['subunit_1']['employee_hours_q4'] = p[58]
            d['subunit_1']['tons_production_q4'] = p[59]

            d['subunit_2'] = {}
            d['subunit_2']['subunit_2_code'] = p[60]
            d['subunit_2']['document_number_q1'] = p[61]
            d['subunit_2']['number_employees_q1'] = p[62]
            d['subunit_2']['employee_hours_q1'] = p[63]
            d['subunit_2']['tons_production_q1'] = p[64]
            d['subunit_2']['document_number_q2'] = p[65]
            d['subunit_2']['number_employees_q2'] = p[66]
            d['subunit_2']['employee_hours_q2'] = p[67]
            d['subunit_2']['tons_production_q2'] = p[68]
            d['subunit_2']['document_number_q3'] = p[69]
            d['subunit_2']['number_employees_q3'] = p[70]
            d['subunit_2']['employee_hours_q3'] = p[71]
            d['subunit_2']['tons_production_q3'] = p[72]
            d['subunit_2']['document_number_q4'] = p[73]
            d['subunit_2']['number_employees_q4'] = p[74]
            d['subunit_2']['employee_hours_q4'] = p[75]
            d['subunit_2']['tons_production_q4'] = p[76]

            d['subunit_3'] = {}
            d['subunit_3']['subunit_3_code'] = p[77]
            d['subunit_3']['document_number_q1'] = p[78]
            d['subunit_3']['number_employees_q1'] = p[79]
            d['subunit_3']['employee_hours_q1'] = p[80]
            d['subunit_3']['tons_production_q1'] = p[81]
            d['subunit_3']['document_number_q2'] = p[82]
            d['subunit_3']['number_employees_q2'] = p[83]
            d['subunit_3']['employee_hours_q2'] = p[84]
            d['subunit_3']['tons_production_q2'] = p[85]
            d['subunit_3']['document_number_q3'] = p[86]
            d['subunit_3']['number_employees_q3'] = p[87]
            d['subunit_3']['employee_hours_q3'] = p[88]
            d['subunit_3']['tons_production_q3'] = p[89]
            d['subunit_3']['document_number_q4'] = p[90]
            d['subunit_3']['number_employees_q4'] = p[91]
            d['subunit_3']['employee_hours_q4'] = p[92]
            d['subunit_3']['tons_production_q4'] = p[93]

            d['subunit_4'] = {}
            d['subunit_4']['subunit_4_code'] = p[94]
            d['subunit_4']['document_number_q1'] = p[95]
            d['subunit_4']['number_employees_q1'] = p[96]
            d['subunit_4']['employee_hours_q1'] = p[97]
            d['subunit_4']['tons_production_q1'] = p[98]
            d['subunit_4']['document_number_q2'] = p[99]
            d['subunit_4']['number_employees_q2'] = p[100]
            d['subunit_4']['employee_hours_q2'] = p[101]
            d['subunit_4']['tons_production_q2'] = p[102]
            d['subunit_4']['document_number_q3'] = p[103]
            d['subunit_4']['number_employees_q3'] = p[104]
            d['subunit_4']['employee_hours_q3'] = p[105]
            d['subunit_4']['tons_production_q3'] = p[106]
            d['subunit_4']['document_number_q4'] = p[107]
            d['subunit_4']['number_employees_q4'] = p[108]
            d['subunit_4']['employee_hours_q4'] = p[109]
            d['subunit_4']['tons_production_q4'] = p[110]

            yield line_one, d


def writeAE(ds):
    """Read and consolidate into one file all of the self-extracted address/employment files contained within each directory of the list of directories ds.

    Args:
        d (list): list of directories of already self-extracted msha address/employment files
    
    Return:
        writes one .csv per directory where the .csv takes the name of the parent folder of the msha files; 
    """

    for d in ds:
        with open(join(d, d.split('/')[-1]+'.csv'), 'w') as csvfile:
            w = csv.writer(csvfile)
            w.writerow(ae_headers)
            for f in listdir(d):
                F = join(d, f)
                if isfile(F) and 'exe' not in F and 'csv' not in F:
                    for row in readAE(F):
                        w.writerow((row[0]['year_file'],row[0]['update_date']) +
                                   (row[1]['mine_id'],
                                    row[1]['contractor'],
                                    row[1]['filler01'],
                                    row[1]['inspection_office'],
                                    row[1]['state_code'],
                                    row[1]['county_code'], row[1]['sic'],
                                    row[1]['filler02'], row[1]['canvass'],
                                    row[1]['mine_type'],
                                    row[1]['status_code'],
                                    row[1]['status_date'],
                                    row[1]['seam_height'],
                                    row[1]['filler03'],
                                    row[1]['prior_status_code'],
                                    row[1]['travel_area'],
                                    row[1]['mailing_control'],
                                    row[1]['company_name'],
                                    row[1]['filler04'],
                                    row[1]['mine_name'],
                                    row[1]['filler05'], row[1]['address'],
                                    row[1]['filler06'], row[1]['city'],
                                    row[1]['filler07'],
                                    row[1]['state_abbreviation'],
                                    row[1]['zip_code'], row[1]['filler08'],
                                    row[1]['county_name'],
                                    row[1]['injury_flag_q1'],
                                    row[1]['injury_count_q1'],
                                    row[1]['injury_flag_q2'],
                                    row[1]['injury_count_q2'],
                                    row[1]['injury_flag_q3'],
                                    row[1]['injury_count_q3'],
                                    row[1]['injury_flag_q4'],
                                    row[1]['injury_count_q4'],
                                    row[1]['work_group'],
                                    row[1]['update_addition_year'],
                                    row[1]['update_addition_number'],
                                    row[1]['update_change_year'],
                                    row[1]['update_change_number'],
                                    row[1]['number_subunits'],
                                    row[1]['subunit_1']['subunit_1_code'],
                                    row[1]['subunit_1']['document_number_q1'],
                                    row[1]['subunit_1']['number_employees_q1'],
                                    row[1]['subunit_1']['employee_hours_q1'],
                                    row[1]['subunit_1']['tons_production_q1'],
                                    row[1]['subunit_1']['document_number_q2'],
                                    row[1]['subunit_1']['number_employees_q2'],
                                    row[1]['subunit_1']['employee_hours_q2'],
                                    row[1]['subunit_1']['tons_production_q2'],
                                    row[1]['subunit_1']['document_number_q3'],
                                    row[1]['subunit_1']['number_employees_q3'],
                                    row[1]['subunit_1']['employee_hours_q3'],
                                    row[1]['subunit_1']['tons_production_q3'],
                                    row[1]['subunit_1']['document_number_q4'],
                                    row[1]['subunit_1']['number_employees_q4'],
                                    row[1]['subunit_1']['employee_hours_q4'],
                                    row[1]['subunit_1']['tons_production_q4'],
                                    row[1]['subunit_2']['subunit_2_code'],
                                    row[1]['subunit_2']['document_number_q1'],
                                    row[1]['subunit_2']['number_employees_q1'],
                                    row[1]['subunit_2']['employee_hours_q1'],
                                    row[1]['subunit_2']['tons_production_q1'],
                                    row[1]['subunit_2']['document_number_q2'],
                                    row[1]['subunit_2']['number_employees_q2'],
                                    row[1]['subunit_2']['employee_hours_q2'],
                                    row[1]['subunit_2']['tons_production_q2'],
                                    row[1]['subunit_2']['document_number_q3'],
                                    row[1]['subunit_2']['number_employees_q3'],
                                    row[1]['subunit_2']['employee_hours_q3'],
                                    row[1]['subunit_2']['tons_production_q3'],
                                    row[1]['subunit_2']['document_number_q4'],
                                    row[1]['subunit_2']['number_employees_q4'],
                                    row[1]['subunit_2']['employee_hours_q4'],
                                    row[1]['subunit_2']['tons_production_q4'],
                                    row[1]['subunit_3']['subunit_3_code'],
                                    row[1]['subunit_3']['document_number_q1'],
                                    row[1]['subunit_3']['number_employees_q1'],
                                    row[1]['subunit_3']['employee_hours_q1'],
                                    row[1]['subunit_3']['tons_production_q1'],
                                    row[1]['subunit_3']['document_number_q2'],
                                    row[1]['subunit_3']['number_employees_q2'],
                                    row[1]['subunit_3']['employee_hours_q2'],
                                    row[1]['subunit_3']['tons_production_q2'],
                                    row[1]['subunit_3']['document_number_q3'],
                                    row[1]['subunit_3']['number_employees_q3'],
                                    row[1]['subunit_3']['employee_hours_q3'],
                                    row[1]['subunit_3']['tons_production_q3'],
                                    row[1]['subunit_3']['document_number_q4'],
                                    row[1]['subunit_3']['number_employees_q4'],
                                    row[1]['subunit_3']['employee_hours_q4'],
                                    row[1]['subunit_3']['tons_production_q4'],
                                    row[1]['subunit_4']['subunit_4_code'],
                                    row[1]['subunit_4']['document_number_q1'],
                                    row[1]['subunit_4']['number_employees_q1'],
                                    row[1]['subunit_4']['employee_hours_q1'],
                                    row[1]['subunit_4']['tons_production_q1'],
                                    row[1]['subunit_4']['document_number_q2'],
                                    row[1]['subunit_4']['number_employees_q2'],
                                    row[1]['subunit_4']['employee_hours_q2'],
                                    row[1]['subunit_4']['tons_production_q2'],
                                    row[1]['subunit_4']['document_number_q3'],
                                    row[1]['subunit_4']['number_employees_q3'],
                                    row[1]['subunit_4']['employee_hours_q3'],
                                    row[1]['subunit_4']['tons_production_q3'],
                                    row[1]['subunit_4']['document_number_q4'],
                                    row[1]['subunit_4']['number_employees_q4'],
                                    row[1]['subunit_4']['employee_hours_q4'],
                                    row[1]['subunit_4']['tons_production_q4']))
    return()

if __name__ == '__main__':

    # main directories to download / read / write data to
    d = ['/Users/easy-e/Downloads/msha/coal',
         '/Users/easy-e/Downloads/msha/other',
         '/Users/easy-e/Downloads/msha/coal_contractor',
         '/Users/easy-e/Downloads/msha/other_contractor']

    # download
    # f = downloadAE(d[0], range(1983, 2014), coal = True)
    # f = downloadAE(d[1], range(1983, 2014), coal = False, other = True)
    # f = downloadAE(d[2], range(1983, 2014), coal = False, coal_contractors = True)
    # f = downloadAE(d[3], range(1983, 2014),  coal = False, other_contractors = True)

    # csv
    # writeAE(d)
    


