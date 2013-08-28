#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function
from datetime import datetime
from os.path import join, basename, isfile
from os import makedirs, listdir
from errno import EEXIST
from shutil import rmtree
from aux import states
from struct import Struct
from bson.code import Code
from pymongo import MongoClient
from cStringIO import StringIO
from csv import DictReader
from zipfile import ZipFile
from urllib2 import urlopen, URLError, HTTPError

# fips

def genFIPS_DOCS(fips_titles, years):
    """generate the base documents for database: fips cross year as main index. Extra info is carried along

    Args:
        fips(list): list of dicts each is a document with basic information on that fip
        years(list): list of all years to be included in main index

    Returns:
        new list of fips x year
    """

    fips_list = []
    with open(titles, 'rU') as csvfile:
        spamreader = DictReader(csvfile, delimiter=',', quotechar='"')
        for row in spamreader:
            tmp = {}
            tmp['fips'] = int(row['fips'].strip())
            tmp['name'] = row['name'].strip().decode('latin-1') # handle stupid Dona county, NM
            tmp['state'] = row['state'].strip()
            fips_list.append(tmp)

    out = []
    for y in years:
        for f in fips_list:
            m = {'fips': f['fips'],
                 'name': f['name'],
                 'state': f['state'],
                 'year': y}
            out.append(m)

    return(out)

def downloadCSV(url):
    """download `url` as .zip, extract the .csv and yield csv.DictReader object

    Args:
        url (str): a full URL to a .zip with only one .csv inside

    Returns:
        row (csv.DictReader generator): a row of the .csv as a map
    """

    # read zip file from online
    try:
        u = urlopen(url)
        print('downloading ' + url)
        with ZipFile(StringIO(u.read()), 'r') as zf:
            c = cew_file+'.csv'
            spam = DictReader(zf.open(c), delimiter=',', quotechar='"')

    #handle errors
    except HTTPError, e:
        print('HTTP Error:', e.code, url)
    except URLError, e:
        print('URL Error:', e.reason, url)

    for row in spam:
        yield row


def readCSV(f, z = True):
    """read `f` from disk and yield csv.DictReader object
    
    Args:
        f (str): a pathname to a .csv or .zip with only one .csv inside
        z (boolean): True => .zip file, False => .csv

    Returns:
        row (csv.DictReader generator): a row of the .csv as a map
    """

    F = basename(u)
    if z:
        with ZipFile(f+'.zip', 'r') as zf:
            spam = DictReader(zf.open(F+'.csv'), delimiter=',', quotechar='"')
    else:
        spam = DictReader(open(f+'.csv', 'r'), delimiter = ',', quotechar='"')

    for row in spam:
        yield row



def updateDoc(varName, varData, time_period, year, collection, 
              sub_date = None, fips = None, state = None, name = None):
    """insert/update document in 'collection' with appropriate format

    Args:
        fips(int): fips number of document of interest
        state(string): state abbreviation of document of interest
        name(string): name of document of interest
        time_period(string): either 'yearly', 'monthly', 'quarterly'
        sub_date(int): an integer specifying the month or quarter, relative to time_period
        year(int): the year of interest
        collection: which collection is to be updated
        varName(string): name of the variable to add to document of interest
        varData(float): data to be added to document

    Returns:
        Nothing: updates the collection raising error if something went wrong
    """

    # ensure appropriate arguments are supplied
    if not any([fips, state, name]):
        print('Please specify at least of the following arguments: fips, state, name.')
        return(0)

        
    # prepare varData if have monthly or quarterly data
    if time_period == 'monthly':
        varName = varName + '.monthly.' + sub_date
    elif time_period == 'quarterly':
        varName = varName + '.quarterly.' + sub_date
    else:
        print('Do not understand time_period: %s.' % time_period)
        return(0)
        
    # try to update appropriate document
    try:
        if fips:
            collection.update({'$or': [{'fips': fips}, {'fips': fips, 'year': year}]},
                              {'$set': {'year': year, varName: varData}},
                              upsert = 1)
        elif state:
            collection.update({})
        elif name:
            collection.update({})

    except OperationFailure, e:
        print('Update error:', e)    

# utilities




# extra

# get county employment numbers
cew_base = 'ftp://ftp.bls.gov/pub/special.requests/cew/beta/'
year = '2012'
cew_file = '2012.annual.singlefile'
# url = os.path.join(cew_base, year, cew_file+'.zip')


if __name__ == '__main__':







