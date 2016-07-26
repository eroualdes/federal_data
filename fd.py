# fd provides analysis ready US federal data.
# Copyright (C) 2016 Edward A. Roualdes

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import sys
import argparse
from pathlib import Path
from distutils.util import strtobool as stb
import pandas as pd
from zipfile import ZipFile as zf
import requests as r
import re

# globals/decorators
actions = {}
def action(fn):
  global actions
  assert fn.__doc__, "Document the subcommand {!r}.".format(fn.__name__)
  actions[fn.__name__] = fn
  return fn

############################ agency: bls #############################

bls = {
  'base': 'http://www.bls.gov/',
  'datasets': {
    'cew': 'Quarterly Census of Employment and Wages',
    'ce': 'Employment, Hours, and Earnings - National',
    'sm': 'Employment, Hours, and Earnings - State and Metro Area',
  },
}

def get_bls_dtypes(bls_dict):
  """Get dtypes from dictionary that defines bls:dataset of interest."""
  # TODO this might be generalized to all agencies.
  items = bls_dict['dtype'].items()
  ints = [k for k,v in items if v == int]
  flts = [k for k,v in items if v == float]
  strs = [k for k,v in items if v == str]
  return [ints, flts, strs,]

########################## agency: bls cew ###########################

bls_cew = {
  'webpage': bls['base']+'cew/datatoc.htm',
  'rgxs': [
    r'(?P<url>cew/data/files/[0-9]{4}/csv/(?P<year>[0-9]{4})_qtrly_naics10_totals.zip)',
    r'(?P<url>cew/data/files/[0-9]{4}/csv/(?P<year>[0-9]{4})_qtrly_by_industry.zip)',
  ],
  'dtype': {
    'area_fips': str,
    'own_code': str,
    'industry_code': str,
    'agglvl_code': str,
    'size_code': str,
    'year': str,
    'qtr': float,
    'disclosure_code': str,
    'area_title': str,
    'own_title': str,
    'industry_title': str,
    'agglvl_title': str,
    'size_title': str,
    'qtrly_estabs_count': float,
    'month1_emplvl': float,
    'month2_emplvl': float,
    'month3_emplvl': float,
    'total_qtrly_wages': float,
    'taxable_qtrly_wages': float,
    'qtrly_contributions': float,
    'avg_wkly_wage': float,
    'lq_disclosure_code': str,
    'lq_qtrly_estabs_count': float,
    'lq_month1_emplvl': float,
    'lq_month2_emplvl': float,
    'lq_month3_emplvl': float,
    'lq_total_qtrly_wages': float,
    'lq_taxable_qtrly_wages': float,
    'lq_qtrly_contributions': float,
    'lq_avg_wkly_wage': float,
    'oty_disclosure_code': str,
    'oty_qtrly_estabs_count_chg': float,
    'oty_qtrly_estabs_count_pct_chg': float,
    'oty_month1_emplvl_chg': float,
    'oty_month1_emplvl_pct': float,
    'oty_month2_emplvl_chg': float,
    'oty_month2_emplvl_pct': float,
    'oty_month3_emplvl_chg': float,
    'oty_month3_emplvl_pct': float,
    'oty_total_qtrly_wages_chg': float,
    'oty_total_qtrly_wages_pct': float,
    'oty_taxable_qtrly_wages_chg': float,
    'oty_taxable_qtrly_wages_pct': float,
    'oty_qtrly_contributions_chg': float,
    'oty_qtrly_contributions_pct': float,
    'oty_avg_wkly_wage_chg': float,
    'oty_avg_wkly_wage_pct': float,
  },
}

def get_bls_cew_urls():
  """Return full BLS cew URLs to be downloaded."""
  html = r.get(bls_cew['webpage']).text
  for rgx in bls_cew['rgxs']:
    for url_match in re.finditer(rgx, html):
      yield bls['base']+url_match.group('url')


@action
def bls_cew_download(fdDir):
  """Download BLS cew data."""
  d = check_directory_download(fdDir.joinpath('bls/cew'))
  vprint("bls:cew -> {0}".format(d))
  for url in get_bls_cew_urls():
    copy_url(url, d)
    vprint("bls:cew data downloaded\x1b[K.")


@action
def bls_cew_consolidate(fdDir):
  """Consolidate downloaded BLS cew data."""
  d = check_directory_consolidate(fdDir.joinpath('bls/cew'))
  zips = d.glob('*.zip')
  csvfile = d / 'data.csv'
  dtypes = get_bls_dtypes(bls_cew)
  header = True                 # write header only once

  with csvfile.open('a') as f:
    for z in zips:

      vprint('Consolidating {0}...'.format(str(z).split('/')[-1]), end="\r")

      with zf(str(z), 'r') as zfile:
        csvs = (csv for csv in zfile.namelist()
                if re.search(r'all industries.csv', csv))
        for csv in csvs:
          for chunk in pd.read_csv(zfile.open(csv), chunksize=10000):
            if False:
              # TODO consolidate only fips rows of CSVs?
              # TODO need have fips.csv on hand
              fips = pd.read_csv(fdDir +  "fips.csv")
              chunk = chunk[chunk.area_fips.isin(fips.fips)]

            # fix incorrectly named column
            try:
              chunk.rename(
                columns={
                  'oty_taxable_qtrly_wages_chg.1': 'oty_taxable_qtrly_wages_pct',
                },
                inplace=True
              )
            except KeyError:
              pass

            # make data types match across chunks
            chunk = convert_dtypes(chunk, dtypes)
            chunk.to_csv(f, header=header, index=False)
            header = False

  vprint("bls:cew data consolidated\x1b[K.")

########################### agency: bls ce ###########################

bls_ce = {
  'webpage': 'http://download.bls.gov/pub/time.series/ce/',
  'data_urls': [
    'ce.data.0.AllCESSeries',
    'ce.datatype',
    'ce.industry',
    'ce.period',
    'ce.seasonal',
    'ce.series',
    'ce.supersector',
  ],
  'docs': 'ce.txt',
  'dtype': {
    'data_type_text': str,
    'industry_name': str,
    'period_name': str,
    'series_title': str,
    'supersector_name': str,
    'series_id': str,
    'industry_code': str,
    'naics_code': str,
    'period': str,
    'footnote_codes': str,
    'sort_sequence': str,
    'publishing_status': str,
    'supersector_code': str,
    'data_type_code': str,
    'seasonal': str,
    'footnote_code_series': str,
    'begin_period': str,
    'end_period': str,
    'display_level': str,
    'selectable': str,
    'season_text': str,
    'supersector_name': str,
    'period_abbr': str,
    'year': int,
    'value': float,
    'begin_year': int,
    'end_year': int,
  },
}

@action
def bls_ce_download(fdDir):
  """Download BLS ce data."""
  d = check_directory_download(Path(fdDir, 'bls/ce'))
  vprint("bls:ce -> {0}".format(d))
  for url in bls_ce['data_urls']:
    copy_url(bls_ce['webpage']+url, d)
    vprint("bls:ce data downloaded\x1b[K.")

@action
def bls_ce_consolidate(fdDir):
  """Consolidate downloaded BLS ce data."""
  d = check_directory_consolidate(fdDir.joinpath('bls/ce'))
  vprint('Consolidating {0}...'.format(d), end="\r")

  # merge to series
  series = pd.read_table(
    d / 'ce.series',
    dtype={
      'supersector_code': str,
      'data_type_code': str,
      'industry_code': str,
      'seasonal': str,
      'begin_year': int,
      'end_year': int,
    }
  )
  series.rename(
    columns={'footnote_codes': 'footnote_code_series',},
    inplace=True
  )

  data_type = pd.read_table(
    d / 'ce.datatype',
    index_col=False,
    dtype={
      'data_type_code': str,
      'data_type_text': str,
    }
  )
  series = pd.merge(series, data_type, how='left', on='data_type_code')
  del data_type

  industry_type = pd.read_table(
    d / 'ce.industry',
    index_col=False,
    dtype={
      'industry_code': str,
      'naics_code': str,
      'publishing_status': str,
      'industry_name': str,
      'display_level': str,
      'selectable': str,
      'sort_sequence': str,
    }
  )
  series = pd.merge(series, industry_type, how='left', on='industry_code')
  del industry_type

  season = pd.read_table(
    d / 'ce.seasonal',
    index_col=False,
    dtype={
      'industry_code': str,
      'seasonal_text': str,
    }
  )
  season.columns = ['seasonal', 'season_text',]
  series = pd.merge(series, season, how='left', on='seasonal')
  del season

  sector = pd.read_table(
    d / 'ce.supersector',
    index_col=False,
    dtype={
      'supersector_code': str,
      'supersector_name': str,
    }
  )
  series = pd.merge(series, sector, how='left', on='supersector_code')
  del sector

  period = pd.read_table(
    d / 'ce.period',
    header=None,
    names=['period', 'period_abbr', 'period_name',]
  )

  # merge series and period to All in chunks then write
  csvfile = d / 'data.csv'
  header = True                   # only write headers once
  dtypes = get_bls_dtypes(bls_ce)
  with csvfile.open('a') as f:
    for chunk in pd.read_table(d / 'ce.data.0.AllCESSeries', chunksize=10000):
      chunk = pd.merge(chunk, series, how='left', on='series_id')
      chunk = pd.merge(chunk, period, how='left', on='period')
      chunk = convert_dtypes(chunk, dtypes)
      chunk.to_csv(f, header=header, index=False)
      header = False
      vprint("bls:ce data consolidated\x1b[K.")

############################## agency: bls sm ###############################
bls_sm = {
  'webpage': 'http://download.bls.gov/pub/time.series/sm/',
  'data_urls': [
    'sm.data.1.AllData',
    'sm.area',
    'sm.data_type',
    'sm.industry',
    'sm.series',
    'sm.state',
    'sm.supersector',
  ],
  'docs': 'sm.txt',
  'dtype': {
    'area_code': str,
    'area_name': str,
    'benchmark_year': int,
    'state_code': str,
    'state_name': str,
    'data_type_text': str,
    'industry_name': str,
    'period_name': str,
    'series_id': str,
    'supersector_code': str,
    'industry_code': str,
    'period': str,
    'footnote_codes': str,
    'footnote_code': str,
    'footnote_text': str,
    'data_type_code': str,
    'seasonal': str,
    'begin_period': str,
    'end_period': str,
    'period_abbr': str,
    'period': str,
    'year': int,
    'value': float,
    'begin_year': int,
    'end_year': int,
  },
}

# TODO consolidate bls:sm:
# 1. merge series <- area, supersector, datatype, industry, state
# 2. merge all <- series

@action
def bls_sm_download(fdDir):
  """Download BLS sm data."""
  d = check_directory_download(Path(fdDir, 'bls/sm'))
  vprint("bls:sm -> {0}".format(d))
  for url in bls_sm['data_urls']:
    copy_url(bls_sm['webpage']+url, d)
    vprint("bls:sm data downloaded\x1b[K.")

############################# utilities ##############################

def copy_url(url, directory):
  """Copy url into directory."""

  filename = url.split('/')[-1]
  d = Path(directory)
  vprint('Downloading {0}...\x1b[K'.format(filename), end="\r")

  path = d / filename
  req = r.get(url, stream=True)
  if req.status_code != r.codes.ok:
    req.raise_for_status()
  with path.open('wb+') as f:
    for chunk in req.iter_content(chunk_size=1024):
      if chunk:
        f.write(chunk)

def proceed(prompt):
  """User permission to proceed."""
  while True:
    print(prompt)
    try:
      choice = stb(input().lower())
      return choice
    except ValueError:
      print("Please respond with 'yes' or 'no'.")

def vprint(*pargs, **pkwargs):
  """Verbose printing."""
  global args
  if args.verbose:
    print(*pargs, **pkwargs, flush=True)

def check_directory_download(p):
  """Check directory (p, a Path) is appropriate for downloading:
  exists and is empty.
  """
  if p.exists() and p.is_dir():
    contents = [x for x in p.iterdir()]
    if contents:
      msg = ('Directory {0} is not empty -- files will be overwritten.',
             '  Proceed anyway?')
      permission = proceed(''.join(msg).format(str(p)))
      if not permission:
        sys.exit(1)
  else:
    msg = 'Director {0} does not exist; make it and try again.'
    print(msg.format(str(p)))
    sys.exit(1)
  return p

def check_directory_consolidate(p):
  """Check directory (p, a Path) is appropriate for consolidating: exists."""
  # TODO check directory for appropriate files.
  if not p.exists() or not p.is_dir():
    msg = 'Director {0} does not exist; download data first and try again.'
    print(msg.format(str(p)))
    sys.exit(1)
  return p

def convert_dtypes(df, dtypes):
  """Convert df's variables to specified dtypes."""
  ints, flts, strs = dtypes
  df[ints] = df[ints].astype('int8') if ints else None
  df[flts] = df[flts].astype('float32') if flts else None
  df[strs] = df[strs].astype('str') if strs else None
  return df


################################ cli #################################
########################## cli: subcommands ##########################

def dispatch(args):
  """Dispatch user supplied action/agency to appropriate function."""

  global actions

  act = '_'.join(args.ad.split(':') + [args.action])

  if act in actions:
    actions[act](args.directory)
  else:
    # TODO add more helpful fail; does fd not understand the dataset or agency?
    msg = "fd doesn't understand how to {0} {1}."
    print(msg.format(args.action, args.ad))

agencies = {'bls': bls}
def available(args):
  """Print available agencies or their datasets."""

  ag = args.agency
  if ag:
    if ag in agencies:
      agency = agencies[ag]

      print('{0} has available datasets:'.format(ag.upper()))

      for k, v in agency['datasets'].items():
        print('\t{0:3s} - {1}'.format(k, v))
    else:
      print("fd doesn't know how to work with {0}, yet.".format(ag))
  else:
    # TODO when more agencies added, formatting will become necessary
    msg = ("fd plays nicely with some of the datasets",
           " from the following agencies:\n\t {0}")
    print(''.join(msg).format(', '.join(agencies.keys()).upper()))


def get_choices():
  """Get allowable choices of agency:dataset combinations."""

  choices = []
  for k,v in agencies.items():
    for d in v['datasets']:
      choices.append(k + ':' + d)
  return choices

############################ cli: parsing ############################

parser = argparse.ArgumentParser(
  prog='fd',
  description='Provide analysis ready US federal data.'
)

parser.add_argument(
  '-d',
  '--directory',
  default=Path.home() / 'fdata',
  type=Path,
  help='set directory for fd to use'
)

parser.add_argument(
  '-v',
  '--verbose',
  action='store_true',
  help='print status along the way'
)

parser.add_argument(
  '--version',
  action='version',
  version='%(prog)s v0.1'
)

subparser = parser.add_subparsers(
  help='actions to perform on federal agencies',
  metavar='action',
  dest='action'
)

parser_available = subparser.add_parser(
  'available',
  aliases='a',
  description="Print available agencies or available datasets relative to a specified agency.",
  help='list available agencies or datasets',
  formatter_class=argparse.RawDescriptionHelpFormatter,
  epilog="""NB.  Print available federal agencies when no agency is specified.

examples:

  $ fd available

  $ fd available bls
  """
)

parser_available.add_argument(
  'agency',
  help='list datasets from specified agency',
  metavar='agency',
  type=str.lower,
  nargs='?',
  default=None
)

parser_available.set_defaults(func=available)

parser_download = subparser.add_parser(
  'download',
  aliases='d',
  description="Download specified agency's dataset.",
  help="download agency's dataset",
  formatter_class=argparse.RawDescriptionHelpFormatter,
  epilog="""example:

  $ fd download bls:cew
  """,
)

parser_download.add_argument(
  'ad',
  help='agency and dataset of interest, abbreviations only',
  choices=get_choices(),
  metavar='agency:dataset',
  nargs='?',
  type=str.lower,
  default=None
)

parser_download.set_defaults(func=dispatch)

parser_consolidate = subparser.add_parser(
  'consolidate',
  aliases='c',
  description="Consolidate specified agency's downloaded dataset.",
  help="consolidate agency's dataset",
  formatter_class=argparse.RawDescriptionHelpFormatter,
  epilog="""example:

$ fd consolidate bls:cew
  """
)

parser_consolidate.add_argument(
  'ad',
  help='agency and dataset of interest, abbreviations only',
  choices=get_choices(),
  metavar='agency:dataset',
  nargs='?',
  type=str.lower,
  default=None
)

parser_consolidate.set_defaults(func=dispatch)

########################### manin program ############################

def main():
  global args
  args = parser.parse_args()
  sys.exit(args.func(args))
