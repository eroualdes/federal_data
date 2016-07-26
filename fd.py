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
from inspect import signature
from pathlib import Path
from distutils.util import strtobool as stb
import pandas as pd
from zipfile import ZipFile as zf
import requests as r
import re

# globals/decorators
actions = {}
def act(fdDir):
  pass
action_signature = signature(act)

def action(fn):
  global actions
  matching_signature = signature(fn) == action_signature
  assert matching_signature,"{!r}'s signature misspecified.".format(fn.__name__)
  actions[fn.__name__] = fn
  return fn

subcommands = {}
def subcommand(fn):
  global subcommands
  assert fn.__doc__, "Document the subcommand {!r}.".format(fn.__name__)
  subcommands[fn.__name__] = fn
  return fn

############################ agency: bls #############################

bls = {
  'base': 'http://www.bls.gov/',
  'datasets': {
    'cew': 'Quarterly Census of Employment and Wages',
    'ce': 'Employment, Hours, and Earnings - National'
  }
}

def get_bls_dtypes(bls_dict):
  """Get dtypes from dictionary that defines bls:dataset of interest."""
  # TODO this might be generalized to all agencies.
  items = bls_dict['dtype'].items()
  ints = [k for k,v in items if v == int]
  flts = [k for k,v in items if v == float]
  strs = [k for k,v in items if v == str]
  return [ints, flts, strs]

########################## agency: bls cew ###########################

bls_cew = {
  'webpage': bls['base']+'cew/datatoc.htm',
  'rgxs': [
    r'(?P<url>cew/data/files/[0-9]{4}/csv/(?P<year>[0-9]{4})_qtrly_naics10_totals.zip)',
    r'(?P<url>cew/data/files/[0-9]{4}/csv/(?P<year>[0-9]{4})_qtrly_by_industry.zip)'],
  'dtype': {                  # TODO differentiate float from int
    'area_fips': str, 'own_code': str, 'industry_code': str,
    'agglvl_code': str, 'size_code': str, 'year': str, 'qtr': float,
    'disclosure_code': str, 'area_title': str, 'own_title': str,
    'industry_title': str, 'agglvl_title': str, 'size_title': str,
    'qtrly_estabs_count': float, 'month1_emplvl': float,
    'month2_emplvl': float, 'month3_emplvl': float,
    'total_qtrly_wages': float, 'taxable_qtrly_wages': float,
    'qtrly_contributions': float, 'avg_wkly_wage': float,
    'lq_disclosure_code': str, 'lq_qtrly_estabs_count': float,
    'lq_month1_emplvl': float, 'lq_month2_emplvl': float,
    'lq_month3_emplvl': float, 'lq_total_qtrly_wages': float,
    'lq_taxable_qtrly_wages': float, 'lq_qtrly_contributions': float,
    'lq_avg_wkly_wage': float, 'oty_disclosure_code': str,
    'oty_qtrly_estabs_count_chg': float,
    'oty_qtrly_estabs_count_pct_chg': float,
    'oty_month1_emplvl_chg': float, 'oty_month1_emplvl_pct': float,
    'oty_month2_emplvl_chg': float, 'oty_month2_emplvl_pct': float,
    'oty_month3_emplvl_chg': float, 'oty_month3_emplvl_pct': float,
    'oty_total_qtrly_wages_chg': float, 'oty_total_qtrly_wages_pct': float,
    'oty_taxable_qtrly_wages_chg': float,
    'oty_taxable_qtrly_wages_pct': float,
    'oty_qtrly_contributions_chg': float,
    'oty_qtrly_contributions_pct': float,
    'oty_avg_wkly_wage_chg': float, 'oty_avg_wkly_wage_pct': float
  },
}

def get_bls_cew_urls():
  html = r.get(bls_cew['webpage']).text
  for rgx in bls_cew['rgxs']:
    for url_match in re.finditer(rgx, html):
      yield bls['base']+url_match.group('url')


@action
def bls_cew_download(fdDir):
  d = check_directory_download(fdDir.joinpath('bls/cew'))
  vprint("bls:cew -> {0}".format(d))
  for url in get_bls_cew_urls():
    copy_url(url, d)
    vprint("bls:cew data downloaded\x1b[K.")


@action
def bls_cew_consolidate(fdDir):
  """Consolidate downloaded BLS files."""
  d = check_directory_consolidate(fdDir.joinpath('bls/cew'))
  zips = d.glob('*.zip')
  csvfile = d / 'data.csv'
  dtypes = get_bls_dtypes(bls_cew)
  header = True
  with csvfile.open('a') as f:
    for z in zips:
      vprint('Consolidating {0}...'.format(str(z).split('/')[-1]), end="\r")
      with zf(str(z), 'r') as zfile:
        csvs = (csv for csv in zfile.namelist()
                if re.search(r'all industries.csv', csv))
        for csv in csvs:
          for chunk in pd.read_csv(zfile.open(csv),
                                   iterator=True,
                                   chunksize=10000):
            if False:
              # TODO consolidate only fips rows of CSVs?
              # TODO need have fips.csv on hand
              fips = pd.read_csv(fdDir +  "fips.csv")
              chunk = chunk[chunk.area_fips.isin(fips.fips)]

            # fix incorrectly named column
            try:
              chunk.rename(columns={'oty_taxable_qtrly_wages_chg.1':
                                    'oty_taxable_qtrly_wages_pct'},
                           inplace=True)
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
  'data_urls': ['ce.data.0.AllCESSeries', 'ce.datatype',
                'ce.industry', 'ce.period', 'ce.seasonal',
                'ce.series', 'ce.supersector'],
  'docs': 'ce.txt',
  'dtype': {
    'data_type_text': str, 'industry_name': str,
    'period_name': str, 'series_title': str,
    'supersector_name': str, 'series_id': str,
    'industry_code': str, 'naics_code': str,
    'period': str, 'footnote_codes': str,
    'sort_sequence': str, 'publishing_status': str,
    'supersector_code': str, 'data_type_code': str,
    'seasonal': str, 'footnote_code_series': str,
    'begin_period': str, 'end_period': str,
    'display_level': str, 'selectable': str,
    'season_text': str, 'supersector_name': str,
    'period_abbr': str, 'period_name': str,
    'year': int, 'value': float, 'begin_year': int,
    'begin_period': str, 'end_period': str, 'end_year': int
  }
}

@action
def bls_ce_download(fdDir):
  d = check_directory_download(Path(fdDir, 'bls/ce'))
  vprint("bls:ce -> {0}".format(d))
  for url in bls_ce['data_urls']:
    copy_url(bls_ce['webpage']+url, d)
    vprint("bls:ce data downloaded\x1b[K.")

@action
def bls_ce_consolidate(fdDir):
  d = check_directory_consolidate(fdDir.joinpath('bls/ce'))
  vprint('Consolidating {0}...'.format(d), end="\r")

  # merge to series
  series = pd.read_table(d / 'ce.series',
                         dtype={'supersector_code': str, 'data_type_code': str,
                                'industry_code': str, 'seasonal': str,
                                'begin_year': int, 'end_year': int})
  series.rename(columns={'footnote_codes': 'footnote_code_series'},
                inplace=True)

  data_type = pd.read_table(d / 'ce.datatype',
                            index_col=False, dtype={'data_type_code': str,
                                                    'data_type_text': str})
  series = pd.merge(series, data_type, how='left', on='data_type_code')
  del data_type

  industry_type = pd.read_table(d / 'ce.industry', index_col=False,
                                dtype={'industry_code': str, 'naics_code': str,
                                       'publishing_status': str,
                                       'industry_name': str,
                                       'display_level': str, 'selectable': str,
                                       'sort_sequence': str})
  series = pd.merge(series, industry_type, how='left', on='industry_code')
  del industry_type

  season = pd.read_table(d / 'ce.seasonal', index_col=False,
                         dtype={'industry_code': str, 'seasonal_text': str})
  season.columns = ['seasonal', 'season_text']
  series = pd.merge(series, season, how='left', on='seasonal')
  del season

  sector = pd.read_table(d / 'ce.supersector', index_col=False,
                         dtype={'supersector_code': str,
                                'supersector_name': str})
  series = pd.merge(series, sector, how='left', on='supersector_code')
  del sector

  period = pd.read_table(d / 'ce.period', header=None,
                         names=['period', 'period_abbr', 'period_name'])

    # merge series and period to All in chunks then write
  csvfile = d / 'data.csv'
  header = True                   # only write headers once
  dtypes = get_bls_dtypes(bls_ce) # get dtypes
  with csvfile.open('a') as f:
    for chunk in pd.read_table(d / 'ce.data.0.AllCESSeries', iterator=True,
                               chunksize=10000):
      chunk = pd.merge(chunk, series, how='left', on='series_id')
      chunk = pd.merge(chunk, period, how='left', on='period')
      chunk = convert_dtypes(chunk, dtypes)
      chunk.to_csv(f, header=header, index=False)
      header = False
      vprint("bls:ce data consolidated\x1b[K.")


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
  """Check directory (p, a Path) is appropriate for downloading: exists and is empty."""
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
  # TODO and has appropriate files.
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
    # print(args)
    # vprint(repr(args.directory))
    actions[act](args.directory)
  else:
    # TODO more helpful fail; does fd not understand the dataset or agency?
    msg = "fd doesn't understand how to {0} {1}."
    print(msg.format(args.action, args.ad))

@subcommand
def consolidate(args):
  """Consolidate already downloaded agency dataset(s).

To consolidate already downloaded agency data use:\n\t$ ./fd.py consolidate agency:dataset\nwhere `agency` is the abbreviation for the agency of interest and `dataset` is the abbreviation for a dataset from that agency.
    """
  # TODO \nWhen no dataset is specified, all downloaded AGENCY datasets will be consolidated.
  dispatch(args)

@subcommand
def download(args):
  """Download agency dataset.

To download a dataset from a federal agency use:\n\t$ ./fd.py download agency:dataset\nwhere `agency` is the abbreviation for the agency of interest and `dataset` is the abbreviation for a dataset from that agency.
    """
  dispatch(args)

agencies = {'bls': bls}
@subcommand
def available(args):
  """List available agencies or their datasets.

To list available datasets for a specific agency use:\n\t$ ./fd.py available agency\nA list of available federal agencies is provided when no agency is specified.
    """

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
    msg = ("fd plays nicely with some of the datasets",
           " from the following agencies: {0}")
    print(''.join(msg).format(', '.join(agencies.keys()).upper()))


@subcommand
def help(args):
  """Provide appropriate help message given user supplied args."""
  # TODO provide help on agencies and agency/datasets; provide links?
  # their docs? my own docs?
  global subcommands
  scommand = args.action
  if scommand:
    if scommand in subcommands:
      print(''.join(subcommands[scommand].__doc__.split('\n', 1)[1:]))
    else:
      print("fd doesn't understand specified subcommand, {0}.".format(scommand))
  else:
    parser.print_help()

############################ cli: parsing ############################

parser = argparse.ArgumentParser(
  prog='fd',
  usage='%(prog)s [options] action agency:dataset'
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
  metavar='action', dest='action'
)

parser_available = subparser.add_parser(
  'available',
  aliases='a',
  help='list available agencies or datasets'
)

parser_available.add_argument(
  'agency',
  choices=['bls'],
  metavar='agency',
  type=str.lower,
  nargs='?',
  default=None
)

parser_available.set_defaults(func=available)

parser_download = subparser.add_parser(
  'download',
  aliases='d',
  help="download agency's dataset"
)

parser_download.add_argument(
  'ad',
  metavar='agency:dataset',
  type=str.lower
)

parser_download.set_defaults(func=dispatch)

parser_consolidate = subparser.add_parser(
  'consolidate',
  aliases='c',
  help='consolidate agency datasets'
)

parser_consolidate.add_argument('ad', metavar='agency', type=str.lower)

parser_consolidate.set_defaults(func=dispatch)


parser_help = subparser.add_parser('help', help='help on a specific action')

parser_help.add_argument(
  'action',
  metavar='action',
  type=str.lower,
  nargs='?',
  default=None
)

parser_help.set_defaults(func=help)

def main():
  global args
  args = parser.parse_args()
  sys.exit(args.func(args))
