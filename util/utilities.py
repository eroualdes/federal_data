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
from errno import EEXIST
from shutil import rmtree
from os import makedirs, listdir
from sys import exit

def num(x):
    try:
        x = float(x)
    except ValueError:
        x = 0
    return(x)

def mkdir(path, overwrite):
    """
    make directory with the option to overwrite (=True) existing directory tree, or exit program to specify a new directory. If overwrite == True, specify an opt (= some string)
    """

    try:
        makedirs(path)       # just try to make it
    except OSError as exc:      # catch exception
        if exc.errno == EEXIST:
            if overwrite:
                rmtree(path)
                makedirs(path)
            else:
                exit('Please delete or move the directory \'%s\' and try again.' % path)
                return
        else:
            raise
    return
