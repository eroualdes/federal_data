#!/usr/bin/python
# -*- coding: utf-8 -*-

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
        else:
            raise
    return
