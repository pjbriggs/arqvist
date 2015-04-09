#!/bin/env python
#
# Utility functions for unit testing the curatus package by
# making test directories, files and links
import os
import bz2
import tempfile
import shutil

class MockArchiveFile:
    # Mock version of the ArchiveFile class
    def __init__(self,path):
        self.path = os.path.abspath(path)
        self.basename = os.path.basename(self.path)
        self.ext = os.path.splitext(self.path)[1].lstrip('.')

def make_temp_dir():
    # Create a temporary directory
    return tempfile.mkdtemp()

def rmdir(dir_):
    # Remove a directory
    if os.path.exists(dir_):
        shutil.rmtree(dir_)

def make_subdir(dirn,subdir):
    # Create subdir under dirn
    subdir = os.path.join(dirn,subdir)
    os.mkdir(subdir)
    return subdir

def make_file(filen,dirn=None,text=None,compress=None):
    # Create a new file
    if dirn is not None:
        filen = os.path.join(dirn,filen)
    if compress is None:
        fp = open(filen,'w')
    else:
        if compress == 'bz2':
            fp = bz2.BZ2File(filen,'w')
        else:
            raise Exception("%s: compression not implemented" % compress)
    if text is not None:
        fp.write("%s" % text)
    fp.close()
    return filen

def make_symlink(link,target,dirn=None):
    # Create a symbolic link
    if dirn is not None:
        link = os.path.join(dirn,link)
    os.symlink(target,link)
    return link
