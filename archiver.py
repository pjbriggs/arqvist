#!/bin/env python
#
#     archiver.py: utility for archiving and curating NGS data
#     Copyright (C) University of Manchester 2015 Peter Briggs
#

"""
Archiving and curation helper utility for NGS data

"""

import os
import sys
import logging
from bcftbx.cmdparse import CommandParser
from bcftbx.utils import strip_ext
from auto_process_ngs import applications

__version__ = '0.0.1'

#######################################################################
# Classes
#######################################################################

class DataDir:
    """
    Class for interrogating and manipulating an NGS data dir

    TODO:
    - add a subdirectory to cache checksums etc
    - store data about platform, year?

    """
    def __init__(self,dirn):
        """
        Create a new DataDir instance
        """
        self._dirn = os.path.abspath(dirn)

    @property
    def name(self):
        """
        Basename of the directory
        """
        return os.path.basename(self._dirn)

    @property
    def path(self):
        """
        Full path of the directory
        """
        return self._dirn

    def walk(self):
        """
        Traverse all files in the directory

        Returns:
          Yields the name and full path for each file found.
          
        """
        for d in os.walk(self._dirn):
            for f in d[2]:
                yield os.path.normpath(os.path.join(d[0],f))

    def list_files(self,extensions=None):
        """
        Return a list of files
        """
        files = []
        for f in self.walk():
            ext,compression = get_file_extensions(f)
            if ext in extensions:
                files.append(f)
        return files

    def info(self):
        """
        """
        print "Dir: %s" % self._dirn

    def copy_to(self,working_dir,chmod=None,dry_run=False):
        """Copy (rsync) data dir to another location
        """
        rsync_cmd = applications.general.rsync(self._dirn,
                                               working_dir,
                                               dry_run=dry_run,
                                               chmod=chmod)
        try:
            status = rsync_cmd.run_subprocess()
        except Exception,ex:
            logging.error("Exception rsyncing to archive: %s" % ex)
            status = -1
        return status

#######################################################################
# Functions
#######################################################################

def get_file_extensions(filen):
    """
    Extract extension and compression type from filename

    """
    ext = None
    compression = None
    file_parts = os.path.basename(filen).split('.')
    if file_parts[-1] in ('gz','bz2'):
        compression = file_parts[-1]
        file_parts = file_parts[:-1]
    if len(file_parts) > 1:
        ext = file_parts[-1]
    return (ext,compression)

def compress_files(datadir,extensions,dry_run=False):
    """
    Compress (bzip2) files with specified extensions

    TODO:
    - verify (checksum before and after?)
    - check for existing bz2 file before compressing?

    """
    n_compressed = 0
    for f in DataDir(datadir).walk():
        if os.path.islink(f):
            # Skip link
            continue
        ext,compression = get_file_extensions(f)
        if compression is not None:
            # Already compressed, skip
            continue
        if ext not in extensions:
            # Not specified type
            continue
        # Construct compression command
        bzip2_cmd = applications.Command('bzip2',f)
        print bzip2_cmd
        if dry_run: continue
        # Execute compression command
        try:
            status = bzip2_cmd.run_subprocess()
            n_compressed += 1
        except Exception,ex:
            logging.error("Exception compressing: %s" % ex)
            status = -1
        if status != 0:
            logging.error("Compression failed on %s" % f)
            return status
    print "Compressed %d files" % n_compressed

def find_related(datadir):
    """
    Examine symlinks and find those pointing outside this dir

    TODO:
    - functionality not implemented, should be just 'symlinks'?

    """
    for f in DataDir(datadir).walk():
        if not os.path.islink(f):
            continue
        target = os.readlink(f)
        print target

def find_primary_data(datadir):
    """
    Look for primary data files (csfasta, qual and fastq)
    """
    for f in DataDir(datadir).list_files(extensions=('csfasta',
                                                     'qual',
                                                     'fastq')):
        print "%s" % os.path.relpath(f,datadir)

def find_duplicates(*dirs):
    """
    Locate duplicated files across multiple dirs

    TODO
    - not implemented; use something like duff?
    - also needs to deal with comparing content of compressed
      files with uncompressed files?
    """
    raise NotImplementedError("duplicates command not implemented")

#######################################################################
# Main program
#######################################################################

if __name__ == '__main__':

    # Set up the command line parser
    p = CommandParser(description="Utility for archiving and curating "
                      "NGS sequence data.",
                      version="%prog "+__version__)
    # Add commands
    #
    # Info
    p.add_command('info',help="Get information about a data dir",
                  usage='%prog info DIR',
                  description="Print information about DIR and its "
                  "contents.")
    #
    # Stage data
    p.add_command('stage',help="Make a staging copy of data",
                  usage='%prog stage DIR STAGING_DIR',
                  description="Copy DIR to STAGING_DIR and set up for "
                  "archiving and curation.")
    #
    # List primary data
    p.add_command('primary_data',help="List primary data files",
                  usage='%prog stage DIR',
                  description="List the primary data files found in DIR.")
    #
    # Find duplicates
    p.add_command('duplicates',help="Find duplicated files",
                  usage='%prog duplicates DIR [DIR ...]',
                  description="Look for duplicated files across one or "
                  "more data directories")
    #
    # Compress files
    p.add_command('compress',help="Compress data files",
                  usage='%prog compress DIR EXT [EXT..]',
                  description="Compress data files in DIR with matching "
                  "file extensions using bzip2.")
    p.parser_for('compress').add_option('--dry-run',action='store_true',
                                        dest='dry_run',default=False,
                                        help="Report actions but don't "
                                        "perform them")
    #
    # Look for related directories
    p.add_command('related',help="Locate related data directories",
                  usage='%prog related DIR SEARCH_DIR [SEARCH_DIR ...]',
                  description="Look for related directories under one "
                  "or more search directories")
    # Process command line
    cmd,options,args = p.parse_args()

    # Report name and version
    print "%s version %s" % (os.path.basename(sys.argv[0]),__version__)

    if cmd == 'info':
        if len(args) != 1:
            sys.stderr.write("Need to supply a data dir\n")
            sys.exit(1)
        data = DataDir(args[0]).info()
    elif cmd == 'stage':
        if len(args) != 2:
            sys.stderr.write("Need to supply a data dir and staging location\n")
            sys.exit(1)
        stage_data(args[0],args[1])
    elif cmd == 'primary_data':
        find_primary_data(args[0])
    elif cmd == 'duplicates':
        find_duplicates(*args)
    elif cmd == 'compress':
        if len(args) < 2:
            sys.stderr.write("Need to supply a data dir and at least "
                             "one extension\n")
            sys.exit(1)
        compress_files(args[0],args[1:],dry_run=options.dry_run)
    elif cmd == 'related':
        find_related(args[0])
        
