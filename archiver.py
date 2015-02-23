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
import bz2
import bcftbx.utils as utils
import bcftbx.Md5sum as Md5sum
from bcftbx.cmdparse import CommandParser
from auto_process_ngs import applications

__version__ = '0.0.2'

#######################################################################
# Classes
#######################################################################

class DataDir:
    """
    Class for interrogating and manipulating an NGS data dir

    TODO:
    - add a subdirectory to cache checksums etc
    - store data about platform, year?
    - store and update arbitrary metadata, and
      dump this to a README or other info file for
      curation

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
        print "Dir   : %s" % self._dirn
        # Report total size
        size = get_size(self._dirn)
        print "Size  : %s (%s)" % (utils.format_file_size(size),
                                 utils.format_file_size(size,'K'))
        # Get users and groups
        users = []
        groups = []
        for f in self.walk():
            path = utils.PathInfo(f)
            if path.user not in users:
                users.append(path.user)
            if path.group not in groups:
                groups.append(path.group)
        print "Users : %s" % ', '.join([str(u) for u in users])
        print "Groups: %s" % ', '.join([str(g) for g in groups])

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

def get_size(f,block_size=1):
    """Return size of a file or directory

    By default returns file sizes in bytes (block_size of 1);
    if a file is supplied then returns the size of the file,
    if a directory is supplied then returns the total size of
    all files in the directory (and its subdirectories).
    
    """
    if os.path.isfile(f):
        size = os.lstat(f).st_size
    else:
        size = os.lstat(f).st_size
        for dirpath,dirnames,filenames in os.walk(f):
            for d in dirnames:
                size_incr = os.lstat(os.path.join(dirpath,d)).st_size
                size += size_incr
                ##print "%s: %d" % (os.path.join(dirpath,d),size_incr)
            for f in filenames:
                size_incr = get_size(os.path.join(dirpath,f),block_size=block_size)
                size += size_incr
                ##print "%s: %d" % (os.path.join(dirpath,f),size_incr)
    # Return number of blocks, rounded
    # up or down to nearest integer
    return int(round(float(size)/float(block_size)))

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
    external_dirs = []
    for f in DataDir(datadir).walk():
        # Test if it's a link
        try:
            ln = utils.Symlink(f)
        except Exception:
            # Not a symlink
            continue
        resolved_target = ln.resolve_target()
        if os.path.relpath(resolved_target,datadir).startswith('..'):
            if os.path.isdir(resolved_target):
                d = resolved_target
            else:
                d = os.path.dirname(resolved_target)
            if d not in external_dirs:
                external_dirs.append(d)
    if external_dirs:
        for d in external_dirs:
            print d
    else:
        print "No related directories detected"

def find_primary_data(datadir):
    """
    Look for primary data files (csfasta, qual and fastq)
    """
    for f in DataDir(datadir).list_files(extensions=('csfasta',
                                                     'qual',
                                                     'fastq')):
        if os.path.islink(f):
            lnk=" *"
        else:
            lnk=''
        print "%s%s" % (os.path.relpath(f,datadir),lnk)

def find_symlinks(datadir):
    """
    Examine symlinks and find those pointing outside this dir

    TODO:
    - functionality not implemented, should be just 'symlinks'?

    """
    for f in DataDir(datadir).walk():
        # Test if it's a link
        try:
            ln = utils.Symlink(f)
        except Exception:
            # Not a symlink
            continue
        resolved_target = ln.resolve_target()
        external_target = os.path.relpath(resolved_target,datadir).startswith('..')
        print "%s %s\n\t-> %s\n\t-> %s" % ('[E]' if external_target else '   ',
                                           os.path.relpath(f,datadir),
                                           ln.target,
                                           resolved_target)

def find_duplicates(*dirs):
    """
    Locate duplicated files across multiple dirs

    TODO
    - not implemented; use something like duff?
    - also needs to deal with comparing content of compressed
      files with uncompressed files?
    """
    # Generate MD5 checksums
    checksums = {}
    for d in dirs:
        for f in DataDir(d).walk():
            # Ignore links
            if os.path.islink(f):
                continue
            # Check for compressed file
            ext,compression = get_file_extensions(f)
            if compression is None:
                chksum = Md5sum.md5sum(f)
            else:
                if compression == 'bz2':
                    fp = bz2.BZ2File(f)
                    chksum = Md5sum.md5sum(f)
                else:
                    raise NotImplementedError("%s: duplicates command not yet "
                                              "implemented for this type of "
                                              "compression" % f)
            # Store checksum info
            if chksum not in checksums:
                checksums[chksum] = []
            checksums[chksum].append(f)
    # Report checksums that have multiple entries
    n_duplicates = 0
    for chksum in checksums:
        if len(checksums[chksum]) > 1:
            print "%s\t%s" % (chksum,','.join(checksums[chksum]))
            n_duplicates += 1
    # Finished
    if not n_duplicates:
        print "No duplicates found"
    else:
        print "%d duplicated checksums identified" % (n_duplicates)

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
                  usage='%prog primary_data DIR',
                  description="List the primary data files found in DIR.")
    #
    # List symlinks
    p.add_command('symlinks',help="List symlinks",
                  usage='%prog symlinks DIR',
                  description="List the symbolic links found in DIR.")
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
    elif cmd == 'symlinks':
        find_symlinks(args[0])
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
        
