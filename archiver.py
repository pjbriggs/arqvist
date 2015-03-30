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
import gzip
import bcftbx.utils as utils
from bcftbx.cmdparse import CommandParser
from auto_process_ngs import applications
from curatus import get_version
from curatus.core import DataDir,ArchiveSymlink
from curatus.core import get_file_extensions
from curatus.solid import SolidDataDir

__version__ = get_version()

#######################################################################
# Functions
#######################################################################

def stage_data(datadir,staging_dir):
    """
    Make a staging copy of data dir
    """
    DataDir(datadir).copy_to(staging_dir)

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
    external_dirs = DataDir(datadir).list_related_dirs()
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
                                                     'fastq',
                                                     'xsq',)):
        if os.path.islink(f):
            lnk=" *"
        else:
            lnk=''
        print "%s%s" % (os.path.relpath(f,datadir),lnk)

def find_symlinks(datadir):
    """
    Examine symlinks and find those pointing outside this dir
    """
    for f in DataDir(datadir).list_symlinks():
        # Get link target and resolve to an absolute path
        ln = ArchiveSymlink(f)
        resolved_target = ln.resolve_target()
        # Check link status
        absolute = ln.is_absolute
        broken = ln.is_broken
        alt_target = ln.alternative_target
        external = ln.external_to(datadir)
        # Assemble status
        status = ln.classifier
        if external:
            status = 'E' + status
        else:
            status = '-' + status
        print "[%s]\t%s" % (status,os.path.relpath(f,datadir))
        print "\t->: %s" % ln.target
        print "\t->: %s" % resolved_target
        print "\t->: %s" % alt_target

def find_md5sums(datadir,outfile=None):
    """
    Print MD5 sums for files in data directory
    """
    dd = DataDir(datadir)
    dd.md5sums()
    if outfile is None:
        fp = sys.stdout
    else:
        fp = open(outfile,'w')
    for f in dd.files():
        if f.is_link or f.is_dir:
            # Skip links and directories
            continue
        fp.write("%s  %s\n" % (f.md5,f.relpath(datadir)))
    if outfile is None:
        fp.close()

def find_duplicates(*dirs):
    """
    Locate duplicated files across multiple dirs

    TODO
    - not implemented; use something like duff?
    - also needs to deal with comparing content of compressed
      files with uncompressed files?
    """
    # Look for duplicated MD5 checksums
    checksums = {}
    for d in dirs:
        dd = DataDir(d)
        # Generate Md5 checksums
        print "Acquiring MD5 sums for %s" % dd.path
        dd.md5sums()
        for f in dd.files():
            if f.is_link or f.is_dir:
                # Skip links and directories
                continue
            chksum = f.uncompressed_md5
            # Store checksum info
            if chksum not in checksums:
                checksums[chksum] = []
            checksums[chksum].append(f.path)
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

def find_tmp_files(datadir):
    """
    Report temporary files/directories

    """
    nfiles = 0
    total_size = 0
    for f in DataDir(datadir).list_temp():
        size = get_size(f)
        total_size += size
        nfiles += 1
        print "%s\t%s" % (os.path.relpath(f,datadir),
                          utils.format_file_size(size))
    if not nfiles:
        print "No files or directories found"
        return
    print "%d found, total size: %s" % (nfiles,utils.format_file_size(total_size))

def list_files(datadir,extensions=None,owners=None,groups=None,compression=None,
               subdir=None,sort_keys=None,min_size=None):
    """
    Report files owned by specific users and/or groups
    """
    nfiles = 0
    total_size = 0
    if min_size: min_size = convert_size(min_size)
    for f in DataDir(datadir).files(extensions=extensions,
                                    compression=compression,
                                    owners=owners,groups=groups,
                                    subdir=subdir,
                                    sort_keys=sort_keys):
        if min_size and f.size < min_size: continue
        total_size += f.size
        nfiles += 1
        print "%s\t%s\t%s%s\t%s" % (f.user,f.group,
                                    f.relpath(datadir),
                                    f.classifier,
                                    utils.format_file_size(f.size))
    if not nfiles:
        print "No files found"
        return
    print "%d found, total size: %s" % (nfiles,utils.format_file_size(total_size))

def report_solid(datadir):
    """
    Try to group primary data and sort into samples etc for SOLiD runs
    """
    SolidDataDir(datadir).report()

def match_solid_primary_data(datadir,*dirs):
    """
    Try to match up SOLiD datasets with links from analysis dirs
    """
    print "Collecting data from %s" % os.path.basename(datadir)
    solid_data = SolidDataDir(datadir)
    if len(solid_data.libraries) == 0:
        print "No libraries found"
        return
    # Collect all symlink targets
    symlinks = {}
    for dirn in dirs:
        print "Collecting symlinks from %s" % os.path.basename(datadir)
        for f in DataDir(dirn).list_symlinks():
            target = ArchiveSymlink(f).resolve_target()
            if target not in symlinks:
                symlinks[target] = []
            symlinks[target].append((f,target))
    # Check primary data files against links
    lib_links = {}
    for lib in solid_data.libraries:
        ##print "Examining library '%s'..." % lib.name
        lib_links[lib.name] = { 'library': lib,
                                'file_sets'  : [] }
        for fset in lib.get_file_sets():
            for f in fset.files:
                if f.path in symlinks:
                    ##print "- %s is linked" % f.path
                    lib_links[lib.name]['file_sets'].append(fset)
                    break
    # Report
    print "Primary data links from analysis dir:"
    for group in solid_data.library_groups:
        print "* %s *" % group
        for lib in solid_data.libraries_in_group(group):
            msg = []
            # Check that there are references
            file_sets = lib_links[lib.name]['file_sets']
            if file_sets:
                # Reanalyse: check how many file sets are referenced
                if len(file_sets) == 1:
                    # Single file set referenced
                    # Check that each file is referenced
                    ok = True
                    for f in file_sets[0].files:
                        if f.path not in symlinks:
                            ok = False
                            break
                    if ok:
                        msg.append("ok")
                    else:
                        msg.append("partially referenced")
                else:
                    # Multiple file pairs referenced
                    msg.append("multiple file pairs referenced")
            else:
                # Nothing links to this library
                msg.append("no references")
            # Print message
            print "- %s:\t%s" % (lib.name,'; '.join(msg))

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
    # Initialise a cache subdirectory
    p.add_command('init_cache',help="Initialise a cache subdirectory",
                  usage='%prog init_cache DIR',
                  description="Create a cache subdirectory under DIR "
                  "(if one doesn't already exist) and use this to store "
                  "information such as MD5 sums for quick lookup.")
    #
    # List files
    p.add_command('list_files',help="List files filtered by various criteria",
                  usage='%prog list_files OPTIONS DIR',
                  description="List files under DIR filtered by criteria "
                  "specified by one or more OPTIONS.")
    p.parser_for('list_files').add_option('--extensions',action='store',
                                          dest='extensions',default=None,
                                          help="List files with matching "
                                          "extensions")
    p.parser_for('list_files').add_option('--compression',action='store',
                                          dest='compression',default=None,
                                          help="List files with matching "
                                          "compression extensions")
    p.parser_for('list_files').add_option('--owners',action='store',
                                          dest='owners',default=None,
                                          help="List files owned by "
                                          "specified users")
    p.parser_for('list_files').add_option('--groups',action='store',
                                          dest='groups',default=None,
                                          help="List files assigned to "
                                          "specified groups")
    p.parser_for('list_files').add_option('--subdir',action='store',
                                          dest='subdir',default=None,
                                          help="List files in "
                                          "subdirectory SUBDIR under "
                                          "DIR")
    p.parser_for('list_files').add_option('--sort',action='store',
                                          dest='sortkeys',default=None,
                                          help="List files sorted in "
                                          "order according to one or "
                                          "more SORTKEYS ('size',...)")
    p.parser_for('list_files').add_option('--minsize',action='store',
                                          dest='min_size',default=None,
                                          help="Only report files with "
                                          "size greater than MIN_SIZE")
    #
    # List primary data
    p.add_command('primary_data',help="List primary data files",
                  usage='%prog primary_data DIR',
                  description="List the primary data files found in DIR.")
    #
    # List primary data (SOLiD)
    p.add_command('report_solid',help="List primary data files for SOLiD",
                  usage='%prog report_solid DIR',
                  description="List the SOLiD primary data files found in DIR.")
    #
    # Match primary data to links from analysis dir (SOLiD)
    p.add_command('match_solid',help="Find SOLiD datasets linked from analysis dir",
                  usage='%prog match_solid DIR ANALYSIS_DIR',
                  description="Determine which SOLiD datasets found in DIR "
                  "are also linked from ANALYSIS_DIR.")
    #
    # List symlinks
    p.add_command('symlinks',help="List symlinks",
                  usage='%prog symlinks DIR',
                  description="List the symbolic links found in DIR.")
    #
    # Md5sums
    p.add_command('md5sums',help="Generate MD5 checksums",
                  usage='%prog md5sums DIR',
                  description="Generate MD5 checksums for all files "
                  "in DIR. Symlinks are not followed.")
    p.parser_for('md5sums').add_option('-o',action='store',
                                       dest='outfile',default=None,
                                       help="Write MD5 sums to OUTFILE (otherwise "
                                       "writes to stdout)")
    #
    # Find duplicates
    p.add_command('duplicates',help="Find duplicated files",
                  usage='%prog duplicates DIR [DIR ...]',
                  description="Look for duplicated files across one or "
                  "more data directories")
    #
    # Find duplicates
    p.add_command('temp_files',help="Find temporary files & directories",
                  usage='%prog temp_files DIR [DIR ...]',
                  description="Look for temporary files and directories "
                  "in DIR.")
    #
    # Look for related directories
    p.add_command('related',help="Locate related data directories",
                  usage='%prog related DIR SEARCH_DIR [SEARCH_DIR ...]',
                  description="Look for related directories under one "
                  "or more search directories.")
    #
    # Set permissions
    p.add_command('set_permissions',help="Set permissions and ownership",
                  usage='%prog set_permissions OPTIONS DIR',
                  description="Set the permissions and ownership of DIR "
                  "according to the supplied options.")
    p.parser_for('set_permissions').add_option('--chmod',action='store',
                                               dest='mode',default=None,
                                               help="Set file permissions on "
                                               "files to those specified by "
                                               "MODE")
    p.parser_for('set_permissions').add_option('--group',action='store',
                                               dest='group',default=None,
                                               help="Set group ownership on "
                                               "files to GROUP")
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
    # Process command line
    cmd,options,args = p.parse_args()

    # Report name and version
    print "%s version %s" % (os.path.basename(sys.argv[0]),__version__)

    if cmd == 'info':
        if len(args) != 1:
            sys.stderr.write("Need to supply a data dir\n")
            sys.exit(1)
        DataDir(args[0]).info()
    elif cmd == 'stage':
        if len(args) != 2:
            sys.stderr.write("Need to supply a data dir and staging location\n")
            sys.exit(1)
        stage_data(args[0],args[1])
    elif cmd == 'init_cache':
        DataDir(args[0]).init_cache()
    elif cmd == 'list_files':
        list_files(args[0],
                   extensions=(None if options.extensions is None \
                               else options.extensions.split(',')),
                   owners=(None if options.owners is None \
                           else options.owners.split(',')),
                   groups=(None if options.groups is None \
                           else options.groups.split(',')),
                   compression=(None if options.compression is None \
                                else options.compression.split(',')),
                   subdir=options.subdir,
                   sort_keys=(None if options.sortkeys is None \
                              else options.sortkeys.split(',')),
                   min_size=options.min_size)
    elif cmd == 'primary_data':
        find_primary_data(args[0])
    elif cmd == 'report_solid':
        report_solid(args[0])
    elif cmd == 'match_solid':
         match_solid_primary_data(args[0],args[1])
    elif cmd == 'symlinks':
        find_symlinks(args[0])
    elif cmd == 'md5sums':
        find_md5sums(args[0],options.outfile)
    elif cmd == 'duplicates':
        find_duplicates(*args)
    elif cmd == 'temp_files':
        find_tmp_files(args[0])
    elif cmd == 'set_permissions':
        DataDir(args[0]).set_permissions(mode=options.mode,
                                         group=options.group)
    elif cmd == 'compress':
        if len(args) < 2:
            sys.stderr.write("Need to supply a data dir and at least "
                             "one extension\n")
            sys.exit(1)
        compress_files(args[0],args[1:],dry_run=options.dry_run)
    elif cmd == 'related':
        find_related(args[0])
        
