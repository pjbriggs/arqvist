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
import itertools
import bz2
import gzip
import bcftbx.utils as utils
import bcftbx.Md5sum as Md5sum
import bcftbx.SolidData as solid
from bcftbx.cmdparse import CommandParser
from auto_process_ngs import applications

__version__ = '0.0.23'

NGS_FILE_TYPES = ('fa',
                  'fasta',
                  'csfasta',
                  'qual',
                  'fastq',
                  'gff',
                  'gff3',
                  'gtf',
                  'sam',
                  'bam',
                  'bai',
                  'bed',
                  'bd',
                  'bdg',
                  'bw',
                  'xsq',
                  'xls')

#######################################################################
# Classes
#######################################################################

class ArchiveFile(utils.PathInfo):
    """
    Class for storing information about a file

    """
    def __init__(self,filen):
        """
        Create and populate a new ArchiveFile instance
        """
        utils.PathInfo.__init__(self,filen)
        self.size = os.lstat(filen).st_size
        self.timestamp = self.mtime
        self.ext,self.compression = get_file_extensions(filen)
        self.md5 = None
        self.uncompressed_md5 = None

    @property
    def basename(self):
        """
        Return the basename of the file path
        """
        return os.path.basename(self.path)

    @property
    def classifier(self):
        """
        Return classifier for an ArchiveFile object

        Return an indicator consistent with 'ls -F' depending
        on file type:

        / indicates a directory
        @ indicates a link
        * indicates an executable

        Empty string indicates a regular file.
        """
        if self.is_link:
            return '@'
        elif self.is_dir:
            return os.sep
        elif self.is_executable:
            return '*'
        return ''

    def __repr__(self):
        return self.path

class ArchiveSymlink(utils.Symlink):
    """
    Class for interrogating and modifying a symlink

    """
    def __init__(self,path):
        utils.Symlink.__init__(self,path)

    def external_to(self,dirn):
        """
        Check if target is 'external' to the supplied directory
        """
        return os.path.relpath(self.resolve_target(),dirn).startswith('..')

    def rebase(self,old_base,new_base):
        """
        Update the target of an absolute link by replacing leading part
        """
        if not self.is_absolute:
            return
        dirn = os.path.dirname(self.target)
        if dirn.startswith(old_base):
            dirn = dirn.replace(old_base,new_base)
        new_target = os.path.join(dirn,os.path.basename(self.target))
        self.update_target(new_target)

    def make_relative(self):
        """
        Convert an absolute link to a relative link
        """
        if not self.is_absolute:
            return
        new_target = os.path.relpath(self.resolve_target(),
                                     os.path.dirname(self._abspath))
        self.update_target(new_target)

    @property
    def alternative_target(self):
        """
        Return alternative target for broken link

        If link is broken then check for a link to
        an uncompressed file if there is a compressed
        file with the same path, or for a link to a
        compressed file if there is an uncompressed
        version.

        Returns None if link is not broken or if
        there is no alternative target.

        """
        if not self.is_broken:
            return None
        # Check for alternatives
        target = self.resolve_target()
        for ext in ('.gz','.bz2'):
            alt_target = target + ext
            if os.path.exists(alt_target):
                return alt_target
        # Nothing found
        return None

    @property
    def classifier(self):
        """Return classifier for an ArchiveSymlink object

        Return an indicator string with the following
        components:

        A = absolute link target
        r = relative target
        X = broken link
        x = broken link with alternative target

        Empty string indicates a regular file.
        """
        classifier = []
        if self.is_absolute:
            classifier.append('A')
        else:
            classifier.append('r')
        if self.is_broken:
            if not self.alternative_target:
                classifier.append('X')
            else:
                classifier.append('x')
        else:
            classifier.append('-')
        return ''.join(classifier)

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
        self._files = []
        # Collect list of files
        for d in os.walk(self._dirn):
            if os.path.basename(d[0]) == '.archiver':
                # Skip the cache directory
                continue
            for f in d[1]:
                if f == '.archiver':
                    # Skip the cache directory
                    continue
                self._files.append(ArchiveFile(os.path.normpath(os.path.join(d[0],f))))
            for f in d[2]:
                self._files.append(ArchiveFile(os.path.normpath(os.path.join(d[0],f))))
        # Update cache (if present)
        self.update_cache()

    def __del__(self):
        self.write_cache()

    @property
    def has_cache(self):
        """
        Check if a cache directory exists
        """
        cachedir = os.path.join(self._dirn,'.archiver')
        return os.path.exists(cachedir)

    def update_cache(self):
        """
        Update the cache of file information
        """
        # Convenience variable to save lookup time
        dirn = self._dirn
        # Cache directory
        cachedir = os.path.join(dirn,'.archiver')
        if not os.path.exists(cachedir):
            return
        # MD5 information
        md5info = os.path.join(cachedir,'md5info')
        data = {}
        if os.path.exists(md5info):
            # Read in cached data
            with open(md5info,'r') as fp:
                for line in fp:
                    items = line.rstrip('\n').split('\t')
                    data[items[0]] = {
                        'size': int(items[1]),
                        'time': float(items[2]),
                        'md5' : items[3],
                        'uncompressed_md5': items[4]
                    }
            # Verify and remove outdated items
            # i.e. those which are missing, or where size or timestamp
            # has changed
            verified = {}
            for filen in self._files:
                path = filen.relpath(dirn)
                ##print "*** %s ***" % path
                try:
                    f = data[path]
                    ##print "Sizes:\t%s\t%s" % (filen.size,f['size'])
                    ##print "Times:\t%s\t%s" % (filen.timestamp,f['time'])
                    if f['size'] == filen.size and f['time'] == filen.timestamp:
                        # Size and timestamp match
                        filen.md5 = f['md5'] if f['md5'] else None
                        filen.uncompressed_md5 = f['uncompressed_md5'] \
                                                 if f['uncompressed_md5'] else None
                    else:
                        # Size or timestamp mismatch
                        print "%s: size and/or timestamp differs from cache" % path
                        del(data[path])
                except KeyError:
                    print "%s: missing from cache" % path

    def write_cache(self):
        """
        Dump the cache of file information to disk
        """
        # Convenience variable to save lookup time
        dirn = self._dirn
        # Cache directory
        cachedir = os.path.join(dirn,'.archiver')
        if not os.path.exists(cachedir):
            return
        # MD5 information
        md5info = os.path.join(cachedir,'md5info')
        with open(md5info,'w') as fp:
            for f in self._files:
                fp.write("%s\t%s\t%s\t%s\t%s\n" % \
                         (f.relpath(dirn),
                          f.size,
                          f.timestamp,
                          (f.md5 if f.md5 else ''),
                          (f.uncompressed_md5 if f.uncompressed_md5 else '')))

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
        for f in itertools.ifilter(lambda x: not x.is_dir,self._files):
            yield f.path

    def init_cache(self):
        """
        Initialise a cache subdirectory
        """
        cachedir = os.path.join(self._dirn,'.archiver')
        if os.path.exists(cachedir):
            return
        os.mkdir(cachedir)

    def files(self,extensions=None,owners=None,groups=None,compression=None,
              subdir=None,sort_keys=None):
        """
        Return a (filtered) list of ArchiveFile objects
        """
        if extensions:
            files = [f for f in itertools.ifilter(lambda x: x.ext.lower() in extensions,
                                                  self._files)]
        else:
            files = self._files
        if compression:
            files = [f for f in itertools.ifilter(lambda x: x.compression in compression,
                                                  self._files)]
        if owners:
            files = [f for f in itertools.ifilter(lambda x: str(x.user) in owners,files)]
        if groups:
            files = [f for f in itertools.ifilter(lambda x: str(x.group) in groups,files)]
        if subdir:
            files = [f for f in itertools.ifilter(lambda x:
                                                  x.relpath(self._dirn).startswith(subdir),
                                                  files)]
        if sort_keys:
            for key in sort_keys:
                if key == 'size':
                    files = sorted(files,key=lambda f: f.size)
                else:
                    raise NotImplementedError("Sort on '%s' not implemented" % key)
        return files

    def list_files(self,extensions=None,owners=None,groups=None,compression=None,
                   subdir=None,sort_keys=None):
        """
        Return a (filtered) list of file paths
        """
        return [f.path for f in self.files(extensions=extensions,
                                           owners=owners,groups=groups,
                                           compression=compression,
                                           subdir=subdir,
                                           sort_keys=sort_keys)]

    def list_symlinks(self):
        """
        Return a list of symlinks
        """
        return [f.path for f in itertools.ifilter(lambda x: x.is_link,self._files)]

    def list_temp(self):
        """
        Return a list of temporary files/directories
        """
        return [f.path for f in itertools.ifilter(lambda x: bool(x.basename.count('tmp')),
                                                  self._files)]

    def list_related_dirs(self):
        """
        Examine symlinks and find those pointing to external directories
        """
        external_dirs = []
        for f in self.list_symlinks():
            ln = ArchiveSymlink(f)
            resolved_target = ln.resolve_target()
            if ln.external_to(self._dirn):
                if os.path.isdir(resolved_target):
                    d = resolved_target
                else:
                    d = os.path.dirname(resolved_target)
                if d not in external_dirs:
                    external_dirs.append(d)
        return external_dirs

    def md5sums(self):
        """
        Generate MD5sums
        """
        for f in self._files:
            if f.is_link or f.is_dir:
                # Ignore links or directories
                continue
            if f.md5 is None:
                # Generate MD5 sum
                f.md5 = Md5sum.md5sum(f.path)
            if f.uncompressed_md5 is None:
                # Generate MD5 for uncompressed contents
                if f.compression is None:
                    f.uncompressed_md5 = f.md5
                elif f.compression == 'bz2':
                    fp = bz2.BZ2File(f.path,'r')
                    f.uncompressed_md5 = Md5sum.md5sum(fp)
                elif f.compression == 'gz':
                    fp = gzip.GzipFile(f.path,'rb')
                    f.uncompressed_md5 = Md5sum.md5sum(fp)
                else:
                    raise NotImplementedError("%s: md5sums not implemented for "
                                              "compression type" % f)

    def set_permissions(self,mode=None,group=None):
        """
        Set permissions and group ownership on files
        """
        if group:
            gid = utils.get_gid_from_group(group)
            print "Group %s = %s" % (group,gid)
        for f in self._files:
            f.chown(group=gid)
            os.system('chmod %s %s' % (mode,f.path))

    def info(self):
        """
        Report information about the directory 
        """
        # Collect total size, users etc
        size = sum([f.size for f in self._files])
        users = set([f.user for f in self._files])
        groups = set([f.group for f in self._files])
        nfiles = len(filter(lambda x: not x.is_dir,self._files))
        compression = set([f.compression for f in self._files])
        compression.discard('')
        extensions = set([f.ext for f in filter(lambda x: x.is_file and x.ext.lower()
                                                in NGS_FILE_TYPES,self._files)])
        extensions.discard('')
        # Top-level directories
        top_level = utils.list_dirs(self._dirn)
        # Oldest file modification time
        oldest = reduce(lambda x,y: x if x.timestamp < y.timestamp else y,self._files)
        newest = reduce(lambda x,y: x if x.timestamp > y.timestamp else y,self._files)
        # Uneadable/unwriteable files
        has_unreadable = reduce(lambda x,y: x or not y.is_readable,self._files,False)
        has_group_unreadable = reduce(lambda x,y: x or not y.is_group_readable,self._files,False)
        has_group_unwritable = reduce(lambda x,y: x or not y.is_group_writable,self._files,False)
        # Report information
        print "Dir   : %s" % self._dirn
        print "Size  : %s (%s)" % (utils.format_file_size(size),
                                   utils.format_file_size(size,'K'))
        print "Has cache: %s" % ('yes' if self.has_cache else 'no')
        print "#files: %d" % nfiles
        print "File types: %s" % ', '.join([str(ext) for ext in extensions])
        print "Compression types: %s" % ', '.join([str(c) for c in compression])
        print "Users : %s" % ', '.join([str(u) for u in users])
        print "Groups: %s" % ', '.join([str(g) for g in groups])
        print "Oldest: %s %s" % (oldest.datetime.ctime(),oldest.relpath(self._dirn))
        print "Newest: %s %s" % (newest.datetime.ctime(),newest.relpath(self._dirn))
        # Top-level subdirectories
        print "Top-level subdirectories:"
        for subdir in top_level:
            subdir_size = os.path.join(self._dirn,subdir)
            subdir_files = self.files(subdir=subdir)
            subdir_users = set([f.user for f in subdir_files])
            extensions = set([f.ext for f in filter(lambda x: x.is_file and x.ext.lower()
                                                    in NGS_FILE_TYPES,subdir_files)])
            usr_unreadable = reduce(lambda x,y: x and not y.is_readable,subdir_files,False)
            grp_unreadable = reduce(lambda x,y: x or not y.is_group_readable,subdir_files,False)
            grp_unwritable = reduce(lambda x,y: x or not y.is_group_writable,subdir_files,False)
            print "- %s/\t%d\t%s\t%s\t%s\t%s" % (subdir,
                                                 len(subdir_files),
                                                 utils.format_file_size(get_size(subdir_size)),
                                                 ','.join(extensions),
                                                 ','.join([str(u) for u in subdir_users]),
                                                 'u%s,g%s%s' % (('-' if usr_unreadable else 'r'),
                                                                ('-' if grp_unreadable else 'r'),
                                                                ('-' if grp_unwritable else 'w')))
        # File permissions
        print "File permissions:"
        print "- unreadable by owner: %s" % ('yes' if has_unreadable else 'no')
        print "- unreadable by group: %s" % ('yes' if has_group_unreadable else 'no')
        print "- unwritable by group: %s" % ('yes' if has_group_unwritable else 'no')
        print "#Temp files: %d" % len(self.list_temp())
        # Related directories
        ## commented out for now - for solid data this can create a long
        ## but not very useful list
        #related = self.list_related_dirs()
        #print "Related directories:"
        #if related:
        #    for d in related:
        #        print "- %s" % d
        #else:
        #    print "- None found"

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

class SolidLibrary:
    """
    Utility class for holding data about a SOLiD library
    """
    def __init__(self,sample,library):
        self._sample_name = ('' if sample is None else str(sample))
        self._library_name = ('' if library is None else str(library))
        self._files = {}
    @property
    def sample_name(self):
        """
        Return sample name for this library
        """
        return self._sample_name
    @property
    def library_name(self):
        """
        Return library name for this library
        """
        return self._library_name
    @property
    def name(self):
        """
        Return the short name for this library
    
        This is either the supplied library name, or
        the sample name if the library name was empty.
        """
        if not self._library_name:
            return self._sample_name
        return self._library_name
    @property
    def fullname(self):
        """
        Return the full name for this library

        This is a name made up from the supplied
        sample and library names concatenated with a
        slash ('/') e.g. SAMPLE/LIBRARY

        If either component is null then the full
        name will be of the form /LIBRARY or SAMPLE/.
        """
        return "%s/%s" % (self._sample_name,self._library_name)
    @property
    def group(self):
        """
        Return the group identifier for the library
        """
        return utils.extract_initials(self.name)
    @property
    def timestamps(self):
        """
        Return a list of associated timestamps
        """
        return sorted(self._files.keys())
    def add_file_pair(self,timestamp,file_pair):
        """
        Associate a pair of files with a timestamp

        file_pair should be a tuple or list of
        ArchiveFile objects.

        The files can be accessed using the
        'get_file_pairs' method.
        """
        if timestamp not in self.timestamps:
            self._files[timestamp] = []
        self._files[timestamp].append(list(file_pair))
    def get_file_pairs(self,timestamp=None):
        """
        Return file pairs

        If timestamp is not None then return
        just the file pairs associated with that
        timestamp. Otherwise return all file
        pairs.
        """
        if timestamp is not None:
            return self._files[timestamp]
        else:
            file_pairs = []
            for ts in self.timestamps:
                for p in self.get_file_pairs(ts):
                    file_pairs.append(p)
            return file_pairs

class SolidDataDir(DataDir):
    """
    Subclass of DataDir with additional methods specifically for
    examining SOLiD data
    """
    def __init__(self,dirn):
        # Init base class
        DataDir.__init__(self,dirn)
        self._primary_data = None
        self._libraries = None
        self._library_names = None
        self._library_groups = None
        self._populate()

    def _populate(self):
        """
        Acquire data specifically for SOLiD
        """
        # Get csfasta and qual files
        primary_data = self.files(extensions=('csfasta','qual',))
        # Filter out links and obvious analysis products
        # e.g.
        # U2OS_input/FOXM1_filtered_T_F3.csfasta.bz2
        # U2OS_input/FOXM1_filtered_U_F3_QV.qual.bz2
        primary_data = filter(lambda f: not f.is_link,primary_data)
        primary_data = filter(lambda f: f.basename.count('_T_F3') < 1
                              and f.basename.count('_U_F3') < 1,
                              primary_data)
        # Sort into csfasta/qual pairs
        pairs = {}
        for f in primary_data:
            name = strip_extensions(f.relpath(self._dirn))
            if f.ext == 'qual':
                name = name.replace('_QV','')
            if name not in pairs:
                pairs[name] = []
            pairs[name].append(f)
        # Check for and report 'bad' pairs
        for name in pairs:
            pair = pairs[name]
            if len(pair) != 2:
                logging.warning("Wrong number of files in \"pair\" '%s' (%d)" %
                                (name,len(pair)))
            for ext in ('csfasta','qual'):
                if len(filter(lambda f: f.ext == ext,pair)) > 1:
                    logging.warning("Multiple %s files in \"pair\" '%s'" % (ext,name))
        # Extract sample/library names and timestamps
        # For SOLiD the top-level is a 'sample', which may be
        # split into multiple 'libraries'
        libraries = []
        for name in pairs:
            # Example name formats
            # LH_POOL/results.F1B1/libraries/LH1/primary.20111208144829752/reads/solid0127_20111207_FRAG_BC_LH_POOL_BC_LH1
            # ZD_hu/results.F1B1/primary.20091220022109452/reads/solid0424_20091214_ZD_hu_F3
            # SH_JC1_pool/results.F1B1/libraries_MM2/JC_SEQ30/primary.20120125063517232/reads/solid0127_20120117_PE_BC_SH_JC1_pool_F5-BC_JC_SEQ30
            fields = name.split(os.sep)
            # Attempt to extract from the path
            sample = None
            library = None
            try:
                i = fields.index('results.F1B1')
                sample = fields[i-1]
                fields = fields[i+1:]
            except ValueError:
                pass
            for i,field in enumerate(fields):
                if field.startswith('libraries'):
                    library = fields[i+1]
                    fields = fields[i+2:]
                    break
            # Extract timestamp
            timestamp = solid.extract_library_timestamp(name)
            if timestamp is None:
                timestamp = 'unknown'
            # Store the library
            existing_lib = False
            lib = SolidLibrary(sample,library)
            for l in libraries:
                if l.fullname == lib.fullname:
                    lib = l
                    existing_lib = True
                    break
            if not existing_lib:
                libraries.append(lib)
            lib.add_file_pair(timestamp,pairs[name])
        # Detect and discard libraries that don't represent
        # 'real' datasets
        self._libraries = []
        for lib in libraries:
            if lib.library_name in ('missing-bc','missing-f3','unassigned'):
                print "Discarding %s" % lib.fullname
                continue
            elif not lib.library_name:
                # Sample name with empty library name
                # See if there are other libraries with the
                # same sample name but a non-empty library name
                # If so then this one can be discarded
                if len(filter(lambda l: l.sample_name == lib.sample_name and \
                              l.library_name != lib.library_name,
                              libraries)) > 0:
                    print "Discarding %s" % lib.fullname
                    continue
            ##print "Keeping %s" % lib.fullname
            self._libraries.append(lib)
        # Sort the libraries by name
        self._libraries = sorted(self._libraries,key=lambda l: l.name)

    @property
    def libraries(self):
        return [l for l in self._libraries]

    @property
    def library_groups(self):
        """
        Return list of library group names
        """
        return sorted(list(set([l.group for l in self._libraries])))

    def libraries_in_group(self,group):
        """
        Return list of libraries belonging to group
        """
        return filter(lambda l: l.group == group,self._libraries)

    def report(self):
        """
        Report
        """
        if len(self.libraries) == 0:
            print "No libraries found: not a SOLiD primary data directory?"
            return
        print "Libraries (ungrouped): %s" % \
            ', '.join(sorted([l.name for l in self.libraries]))
        print "Groups:"
        for group in self.library_groups: 
            print "- %s: %s" % (group,
                                ', '.join(sorted([l.name for \
                                                  l in self.libraries_in_group(group)])))
        for lib in self.libraries:
            print '=' * (len(lib.fullname)+4)
            print "* %s *" % lib.fullname
            print '=' * (len(lib.fullname)+4)
            for timestamp in lib.timestamps:
                print "* Timestamp: %s" % timestamp
                for pair in lib.get_file_pairs(timestamp):
                    for f in pair:
                        print "- %s" % f.relpath(self._dirn)

#######################################################################
# Functions
#######################################################################

def strip_extensions(path):
    """
    Strip off all trailing extensions
    """
    path,ext = os.path.splitext(path)
    ##print "%s %s" % (path,ext)
    while ext:
        path,ext = os.path.splitext(path)
        ##print "%s %s" % (path,ext)
    return path

def get_file_extensions(filen):
    """
    Extract extension and compression type from filename

    """
    ext = ''
    compression = ''
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

def convert_size(size):
    """
    Convert arbitary file size (e.g. 1T, 100G etc) to bytes
    """
    size = str(size)
    units = size[-1].upper()
    try:
        power = 'BKMGT'.index(units)
        size = float(size[:-1])
        for i in range(power):
            size = size * 1024.0
    except ValueError:
        size = float(size)
    return size

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

def find_md5sums(datadir):
    """
    """
    DataDir(datadir).md5sums()

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
                                'file_pairs'  : [] }
        for fpair in lib.get_file_pairs():
            for f in fpair:
                if f.path in symlinks:
                    ##print "- %s is linked" % f.path
                    lib_links[lib.name]['file_pairs'].append(fpair)
                    break
    # Report
    print "Primary data links from analysis dir:"
    for group in solid_data.library_groups:
        print "* %s *" % group
        for lib in solid_data.libraries_in_group(group):
            msg = []
            # Check that there are references
            file_pairs = lib_links[lib.name]['file_pairs']
            if file_pairs:
                # Reanalyse: check how many file pairs are referenced
                if len(file_pairs) == 1:
                    # Single file pair referenced
                    # Check that each file is referenced
                    ok = True
                    for f in file_pairs[0]:
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
                  "in DIR. Symlinks are not followed. If a file is "
                  "compressed then checksums are calculated for")
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
        find_md5sums(args[0])
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
        
