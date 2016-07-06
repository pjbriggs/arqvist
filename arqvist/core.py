#!/bin/env python
#
#     core.py: core classes and functions
#     Copyright (C) University of Manchester 2015 Peter Briggs
#

"""
Core classes and functions

"""

import os
import stat
import gzip
import bz2
import fnmatch
import itertools
import logging
import tempfile
import datetime
import bcftbx.utils as utils
import bcftbx.Md5sum as Md5sum
from auto_process_ngs import applications

# File extensions for Next Generation Sequencing (NGS)
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

    Available attributes:

    - size
    - timestamp
    - mode
    - ext
    - compression

    Properties:

    - basename
    - type
    - is_readable
    - classifier

    Methods:

    - get_md5sums
    - compress

    """
    def __init__(self,filen):
        """
        Create and populate a new ArchiveFile instance
        """
        utils.PathInfo.__init__(self,os.path.abspath(filen))
        # !!!FIXME should be able to st_size from PathInfo!!!
        self.size = os.lstat(self.path).st_size
        self.timestamp = datetime.datetime.utcfromtimestamp(self.mtime)
        self.mode = oct(stat.S_IMODE(os.lstat(self.path).st_mode))
        self.ext,self.compression = get_file_extensions(self.path)
        self._md5 = None
        self._uncompressed_md5 = None

    @property
    def basename(self):
        """
        Return the basename of the file path
        """
        return os.path.basename(self.path)

    @property
    def type(self):
        """
        Return a one letter identifier for the file type
        """
        if self.is_link:
            return 's'
        elif self.is_dir:
            return 'd'
        elif self.is_file:
            return 'f'
        raise OSError("Unable to identify type for %s" % self.path)

    @property
    def is_readable(self):
        """
        Return True if mode allows read by current user
        """
        return os.access(self.path,os.R_OK)

    @property
    def md5(self):
        """
        Return MD5 checksum for the ArchiveFile
        """
        if self.is_link or self.is_dir:
            # Ignore links or directories
            return None
        if self._md5 is None:
            # Generate MD5 sum
            self._md5 = Md5sum.md5sum(self.path)
        return self._md5

    @property
    def uncompressed_md5(self):
        """
        Return MD5 checksum for the uncompressed ArchiveFile
        """
        if self.is_link or self.is_dir:
            # Ignore links or directories
            return None
        if self._uncompressed_md5 is None:
            # Generate MD5 for uncompressed contents
            if not self.compression:
                self._uncompressed_md5 = self.md5
            elif self.compression == 'bz2':
                fp = bz2.BZ2File(self.path,'r')
                self._uncompressed_md5 = Md5sum.md5sum(fp)
            elif self.compression == 'gz':
                fp = gzip.GzipFile(self.path,'rb')
                self._uncompressed_md5 = Md5sum.md5sum(fp)
            else:
                logging.warning("%s: md5sums not implemented for "
                                "compression type '%s'"
                                % (self,self.compression))
        return self._uncompressed_md5

    @property
    def target(self):
        if not self.is_link:
            # Ignore non-symlinks
            return None
        return utils.Symlink(self.path).target

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

    def get_md5sums(self):
        """
        Generate MD5sums

        Generate and return MD5 sums for the file and
        for the uncompressed contents.

        Sets the 'md5' and 'uncompressed_md5' properties on
        the current instance.

        Returns tuple (md5,md5_uncompressed_contents).

        """
        return (self.md5,self.uncompressed_md5)

    def compress(self,dry_run=False):
        """
        Compress the file

        Performs compression using bzip2, and transfers
        the timestamp from the original file to the
        compressed version.

        If 'dry_run' is True then report the compression
        operation but don't report anything.
              
        Returns status:

        0 indicates success
        -1 indicates nothing to do, no error
        >0 indicates an error

        """
        if self.compression:
            logging.warning("%s: already compressed" % self)
            return -1
        # Check for existing compressed file
        bz2file = self.path + '.bz2'
        if os.path.exists(bz2file):
            logging.warning("%s: compressed copy already exists" % self)
            return -1
        # Get MD5 checksum
        checksum = self.md5
        # Capture timestamp for parent directory
        parent_mtime = os.lstat(os.path.dirname(self.path)).st_mtime
        # Compress to a temp file
        bzip2_cmd = applications.Command('bzip2','-c',self.path)
        print bzip2_cmd
        if dry_run:
            return -1
        fd,tmpbz2 = tempfile.mkstemp(dir=os.path.dirname(self.path),
                                     suffix='.bz2.tmp')
        # Execute the compression command
        try:
            status = bzip2_cmd.run_subprocess(log=tmpbz2)
        except Exception,ex:
            logging.error("Exception compressing %s: %s" % (self,ex))
            status = 1
        if status != 0:
            logging.error("Compression failed for %s" % self)
        else:
            # Verify the checksum for the contents of the
            # compressed file
            uncompressed_checksum = Md5sum.md5sum(bz2.BZ2File(tmpbz2,'r'))
            if uncompressed_checksum == checksum:
                # Rename the compressed file, reset the timestamps
                # and remove the source
                os.rename(tmpbz2,bz2file)
                os.utime(bz2file,(self.mtime,self.mtime))
                os.remove(self.path)
                os.utime(os.path.dirname(self.path),(parent_mtime,parent_mtime))
                # Update attributes
                # FIXME have to explicitly deal with name mangling
                # in order to update the path in the superclass
                # - this should really be done in a better way
                self._PathInfo__path = bz2file
                self._PathInfo__st = os.lstat(self.path)
                self.compression = 'bz2'
                self._md5 = None
            else:
                logging.error("Bad checksum for compressed version of %s" % self)
                status = 1
        # Remove the temp file
        if os.path.exists(tmpbz2):
            os.remove(tmpbz2)
        # Finish
        return status

    def __repr__(self):
        return self.path

class ArchiveSymlink(utils.Symlink):
    """
    Class for interrogating and modifying a symlink

    """
    def __init__(self,path):
        utils.Symlink.__init__(self,path)

    @property
    def path(self):
        """
        Return the path for the symlink
        """
        return self._path

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
        alt_target,ext = os.path.splitext(target)
        if ext in ('.gz','.bz2'):
            if os.path.exists(alt_target):
                return alt_target
        # Nothing found
        return None

    @property
    def classifier(self):
        """Return classifier for an ArchiveSymlink object

        Return an indicator string with the following
        components:

        First character - one of:

        A = absolute link target
        r = relative target

        Second character - one of:

        - = working link
        X = broken link
        x = broken link with alternative target

        For example: 'AX' = broken absolute link.

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
    - store data about platform, year?
    - store and update arbitrary metadata, and
      dump this to a README or other info file for
      curation

    """
    def __init__(self,dirn,files=None):
        """
        Create a new DataDir instance

        files: optional, if specified then should be a list
               of ArchiveFile instances to populate the DataDir
               with

        """
        self._dirn = os.path.abspath(dirn)
        self._nfiles = 0
        self._size = 0
        self._files = []
        self._extensions = []
        self._compression = []
        self._users = []
        self._groups = []
        self.oldest = None
        self.newest = None
        self.usr_unreadable = False
        self.grp_unreadable = False
        self.grp_unwritable = False
        # Populate
        if files is not None:
            # List of files supplied
            for f in files: self._add_file(f)
        else:
            # Collect list of files
            for d in os.walk(self._dirn):
                if os.path.basename(d[0]) == '.archiver':
                    # Skip the cache directory
                    continue
                for f in d[1]:
                    if f == '.archiver':
                        # Skip the cache directory
                        continue
                    self._add_file(ArchiveFile(os.path.normpath(os.path.join(d[0],f))))
                for f in d[2]:
                    self._add_file(ArchiveFile(os.path.normpath(os.path.join(d[0],f))))
        # Update cache (if present)
        self.update_cache()

    def _add_file(self,f):
        """
        Add a file/directory and update stored info
        """
        self._files.append(f)
        # General info
        if not f.is_dir:
            self._nfiles += 1
        self._size += f.size
        # File and compression types
        if f.ext in NGS_FILE_TYPES and f.ext not in self._extensions:
            self._extensions.append(f.ext)
        if f.compression and f.compression not in self._compression:
            self._compression.append(f.compression)
        # Users and groups
        if f.user not in self._users:
            self._users.append(f.user)
        if f.group not in self._groups:
            self._groups.append(f.group)
        # Oldest and newest modification times
        try:
            self.oldest = f if f.mtime < self.oldest.mtime else self.oldest
            self.newest = f if f.mtime > self.newest.mtime else self.newest
        except AttributeError:
            self.oldest = f
            self.newest = f
        # Permissions i.e. unreadable/unwriteable files
        self.usr_unreadable = self.usr_unreadable or not f.is_readable
        self.grp_unreadable = self.grp_unreadable or not f.is_group_readable
        self.grp_unwritable = self.grp_unwritable or not f.is_group_writable
        # Return the file instance
        return f

    def __del__(self):
        self.write_cache()

    def __len__(self):
        """
        Number of files in the directory
        """
        return self._nfiles

    @property
    def has_cache(self):
        """
        Check if a cache directory exists
        """
        cachedir = os.path.join(self._dirn,'.archiver')
        return os.path.exists(cachedir)

    def init_cache(self):
        """
        Initialise a cache subdirectory
        """
        cachedir = os.path.join(self._dirn,'.archiver')
        if os.path.exists(cachedir):
            return
        os.mkdir(cachedir)

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
                    if f['size'] == filen.size and f['time'] == filen.mtime:
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
                          f.mtime,
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

    @property
    def size(self):
        """
        Total size (in bytes) of the directory contents
        """
        return self._size

    @property
    def extensions(self):
        """
        File types (i.e. extensions) found under the directory
        """
        return self._extensions

    @property
    def compression(self):
        """
        Compression types found under the directory
        """
        return self._compression

    @property
    def users(self):
        """
        User names associated with directory contents
        """
        return self._users

    @property
    def groups(self):
        """
        Group names associated with directory contents
        """
        return self._groups

    def files(self,extensions=None,owners=None,groups=None,compression=None,
              subdir=None,pattern=None,sort_keys=None):
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
        if pattern:
            files = [f for f in itertools.ifilter(lambda x: fnmatch.fnmatch(x.basename,
                                                                            pattern),
                                                  files)]
        if sort_keys:
            for key in sort_keys:
                if key == 'size':
                    files = sorted(files,key=lambda f: f.size)
                else:
                    raise NotImplementedError("Sort on '%s' not implemented" % key)
        return files

    def symlinks(self):
        """
        Return list of symbolic links as ArchiveSymlink objects
        """
        return [ArchiveSymlink(f.path)
                for f in itertools.ifilter(lambda x: x.is_link,self._files)]

    def list_temp(self):
        """
        Return a list of temporary files/directories
        """
        return [f.path for f in itertools.ifilter(lambda x: bool(x.basename.count('tmp')),
                                                  self._files)]

    def related_dirs(self):
        """
        Examine symlinks and find those pointing to external directories
        """
        external_dirs = []
        for ln in self.symlinks():
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
            f.get_md5sums()

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
        # Report information
        print "Dir   : %s" % self._dirn
        print "Size  : %s (%s)" % (utils.format_file_size(self.size),
                                   utils.format_file_size(self.size,'K'))
        print "Has cache: %s" % print_yes_no(self.has_cache)
        print "#files: %d" % len(self)
        print "File types: %s" % print_list(self.extensions)
        print "Compression types: %s" % print_list(self.compression)
        print "Users : %s" % print_list(self.users)
        print "Groups: %s" % print_list(self.groups)
        print "Oldest: %s %s" % (self.oldest.datetime.ctime(),self.oldest.relpath(self._dirn))
        print "Newest: %s %s" % (self.newest.datetime.ctime(),self.newest.relpath(self._dirn))
        # Top-level subdirectories
        print "Top-level subdirectories:"
        print "# Dir\tFiles\tSize\tFile types\tUsers\tPerms"
        for subdir in  utils.list_dirs(self._dirn):
            sd = DataDir(os.path.join(self._dirn,subdir),
                         files=self.files(subdir=subdir))
            print "- %s/\t%d\t%s\t%s\t%s\t%s" % (subdir,
                                                 len(sd),
                                                 utils.format_file_size(sd.size),
                                                 print_list(sd.extensions),
                                                 print_list(sd.users),
                                                 print_perms(sd.usr_unreadable,
                                                             sd.grp_unreadable,
                                                             sd.grp_unwritable))
        # File permissions
        print "File permissions:"
        print "- unreadable by owner: %s" % print_yes_no(self.usr_unreadable)
        print "- unreadable by group: %s" % print_yes_no(self.grp_unreadable)
        print "- unwritable by group: %s" % print_yes_no(self.grp_unwritable)
        print "#Temp files: %d" % len(self.list_temp())

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

    Returns a tuple (ext,compression) where compression
    is one of '','gz' or 'bz2' (empty string indicates no
    compression) and ext is the trailing file extension
    once any compression extension has been removed.

    For example:
    >>> get_file_extensions('test')
    ('','')
    >>> get_file_extensions('test.bz2')
    ('','bz2')
    >>> get_file_extensions('test.fastq')
    ('fastq','')
    >>> get_file_extensions('test.fastq.gz')
    ('fastq','gz')
    >>> get_file_extensions('test.file.fastq.gz')
    ('fastq','gz')

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

def print_list(l,delimiter=','):
    """
    Pretty-print an arbitrary list
    """
    return delimiter.join([str(x) for x in l])

def print_yes_no(b):
    """
    Return 'yes' or 'no' depending on boolean value
    """
    if b:
        return 'yes'
    return 'no'

def print_perms(usr_unreadable,grp_unreadable,grp_unwritable):
    """
    Return summary string for permissions

    The string is of the form e.g. 'ur,gr-'

    - 'u' indicates permissions for the current user
    - 'g' indicates permissions for the group

    - 'r' indicates read permission for all files
    - 'w' indicates write permission for all files
    - '-' indicates missing permissions for some or all
          files

    """
    return 'u%s,g%s%s' % (('-' if usr_unreadable else 'r'),
                          ('-' if grp_unreadable else 'r'),
                          ('-' if grp_unwritable else 'w'))
