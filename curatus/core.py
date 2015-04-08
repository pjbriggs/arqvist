#!/bin/env python
#
#     curatus/core.py: core classes and functions
#     Copyright (C) University of Manchester 2015 Peter Briggs
#

"""
Core classes and functions

"""

import os
import bz2
import itertools
import logging
import tempfile
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

    def get_md5sums(self):
        """
        Generate MD5sums

        Generate and return MD5 sums for the file and
        for the uncompressed contents.

        Sets the 'md5' and 'uncompressed_md5' properties on
        the current instance.

        Returns tuple (md5,md5_uncompressed_contents).

        """
        if self.is_link or self.is_dir:
            # Ignore links or directories
            return (None,None)
        if self.md5 is None:
            # Generate MD5 sum
            self.md5 = Md5sum.md5sum(self.path)
        if self.uncompressed_md5 is None:
            # Generate MD5 for uncompressed contents
            if not self.compression:
                self.uncompressed_md5 = self.md5
            elif self.compression == 'bz2':
                fp = bz2.BZ2File(self.path,'r')
                self.uncompressed_md5 = Md5sum.md5sum(fp)
            elif self.compression == 'gz':
                fp = gzip.GzipFile(self.path,'rb')
                self.uncompressed_md5 = Md5sum.md5sum(fp)
            else:
                logging.warning("%s: md5sums not implemented for "
                                "compression type '%s'"
                                % (self,self.compression))
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
        self.get_md5sums()
        checksum = self.md5
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
                # Rename the compressed file, reset the timestamp
                # and remove the source
                os.rename(tmpbz2,bz2file)
                os.utime(bz2file,(self.mtime,self.mtime))
                os.remove(self.path)
                # Update self
                self = ArchiveFile(bz2file)
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
        print "# Dir\tFiles\tSize\tFile types\tUsers\tPerms"
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
                                                 len(filter(lambda f: not f.is_dir,
                                                            subdir_files)),
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
