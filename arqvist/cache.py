#!/bin/env python
#
#     cache.py: caching classes and functions
#     Copyright (C) University of Manchester 2016 Peter Briggs
#

"""
Caching classes and functions

"""

import os
import datetime
import dateutil.parser
import tempfile
import shutil
import fnmatch
from .core import ArchiveFile
from .core import get_file_extensions
from bcftbx.utils import AttributeDictionary
import bcftbx.utils as bcfutils

FILE_ATTRIBUTES = ('basename',
                   'type',
                   'size',
                   'timestamp',
                   'mode',
                   'uid',
                   'gid',
                   'ext',
                   'compression',
                   'md5',
                   'uncompressed_md5',
                   'relpath',)

class DirCache(object):
    """
    Cache info on contents of directory

    Usage:

    >>> c = DirCache('Downloads')

    The cache data is held in memory but can be saved to disk
    (within a '.arqvist' directory under the top-level
    directory) using:

    >>> c.save()

    When the DirCache object is reinstantiated the data will
    be read from the cache automatically.

    A list of files in the cache can be obtained using the
    'files' method, e.g.:

    >>> for f in c.files:
    >>> ...

    Data on each file in the cache are stored as CacheFile
    instances which can be retrieved as keys of the DirCache
    instance, e.g.:

    >>> f = c['myfile']

    Note that the keys are relative paths to the original
    source directory.

    Once the cache has been written to disk the information
    stored will be unchanged even if the contents of the
    original directory have changed. To check if the cache is
    'stale' (that is, no longer reflects the contents of the
    original directory) use the 'is_stale' method.

    To get lists of the modified, new (aka 'untracked') or
    deleted files use the 'status' method, e.g.:

    >>> delelted,modified,untracked = c.status()

    To update the cache in memory use the 'update' method;
    (note that the cache must then be explicitly rewritten to
    disk with the 'save' method).

    """
    
    def __init__(self,dirn,include_checksums=False):
        """
        Create a new DirCache instance

        Arguments:
          dirn (str): source directory to build the
            cache for
          include_checksums (bool): if True then
            also generate MD5 checksums (default is
            False)

        """
        self._dirn = os.path.abspath(dirn)
        self._cache_dirname = '.arqvist'
        self._files = {}
        self._file_attributes = FILE_ATTRIBUTES
        self._ignore = []
        # Populate cache
        if not self.load():
            self.build(include_checksums=include_checksums)

    def __getitem__(self,key):
        return self._files[os.path.normpath(key)]

    def __len__(self):
        return len(self._files)

    @property
    def dirn(self):
        """
        Return path to the source directory
        """
        return self._dirn

    @property
    def cachedir(self):
        """
        Return path to cache directory
        """
        return os.path.join(self._dirn,self._cache_dirname)

    @property
    def exists(self):
        """
        Check if the cache exists on disk
        """
        return os.path.exists(os.path.join(self.cachedir,
                                           'files'))

    @property
    def files(self):
        """
        Return sorted list of files
        """
        return sorted(self._files.keys())

    @property
    def is_stale(self):
        """
        Check if the cache differs from reality
        """
        d,m,u = self.status()
        if d or m or u:
            return True
        return False

    def _walk(self,dirn=None):
        """
        Internal: walk the source directory structure

        By default this traverses the directory structure
        associated with the DirCache object and yields
        the full path to each subdirectory and file that
        is found.

        Optionally it can be used to traverse an
        alternative directory structure by explicitly
        specifying the ``dirn`` argument.

        """
        if dirn is None:
            dirn = self._dirn
        for d in os.walk(dirn):
            if os.path.basename(d[0]) == self._cache_dirname:
                # Skip the cache directory
                continue
            for f in d[1]:
                if f == self._cache_dirname:
                    # Skip the cache directory
                    continue
                yield os.path.normpath(os.path.join(d[0],f))
            for f in d[2]:
                yield os.path.normpath(os.path.join(d[0],f))

    def _add_file(self,*args,**kws):
        """
        Add a file/directory and update stored info

        Arguments:
          args (list): path components to assemble
            into path for the file being added
          include_checksums (bool): if True then
            also generate MD5 checksums (default is
            False)

        """
        # Extract keywords
        if 'include_checksums' in kws:
            include_checksums = kws['include_checksums']
        else:
            include_checksums = False
        # Construct the normalised and relative file paths
        fn = os.path.normpath(os.path.join(*args))
        f = ArchiveFile(fn)
        relpath = f.relpath(self._dirn)
        # Construct cache representation
        cachefile = CacheFile(relpath,
                              type=f.type,
                              size=f.size,
                              timestamp=f.timestamp,
                              mode=f.mode,
                              uid=f.uid,
                              gid=f.gid)
        # Generate checksums
        if include_checksums:
            cachefile['md5'] = f.md5
            cachefile['uncompressed_md5'] = f.uncompressed_md5
        # Store in the index
        if relpath in self._files:
            print "%s: updating cache" % relpath
        self._files[relpath] = cachefile

    def build(self,include_checksums=False):
        """
        Build the cache in memory
        """
        for f in self._walk():
            if self.ignore(f):
                continue
            self._add_file(f,include_checksums=include_checksums)

    def update(self,include_checksums=False):
        """
        Update the cache in memory

        Arguments:
          include_checksums (bool): if True then
            also generate MD5 checksums (default is
            False)

        """
        self.build(include_checksums=include_checksums)
        for f in self.files:
            if not os.path.lexists(os.path.join(self._dirn,f)):
                print "%s: removing" % f
                del(self._files[f])

    def load(self):
        """
        Load the cache into memory from disk
        """
        # Files/patterns to ignore
        ignorefile = os.path.join(self.cachedir,'ignore')
        if os.path.exists(ignorefile):
            with open(ignorefile,'r') as fp:
                for line in fp:
                    line = line.rstrip()
                    if line.startswith('#'):
                        continue
                    self._ignore.append(line)
        # Cached files
        filecache = os.path.join(self.cachedir,'files')
        if not os.path.exists(filecache):
            return False
        with open(filecache,'r') as fp:
            attributes = None
            for line in fp:
                line = line.rstrip('\n')
                if not attributes:
                    attributes = line[1:].split('\t')
                    continue
                values = line.split('\t')
                data = dict(zip(attributes,values))
                relpath = data['relpath']
                adf = CacheFile(relpath,**data)
                self._files[relpath] = adf
        return True

    def ignore(self,f,dirn=None):
        """
        Check if file matches one of the 'ignore' patterns

        The file path is taken to be relative to the parent
        directory of the DirCache instance before being tested
        against the 'ignore' patterns.; if the ``dirn``
        argument is explicitly specified then the file path
        will be taken to be relative to that instead.

        Returns True if the file matches at least one 'ignore'
        pattern, otherwise returns False.

        """
        if dirn is None:
            dirn = self._dirn
        for pattern in self._ignore:
            if fnmatch.fnmatch(os.path.relpath(f,dirn),pattern):
                return True
        return False

    def status(self,dirn=None,attributes=('type','size',)):
        """
        Check the cache against a source directory

        Checks for cache entries that have been deleted (i.e.
        there is a cache entry but no corresponding file on
        disk), modified (i.e. specified attributes are
        different on disk) or are new (aka 'untracked', i.e.
        exist on disk but not in the cache).

        By default only the 'type' and 'size' attributes are
        checked to determine if an entry has been modified;
        the list of attributes can be changed by explicitly
        specifying the ``attributes`` argument.

        By default the cache is checked against the directory
        associated with the DirCache instance; otherwise if
        the ``dirn`` argument is explicitly specified then
        this directory is checked instead.

        Returns a tuple of lists of deleted, modified and
        untracked files.

        """
        if dirn is None:
            dirn = self._dirn
        deleted = []
        modified = []
        untracked = []
        for f in self._walk(dirn):
            if self.ignore(f,dirn):
                continue
            try:
                relpath = os.path.relpath(f,dirn)
                cachefile = self[relpath]
                if cachefile.compare(f,attributes):
                    modified.append(relpath)
            except KeyError:
                untracked.append(relpath)
        for f in self.files:
            if not os.path.lexists(os.path.join(dirn,f)):
                deleted.append(f)
        return (deleted,modified,untracked)

    def normalise_relpaths(self,paths,dirn=None,workdir=None,
                           abspaths=False):
        """
        Transform a list of relative paths

        Given a list of relative paths, returns a new list
        where each path has been transformed as follows:

        - each path is appended to the base directory
          (defaults to the DirCache source directory)
        - if absolute paths are not requested (``abspaths``
          is False, i.e. the default) then each path is
          subsequently made relative to the specified
          working directory (also defaults to the DirCache
          source directory)
        - if absolute paths are requested then the working
          directory is ignored, even if specified.

        """
        if dirn is None:
            dirn = self._dirn
        if workdir is None:
            workdir = self._dirn
        if not abspaths:
            return [os.path.relpath(os.path.join(dirn,p),workdir)
                    for p in paths]
        else:
            return [os.path.normpath(os.path.join(dirn,p))
                    for p in paths]

    def save(self):
        """
        Write the cache data to disk
        """
        # Create cache dir if it doesn't exist
        if not os.path.exists(self.cachedir):
            bcfutils.mkdir(self.cachedir)
        # Write to temp file first
        fp,fname = tempfile.mkstemp(dir=self.cachedir)
        fp = os.fdopen(fp,'w')
        fp.write('#%s\n' % '\t'.join(self._file_attributes))
        for f in self.files:
            fp.write('%s\n' % '\t'.join([str(self[f][x])
                                         for x in self._file_attributes]))
        fp.close()
        # Make copy of old file
        if os.path.exists(os.path.join(self.cachedir,'files')):
            shutil.copy2(os.path.join(self.cachedir,'files'),
                         os.path.join(self.cachedir,'files.bak'))
        # Move new version
        shutil.move(fname,os.path.join(self.cachedir,'files'))
        # Set permissions
        os.chmod(os.path.join(self.cachedir,'files'),0664)

class CacheFile(AttributeDictionary,object):
    """
    Class to store cached info on a file

    Usage:

    >>> f = CacheFile('path/to/file')

    creates an 'empty' CacheFile instance, or:

    >>> f = CacheFile('path/to/file',
    ...               size=512,
    ...               timestamp='2016-03-22 13:15:47.955909',
    ...               type='f')

    creates a populated CacheFile instance.

    Attributes can be accessed as dictionary keys or
    attributes, e.g.

    >>> f.size

    or

    >>> f['size']

    Keys can also be used to set new values:

    >>> f['size'] = 1024

    """
    def __init__(self,path,**kws):
        """
        Create a new CacheFile instance

        Arguments:
          path (str): path to the 'parent' file (typically
            relative to a reference directory)

        """
        self._file_attributes = FILE_ATTRIBUTES
        AttributeDictionary.__init__(self)
        # Initialise everything to None
        for x in self._file_attributes:
            self[x] = None
        # Set the file/base name
        self['relpath'] = path
        self['basename'] = os.path.basename(path)
        # Set the compression and file extension
        self['ext'],self['compression'] = get_file_extensions(path)
        # Store the supplied data
        for x in kws:
            if x not in self._file_attributes:
                raise KeyError("Unrecognised key: '%s'" % x)
            self[x] = kws[x]
            # Explicitly set None data items
            if self[x] == 'None':
                self[x] = None
        # Explicitly set file sizes, uids and gids to integer
        for attr in ('size','uid','gid',):
            if getattr(self,attr) is not None:
                self[attr] = int(getattr(self,attr))
        # Explicitly handle time stamp
        if self.timestamp is not None:
            self['timestamp'] = dateutil.parser.parse(str(self.timestamp))

    @property
    def attributes(self):
        """
        Return list of attributes associated with the CacheFile
        """
        return self._file_attributes

    def is_stale(self,size,timestamp):
        """
        Check if cached attributes differ from supplied values

        Note that the test is only implemented for files; all
        other types return False without any tests.

        """
        if self.type == 'f':
            return (size != self.size or timestamp != self.timestamp)
        else:
            return False

    def compare(self,path,attributes=('size','timestamp')):
        """
        Compares cached file properties with one on disk

        Returns a list of the attributes that have been
        modified, or an empty list if they are the same.

        """
        modified_attrs = []
        f = ArchiveFile(path)
        for attr in attributes:
            try:
                if self[attr] != getattr(f,attr):
                    modified_attrs.append(attr)
            except AttributeError:
                pass
        return modified_attrs

def locate_cache_dir(dirn):
    """
    Find DirCache directory on disk

    Given a directory, locate a top-level DirCache directory
    in either that directory, or one of its parents.

    If no DirCache directory is found then return None, else
    return the path to the directory.

    """
    if dirn is None:
        dirn = os.getcwd()
    while not os.path.exists(os.path.join(dirn,'.arqvist','files')):
        if dirn == os.sep:
            return None
        dirn = os.path.dirname(dirn)
    return dirn
