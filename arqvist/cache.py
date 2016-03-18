#!/bin/env python
#
#     cache.py: caching classes and functions
#     Copyright (C) University of Manchester 2016 Peter Briggs
#

"""
Caching classes and functions

"""

import os
from .core import ArchiveFile
import bcftbx.utils as utils

class DataDirCache:
    """
    """
    
    def __init__(self,dirn):
        """
        """
        self._cache_dirname = '.arqvist'
        self._dirn = os.path.abspath(dirn)
        self._files = {}
        self._file_attributes = ('basename',
                                 'size',
                                 'timestamp',
                                 'owner',
                                 'group',
                                 'ext',
                                 'compression',
                                 'md5',
                                 'uncompressed_md5',
                                 'relpath',)
        # Load data from existing cache
        if os.path.isdir(self.cachedir):
            print "Found %s" % self.cachedir
            self.load()
        # Build file lists
        for d in os.walk(dirn):
            if os.path.basename(d[0]) == self._cache_dirname:
                # Skip the cache directory
                continue
            for f in d[1]:
                if f == self._cache_dirname:
                    # Skip the cache directory
                    continue
                self._add_file(d[0],f)
            for f in d[2]:
                self._add_file(d[0],f)

    def __getitem__(self,key):
        return self._files[key]

    def __len__(self):
        return len(self._files)
    
    @property
    def cachedir(self):
        """
        Return path to cache directory
        """
        return os.path.join(self._dirn,self._cache_dirname)

    @property
    def files(self):
        return self._files.keys()

    def _add_file(self,*args):
        """
        Add a file/directory and update stored info
        """
        # Construct the normalised and relative file paths
        fn = os.path.normpath(os.path.join(*args))
        relpath = os.path.relpath(fn,self._dirn)
        # Look up the file
        archf = ArchiveFile(fn)
        if relpath in self._files:
            adf = self._files[relpath]
            print "%s: already in cache" % relpath
            if archf.size != adf['size'] or archf.timestamp != adf['timestamp']:
                print "*** CACHE IS STALE ***"
                for attr in self._file_attributes:
                    try:
                        file_value = getattr(archf,attr)
                        cache_value = adf[attr]
                        if str(file_value) != str(cache_value):
                            print "%s:\t%s != %s (cached)" % (attr,
                                                              file_value,
                                                              cache_value)
                            print "Types: %s %s" % (type(file_value),
                                                    type(cache_value))
                    except AttributeError:
                        pass
        else:
            # Store the basic attributes
            adf = utils.AttributeDictionary()
            adf['basename'] = args[-1]
            adf['size'] = archf.size
            adf['timestamp'] = archf.timestamp
            adf['owner'] = None
            adf['group'] = None
            adf['ext'] =  archf.ext
            adf['compression'] = archf.compression
            adf['md5'] = None
            adf['uncompressed_md5'] = None
            adf['relpath'] = relpath
            # Store in the index
            self._files[relpath] = adf

    def load(self):
        """
        """
        with open(os.path.join(self.cachedir,'files'),'r') as fp:
            for line in fp:
                line = line.rstrip('\n')
                if line.startswith('#'):
                    attributes = line[1:].split('\t')
                    continue
                adf = utils.AttributeDictionary()
                for attr in self._file_attributes:
                    adf[attr] = None
                values = line.rstrip('\n').split('\t')
                for attr,value in zip(attributes,values):
                    if value == 'None':
                        adf[attr] = None
                    else:
                        try:
                            adf[attr] = int(value)
                        except ValueError:
                            try:
                                adf[attr] = float(value)
                            except ValueError:
                                adf[attr] = value
                self._files[adf.relpath] = adf

    def save(self):
        """
        """
        if not os.path.exists(self.cachedir):
            utils.mkdir(self.cachedir)
        with open(os.path.join(self.cachedir,'files'),'w') as fp:
            fp.write('#%s\n' % '\t'.join(self._file_attributes))
            for f in self.files:
                fp.write('%s\n' % '\t'.join([str(self[f][attr])
                                             for attr in
                                             self._file_attributes]))
