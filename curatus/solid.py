#!/bin/env python
#
#     curatus/solid.py: SOLiD-specific classes and functions
#     Copyright (C) University of Manchester 2015 Peter Briggs
#

"""
SOLiD-specific classes and functions

"""

import os
import core
import logging
import bcftbx.SolidData as SolidData
import bcftbx.utils as utils

#######################################################################
# Classes
#######################################################################

class SolidPrimaryData:
    """
    Utility class for describing a set of SOLiD primary data files

    """
    def __init__(self,*files):
        """Create a new SolidPrimaryData instance

        Optionally also populate with one or more
        files given in the argument list.

        """
        self._f3 = []
        self._f5 = []
        for f in files:
            self.add_file(f)
    def add_file(self,f):
        """Add a primary data file to the file set

        f should be an ArchiveFile instance.

        """
        # Identify the type (i.e. F3 or F5)
        fields = core.strip_extensions(f.basename).split('_')
        try:
            fields.index('F5-BC')
            self._f5.append(f)
            return
        except ValueError:
            pass
        try:
            fields.index('F3')
            self._f3.append(f)
            return
        except ValueError:
            pass
        self._f3.append(f)
    @property
    def f3(self):
        """
        Return the 'F3' assigned data files
        """
        return list(self._f3)
    @property
    def f5(self):
        """
        Return the 'F5' primary data files
        """
        return list(self._f5)
    @property
    def files(self):
        """
        Return all the files in the set
        """
        return self.f3 + self.f5
    @property
    def is_valid(self):
        """
        Check whether the file set is valid

        A valid file set has either one csfasta/qual
        F3 pair, or one csfasta/qual F5 pair.

        """
        if self._f3:
            if len(self._f3) != 2:
                # Must be exactly two F3 files
                logging.error("%d F3 files found" % len(self._f3))
                return False
            exts = [f.ext for f in self._f3]
        elif self._f5:
            if len(self._f5) != 2:
                # If there are F5 files then must also
                # have exactly 2
                logging.error("%d F5 files found" % len(self._f3))
                return False
            exts = [f.ext for f in self._f5]
        # Check this a csfasta/qual pair
        if 'csfasta' not in exts or 'qual' not in exts:
            logging.error("Not csfasta/qual pair (%s)" % ','.join(exts))
            return False
        # All tests passed
        return True
    @property
    def paired_end(self):
        """
        Check whether the file set is paired end

        To be paired end the file set must have
        F5 data

        """
        if self.is_valid:
            return bool(self._f5)
        return False

class SolidLibrary:
    """
    Utility class for holding data about a SOLiD library
    """
    def __init__(self,sample,library):
        self._sample_name = ('' if sample is None else str(sample))
        self._library_name = ('' if library is None else str(library))
        self._file_sets = {}
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
        return sorted(self._file_sets.keys())
    def add_file_set(self,timestamp,file_set):
        """
        Associate a set of files with a timestamp

        file_set should be a SolidPrimaryData object.

        The file sets can be accessed using the
        'get_file_sets' method.

        """
        if timestamp not in self.timestamps:
            self._file_sets[timestamp] = []
        self._file_sets[timestamp].append(file_set)
    def get_file_sets(self,timestamp=None):
        """
        Return file pairs

        If timestamp is not None then return
        just the file sets associated with that
        timestamp. Otherwise return all file
        sets.

        """
        if timestamp is not None:
            return self._file_sets[timestamp]
        else:
            file_sets = []
            for ts in self.timestamps:
                for p in self.get_file_sets(ts):
                    file_sets.append(p)
            return file_sets

class SolidDataDir(core.DataDir):
    """
    Subclass of DataDir with additional methods specifically for
    examining SOLiD data
    """
    def __init__(self,dirn):
        # Init base class
        core.DataDir.__init__(self,dirn)
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
        # Sort into primary data file sets
        file_sets = {}
        for f in primary_data:
            name = "%s/%s" % (os.path.dirname(f.relpath(self._dirn)),
                              get_generic_name(f))
            if name not in file_sets:
                file_sets[name] = SolidPrimaryData()
            file_sets[name].add_file(f)
        # Check for and report 'bad' file sets
        for name in file_sets:
            file_set = file_sets[name]
            if not file_set.is_valid:
                logging.error("Invalid file set '%s'" % name)
                for f in file_set.files:
                    print "* %s" % f.relpath(self._dirn)
        # Extract sample/library names and timestamps
        libraries = []
        for name in file_sets:
            sample,library,timestamp = get_library_names_and_timestamps(name)
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
            lib.add_file_set(timestamp,file_sets[name])
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
                for file_set in lib.get_file_sets(timestamp):
                    for f in file_set.f3:
                        print "- %s" % f.relpath(self._dirn)
                    for f in file_set.f5:
                        print "- %s" % f.relpath(self._dirn)

#######################################################################
# Functions
#######################################################################

def get_generic_name(f):
    """Get a generic name for a SOLiD primary data file

    The file 'f' must be supplied as an ArchiveFile
    instance.

    The returned name consists of the leading path plus the
    file name with extensions and any name elements '_QV',
    '_F5-BC', '_F5' and '_F3' all removed.

    The aim is to generate a name which is common to related
    F3 and F5 reads for both csfasta and qual files.

    """
    name = []
    for field in core.strip_extensions(f.basename).split('_'):
        if field not in ('QV','F5-BC','F5','F3'):
            name.append(field)
    return '_'.join(name)

def get_library_names_and_timestamps(name):
    """
    Extract sample/library names and timestamps

    Given a name from 'get_generic_name', extract the sample,
    library and timestamp elements.

    If no timestamp is present then it is returned
    as the string 'unknown'.

    For 'standard' SOLiD data directories, the top-level is a
    'sample', which may be split into multiple 'libraries', and
    then again in multiple timestamped filesets.

    Some examples:

    * LH_POOL/results.F1B1/libraries/LH1/primary.20111208144829752/reads/solid0127_20111207_FRAG_BC_LH_POOL_BC_LH1
    * ZD_hu/results.F1B1/primary.20091220022109452/reads/solid0424_20091214_ZD_hu_F3
    * SH_JC1_pool/results.F1B1/libraries_MM2/JC_SEQ30/primary.20120125063517232/reads/solid0127_20120117_PE_BC_SH_JC1_pool_F5-BC_JC_SEQ30

    """
    # Extract timestamp
    timestamp = SolidData.extract_library_timestamp(name)
    if timestamp is None:
        timestamp = 'unknown'
    # Attempt to extract sample and library from the path
    new_path = []
    for field in name.split(os.sep)[0:-1]:
        if field == "results.F1B1":
            continue
        elif field.startswith("libraries"):
            continue
        elif field == "reads":
            continue
        elif field.startswith("primary.") or field.startswith("secondary."):
            continue
        new_path.append(field)
    if len(new_path) < 2:
        sample = new_path[0]
        library = None
    else:
        sample,library = new_path[0:2]
    # Return
    return (sample,library,timestamp)
