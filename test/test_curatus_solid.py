#!/bin/env python
#
# Unit tests for the curatus/solid package
import os
import unittest
import utils
import curatus

#
# Tests

from curatus.solid import SolidPrimaryData
class TestSolidPrimaryData(unittest.TestCase):
    def setUp(self):
        # Create and populate a SolidPrimaryData instance
        self.solid_primary_data = SolidPrimaryData()
        self.solid_primary_data.add_file(utils.MockArchiveFile('test1_F3.csfasta'))
        self.solid_primary_data.add_file(utils.MockArchiveFile('test1_F3_QV.qual'))
        self.solid_primary_data.add_file(utils.MockArchiveFile('test1_F5-BC.csfasta'))
        self.solid_primary_data.add_file(utils.MockArchiveFile('test1_F5-BC_QV.qual'))
    def test_add_file(self):
        # Add files
        # !!!This isn't really a test!!!
        raise unittest.SkipTest("SolidPrimaryData.add_file not testable?")
    def test_f3(self):
        # Retrieve F3 files
        self.assertEqual(len(self.solid_primary_data.f3),2)
        files = [f.path for f in self.solid_primary_data.f3]
        self.assertTrue(os.path.abspath('test1_F3.csfasta') in files)
        self.assertTrue(os.path.abspath('test1_F3_QV.qual') in files)
    def test_f5(self):
        # Retrieve F5 files
        self.assertEqual(len(self.solid_primary_data.f5),2)
        files = [f.path for f in self.solid_primary_data.f5]
        self.assertTrue(os.path.abspath('test1_F5-BC.csfasta') in files)
        self.assertTrue(os.path.abspath('test1_F5-BC_QV.qual') in files)
    def test_files(self):
        # Retrieve all files
        self.assertEqual(len(self.solid_primary_data.files),4)
        files = [f.path for f in self.solid_primary_data.files]
        self.assertTrue(os.path.abspath('test1_F3.csfasta') in files)
        self.assertTrue(os.path.abspath('test1_F3_QV.qual') in files)
        self.assertTrue(os.path.abspath('test1_F5-BC.csfasta') in files)
        self.assertTrue(os.path.abspath('test1_F5-BC_QV.qual') in files)
    def test_is_valid(self):
        # F3 file pair
        self.assertTrue(SolidPrimaryData(
            utils.MockArchiveFile('test1_F3.csfasta'),
            utils.MockArchiveFile('test1_F3_QV.qual')).is_valid)
        # F5 file pair
        self.assertTrue(SolidPrimaryData(
            utils.MockArchiveFile('test1_F5-BC.csfasta'),
            utils.MockArchiveFile('test1_F5-BC_QV.qual')).is_valid)
        # Not a file pair
        self.assertFalse(SolidPrimaryData(
            utils.MockArchiveFile('test1_F3.csfasta')).is_valid)
        self.assertFalse(SolidPrimaryData(
            utils.MockArchiveFile('test1_F5-BC_QV.qual')).is_valid)
        # Too many files
        self.assertFalse(SolidPrimaryData(
            utils.MockArchiveFile('test1_F3.csfasta'),
            utils.MockArchiveFile('test1_F3_QV.qual'),
            utils.MockArchiveFile('test2_F3_QV.qual')).is_valid)
    def test_paired_end(self):
        # F3 file pair
        self.assertFalse(SolidPrimaryData(
            utils.MockArchiveFile('test1_F3.csfasta'),
            utils.MockArchiveFile('test1_F3_QV.qual')).paired_end)
        # F5 file pair
        self.assertTrue(SolidPrimaryData(
            utils.MockArchiveFile('test1_F5-BC.csfasta'),
            utils.MockArchiveFile('test1_F5-BC_QV.qual')).paired_end)
        # Not a file pair
        self.assertFalse(SolidPrimaryData(
            utils.MockArchiveFile('test1_F5-BC.csfasta')).paired_end)

from curatus.solid import SolidLibrary
class TestSolidLibrary(unittest.TestCase):
    def test_sample_name(self):
        # Create libraries and check the sample names
        lib = SolidLibrary('PB','PJB_01')
        self.assertEqual(lib.sample_name,'PB')
        lib = SolidLibrary(None,'PJB_01')
        self.assertEqual(lib.sample_name,'')
    def test_library_name(self):
        # Create libraries and check the library names
        lib = SolidLibrary('PB','PJB_01')
        self.assertEqual(lib.library_name,'PJB_01')
        lib = SolidLibrary('PB',None)
        self.assertEqual(lib.library_name,'')
    def test_name(self):
        # Create libraries and check the short names
        lib = SolidLibrary('PB','PJB_01')
        self.assertEqual(lib.name,'PJB_01')
        lib = SolidLibrary('PB',None)
        self.assertEqual(lib.name,'PB')
        lib = SolidLibrary(None,'PJB_01')
        self.assertEqual(lib.name,'PJB_01')
    def test_fullname(self):
        # Create libraries and check the full names
        lib = SolidLibrary('PB','PJB_01')
        self.assertEqual(lib.fullname,'PB/PJB_01')
        lib = SolidLibrary('PB',None)
        self.assertEqual(lib.fullname,'PB/')
        lib = SolidLibrary(None,'PJB_01')
        self.assertEqual(lib.fullname,'/PJB_01')
    def test_group(self):
        # Create libraries and check the group identifier
        lib = SolidLibrary('PB','PJB_01')
        self.assertEqual(lib.group,'PJB')
        lib = SolidLibrary('PB',None)
        self.assertEqual(lib.group,'PB')
        lib = SolidLibrary(None,'PJB_01')
        self.assertEqual(lib.group,'PJB')
    def test_timestamps(self):
        # No timestamps for 'empty' library
        lib = SolidLibrary('PB','PJB_01')
        self.assertEqual(lib.timestamps,[])
        # Add dummy fileset
        lib.add_file_set('9876543210',SolidPrimaryData())
        self.assertEqual(lib.timestamps,['9876543210'])
    def test_add_file_set(self):
        # Add a fileset to a library
        # !!!This isn't really a test!!!
        lib = SolidLibrary('PB','PJB_01')
        lib.add_file_set('9876543210',SolidPrimaryData())
    def test_get_file_sets(self):
        # Add and retrieve fileset to/from a library
        lib = SolidLibrary('PB','PJB_01')
        fs1 = SolidPrimaryData()
        lib.add_file_set('9876543210',fs1)
        # Retrieve from specific timestamp
        self.assertEqual(lib.get_file_sets('9876543210'),[fs1])
        # Retrieve all file sets
        self.assertEqual(lib.get_file_sets(),[fs1])

from curatus.solid import SolidDataDir
from bcftbx.test.test_SolidData import TestUtils
class TestSolidDataDir(unittest.TestCase):
    def setUp(self):
        # Create test directory
        self.se_dir = TestUtils().make_solid_dir('solid0123_20111014_FRAG_BC')
    def tearDown(self):
        # Remove test directory and contents
        utils.rmdir(self.se_dir)
    def test_libraries(self):
        # Check that SolidLibraries are returned
        libs = SolidDataDir(self.se_dir).libraries
        self.assertEqual(len(libs),12)
        for lib in libs:
            self.assertTrue(isinstance(lib,SolidLibrary))
    def test_library_groups(self):
        # Check that the correct groups are returned
        lib_groups = SolidDataDir(self.se_dir).library_groups
        self.assertEqual(lib_groups,['AB','CD','EF'])
    def test_libraries_in_group(self):
        # Check that the correct library names are returned
        self.assertEqual([l.name for l in SolidDataDir(self.se_dir).libraries_in_group('AB')],
                         ['AB_A1M1','AB_A1M1_input','AB_A1M2','AB_A1M2_input'])
        self.assertEqual([l.name for l in SolidDataDir(self.se_dir).libraries_in_group('CD')],
                         ['CD_PQ5','CD_ST4','CD_UV5'])
        self.assertEqual([l.name for l in SolidDataDir(self.se_dir).libraries_in_group('EF')],
                         ['EF11','EF12','EF13','EF14','EF15'])
    def test_report(self):
        # Not sure how to test the report method
        raise unittest.SkipTest("SolidDataDir.report not testable?")
    def test_match_primary_data(self):
        # Not sure how to test the matching method
        raise unittest.SkipTest("SolidDataDir.match_primary_data not testable?")

from curatus.solid import get_generic_name
class TestGetGenericName(unittest.TestCase):
    def test_get_generic_name(self):
        self.assertEqual(get_generic_name(utils.MockArchiveFile(
            "LH_POOL/results.F1B1/libraries/LH1/primary.20111208144829752/"
            "reads/solid0127_20111207_FRAG_BC_LH_POOL_BC_LH1")),
                         "solid0127_20111207_FRAG_BC_LH_POOL_BC_LH1")
        self.assertEqual(get_generic_name(utils.MockArchiveFile(
            "ZD_hu/results.F1B1/primary.20091220022109452/reads/"
            "solid0424_20091214_ZD_hu_F3")),
                         "solid0424_20091214_ZD_hu")
        self.assertEqual(get_generic_name(utils.MockArchiveFile(
            "SH_JC1_pool/results.F1B1/libraries_MM2/JC_SEQ30/"
            "primary.20120125063517232/reads/"
            "solid0127_20120117_PE_BC_SH_JC1_pool_F5-BC_JC_SEQ30")),
                         "solid0127_20120117_PE_BC_SH_JC1_pool_JC_SEQ30")

from curatus.solid import get_library_names_and_timestamps
class TesTGetLibraryNamesAndTimestamps(unittest.TestCase):
    def test_get_library_names_and_timestamps(self):
        smp,lib,ts = get_library_names_and_timestamps("LH_POOL/results.F1B1/libraries/"
                                                      "LH1/primary.20111208144829752/reads/"
                                                      "solid0127_20111207_FRAG_BC_LH_POOL_BC_LH1")
        self.assertEqual(smp,"LH_POOL")
        self.assertEqual(lib,"LH1")
        self.assertEqual(ts,"20111208144829752")
        smp,lib,ts = get_library_names_and_timestamps("ZD_hu/results.F1B1/"
                                                      "primary.20091220022109452/reads/"
                                                      "solid0424_20091214_ZD_hu_F3")
        self.assertEqual(smp,"ZD_hu")
        self.assertEqual(lib,None)
        self.assertEqual(ts,"20091220022109452")
        smp,lib,ts = get_library_names_and_timestamps("SH_JC1_pool/results.F1B1/"
                                                      "libraries_MM2/JC_SEQ30/"
                                                      "primary.20120125063517232/reads/"
                                                      "solid0127_20120117_PE_BC_SH_JC1_pool_F5-BC_JC_SEQ30")
        self.assertEqual(smp,"SH_JC1_pool")
        self.assertEqual(lib,"JC_SEQ30")
        self.assertEqual(ts,"20120125063517232")
