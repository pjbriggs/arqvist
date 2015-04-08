#!/bin/env python
#
# Unit tests for the curatus package
import os
import bz2
import unittest
import tempfile
import shutil
import curatus
import curatus.core
import curatus.solid

# utils
#
# Class providing utility functions for making test directories,
# files and links for the unit tests
class utils:
    @classmethod
    def make_temp_dir(self):
        # Create a temporary directory
        return tempfile.mkdtemp()
    @classmethod
    def rmdir(self,dir_):
        # Remove a directory
        if os.path.exists(dir_):
            shutil.rmtree(dir_)
    @classmethod
    def make_file(self,filen,dirn=None,text=None,compress=None):
        # Create a new file
        if dirn is not None:
            filen = os.path.join(dirn,filen)
        if compress is None:
            fp = open(filen,'w')
        else:
            if compress == 'bz2':
                fp = bz2.BZ2File(filen,'w')
            else:
                raise Exception("%s: compression not implemented" % compress)
        if text is not None:
            fp.write("%s" % text)
        fp.close()
        return filen
    @classmethod
    def make_symlink(self,link,target,dirn=None):
        # Create a symbolic link
        if dirn is not None:
            link = os.path.join(dirn,link)
        os.symlink(target,link)
        return link

class TestArchiveFile(unittest.TestCase):
    def setUp(self):
        # Create test directory
        self.dir_ = utils.make_temp_dir()
    def tearDown(self):
        # Remove test directory and contents
        utils.rmdir(self.dir_)
    def test_basename(self):
        filen = utils.make_file('test.txt',dirn=self.dir_,text="This is some text")
        self.assertEqual(curatus.core.ArchiveFile(filen).basename,'test.txt')
    def test_classifier(self):
        filen = utils.make_file('test.txt',dirn=self.dir_)
        self.assertEqual(curatus.core.ArchiveFile(filen).classifier,'')
        self.assertEqual(curatus.core.ArchiveFile(self.dir_).classifier,'/')
    def test_get_md5sums(self):
        filen = utils.make_file('test.txt',dirn=self.dir_,text="This is some text")
        f = curatus.core.ArchiveFile(filen)
        self.assertEqual(f.md5,None)
        self.assertEqual(f.uncompressed_md5,None)
        self.assertEqual(f.get_md5sums(),('97214f63224bc1e9cc4da377aadce7c7',
                                          '97214f63224bc1e9cc4da377aadce7c7'))
        self.assertEqual(f.md5,'97214f63224bc1e9cc4da377aadce7c7')
        self.assertEqual(f.uncompressed_md5,'97214f63224bc1e9cc4da377aadce7c7')
    def test_get_md5sums_compressed_file(self):
        filen = utils.make_file('test.txt.bz2',dirn=self.dir_,text="This is some text",
                                compress='bz2')
        f = curatus.core.ArchiveFile(filen)
        self.assertEqual(f.md5,None)
        self.assertEqual(f.uncompressed_md5,None)
        self.assertEqual(f.get_md5sums(),('c032b31c8a39aaa53b0c6df004e95a64',
                                          '97214f63224bc1e9cc4da377aadce7c7'))
        self.assertEqual(f.md5,'c032b31c8a39aaa53b0c6df004e95a64')
        self.assertEqual(f.uncompressed_md5,'97214f63224bc1e9cc4da377aadce7c7')
    def test_compress(self):
        filen = utils.make_file('test.txt',dirn=self.dir_,text="This is some text")
        f = curatus.core.ArchiveFile(filen)
        self.assertEqual(f.compression,'')
        self.assertEqual(f.compress(),0)
        self.assertEqual(f.path,filen+'.bz2')
        self.assertEqual(f.compression,'bz2')
        self.assertEqual(f.get_md5sums(),('c032b31c8a39aaa53b0c6df004e95a64',
                                          '97214f63224bc1e9cc4da377aadce7c7'))
    def test_repr_(self):
        filen = utils.make_file('test.txt',dirn=self.dir_)
        f = curatus.core.ArchiveFile(filen)
        self.assertEqual(str(f),filen)

class TestArchiveSymlink(unittest.TestCase):
    def setUp(self):
        # Create test directory
        self.dir_ = utils.make_temp_dir()
        # Make files and symlinks for tests
        #
        # File to link to 'test.txt'
        self.filen = utils.make_file('test.txt',dirn=self.dir_)
        # Absolute and relative links to this file
        self.abslink = utils.make_symlink('abslink',self.filen,dirn=self.dir_)
        self.rellink = utils.make_symlink('rellink',os.path.basename(self.filen),
                                          dirn=self.dir_)
        # A broken link (relative)
        self.brklink = utils.make_symlink('brklink','missing.txt',
                                          dirn=self.dir_)
        # A bzipped file and a broken link that points to the uncompressed
        # file name (an example of an "alternative target")
        self.bzfilen = utils.make_file('test2.txt.bz2',dirn=self.dir_,
                                       compress='bz2')
        self.altlink = utils.make_symlink('altlink','test2.txt',dirn=self.dir_)
        # A link that points outside of the temp dir (broken)
        extfilen = os.path.normpath(os.path.join(self.dir_,'..','elsewhere','test3.txt'))
        self.extlink = utils.make_symlink('extlink',extfilen,dirn=self.dir_)
    def tearDown(self):
        # Remove test directory and contents
        utils.rmdir(self.dir_)
    def test_external_to(self):
        self.assertFalse(curatus.core.ArchiveSymlink(self.abslink).external_to(self.dir_))
        self.assertTrue(curatus.core.ArchiveSymlink(self.extlink).external_to(self.dir_))
    def test_rebase(self):
        s = curatus.core.ArchiveSymlink(self.rellink)
        s.rebase(self.dir_,'/some/where/else')
        self.assertEqual(os.readlink(self.rellink),os.path.basename(self.filen))
        s = curatus.core.ArchiveSymlink(self.abslink)
        s.rebase(self.dir_,'/some/where/else')
        self.assertEqual(os.readlink(self.abslink),'/some/where/else/test.txt')
    def test_make_relative(self):
        s = curatus.core.ArchiveSymlink(self.abslink)
        self.assertTrue(s.is_absolute)
        s.make_relative()
        self.assertFalse(s.is_absolute)
        self.assertEqual(s.target,'test.txt')
    def test_alternative_target(self):
        self.assertEqual(curatus.core.ArchiveSymlink(self.abslink).alternative_target,None)
        self.assertEqual(curatus.core.ArchiveSymlink(self.brklink).alternative_target,None)
        self.assertEqual(curatus.core.ArchiveSymlink(self.altlink).alternative_target,
                         self.bzfilen)
    def test_classifier(self):
        self.assertEqual(curatus.core.ArchiveSymlink(self.abslink).classifier,'A-')
        self.assertEqual(curatus.core.ArchiveSymlink(self.rellink).classifier,'r-')
        self.assertEqual(curatus.core.ArchiveSymlink(self.brklink).classifier,'rX')
        self.assertEqual(curatus.core.ArchiveSymlink(self.altlink).classifier,'rx')

class TestDataDir(unittest.TestCase):
    def setUp(self):
        # Create test directory
        self.dir_ = utils.make_temp_dir()
    def tearDown(self):
        # Remove test directory and contents
        utils.rmdir(self.dir_)
    def test_init_cache(self):
        raise NotImplementedError
    def test_has_cache(self):
        raise NotImplementedError
    def test_update_cache(self):
        raise NotImplementedError
    def test_write_cache(self):
        raise NotImplementedError
    def test_name(self):
        self.assertEqual(curatus.core.DataDir(self.dir_).name,
                         os.path.basename(self.dir_))
    def test_path(self):
        self.assertEqual(curatus.core.DataDir(self.dir_).path,self.dir_)
    def test_files(self):
        raise NotImplementedError
    def test_list_files(self):
        raise NotImplementedError
    def test_list_symlinks(self):
        raise NotImplementedError
    def test_list_temp(self):
        raise NotImplementedError
    def test_list_related_dirs(self):
        raise NotImplementedError
    def test_md5sums(self):
        raise NotImplementedError
    def test_set_permissions(self):
        raise NotImplementedError
    def test_info(self):
        raise NotImplementedError
    def test_copy_to(self):
        raise NotImplementedError

class TestStripExtensions(unittest.TestCase):
    # Tests for the curatus.core.strip_extensions function
    def test_strip_extensions(self):
        self.assertEqual(curatus.core.strip_extensions('test'),'test')
        self.assertEqual(curatus.core.strip_extensions('test.bz2'),'test')
        self.assertEqual(curatus.core.strip_extensions('test.fastq.bz2'),'test')

class TestGetFileExtensions(unittest.TestCase):
    # Tests for the curatus.core.get_file_extensions function
    def test_get_file_extensions(self):
        self.assertEqual(curatus.core.get_file_extensions('test'),('',''))
        self.assertEqual(curatus.core.get_file_extensions('test.bz2'),('','bz2'))
        self.assertEqual(curatus.core.get_file_extensions('test.fastq'),('fastq',''))
        self.assertEqual(curatus.core.get_file_extensions('test.fastq.gz'),('fastq','gz'))
        self.assertEqual(curatus.core.get_file_extensions('test.file.fastq.gz'),('fastq','gz'))

class TestGetSize(unittest.TestCase):
    # Tests for the curatus.core.get_size function
    def setUp(self):
        # Create test directory
        self.dir_ = utils.make_temp_dir()
    def tearDown(self):
        # Remove test directory and contents
        utils.rmdir(self.dir_)
    def test_get_size_for_empty_dir(self):
        self.assertEqual(curatus.core.get_size(self.dir_),4096)
    def test_get_size_for_file(self):
        filen = utils.make_file('test.txt',dirn=self.dir_,text="This is some text")
        self.assertEqual(curatus.core.get_size(filen),os.stat(filen).st_size)
    def test_get_size_for_dir(self):
        filen = utils.make_file('test.txt',dirn=self.dir_,text="This is some text")
        self.assertEqual(curatus.core.get_size(self.dir_),os.stat(filen).st_size + 4096)

class TestConvertSize(unittest.TestCase):
    # Tests for the curatus.core.convert_size function
    def test_convert_size(self):
        self.assertEqual(curatus.core.convert_size('1b'),1.0)
        self.assertEqual(curatus.core.convert_size('1K'),1024.0)
        self.assertEqual(curatus.core.convert_size('1M'),1048576.0)
        self.assertEqual(curatus.core.convert_size('1G'),1073741824.0)
        self.assertEqual(curatus.core.convert_size('1T'),1099511627776.0)


        
    
