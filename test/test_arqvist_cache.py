#!/bin/env python
#
# Unit tests for the arqvist cache module
import unittest
import dateutil
import utils
import os

from arqvist.cache import FILE_ATTRIBUTES
from arqvist.cache import DirCache
from arqvist.cache import CacheFile



# DirCache

fq_r1 = """@NB500968:10:H57NTAFXX:1:11101:4210:1091 1:N:0:CGGCAGAA+NCGATCTA
CTCCAGTTCTGAGTAACTTCAAGGGCTCCGCCTCTCCACAGCGCAGCCCC
+
AAAAAEEEEEEEEE6/EEEA//EE/EAEAEEEEEEEE/</E/6EEAEAAA
"""
fq_r2 = """@NB500968:10:H57NTAFXX:1:11101:4210:1091 2:N:0:CGGCAGAA+NCGATCTA
GGGGCTGCTCTGTGGAGAGGCGGAGCCCTTGAAGTTACTCAGAACTGGAG
+
AAAAAEEEAEEAEEEE66AEEAA/EEEEAAE</E6EEEEEEE/EEEEE<A
"""

class TestDirCache(unittest.TestCase):
    def setUp(self):
        # Create test directory
        self.dirn = utils.make_temp_dir()
        # Populate test directory
        utils.make_file('README.txt',dirn=self.dirn,
                        text="This is a README")
        fastqs = utils.make_subdir(self.dirn,'fastqs')
        utils.make_file('PJB_S1_R1.fastq',dirn=fastqs,text=fq_r1)
        utils.make_file('PJB_S1_R2.fastq',dirn=fastqs,text=fq_r2)

    def tearDown(self):
        # Remove test directory and contents
        utils.rmdir(self.dirn)

    def test_create_dircache(self):
        """
        create a new DirCache instance
        """
        dircache = DirCache(self.dirn)
        self.assertEqual(len(dircache),4)
        self.assertEqual(dircache.files,
                         ['README.txt',
                          'fastqs',
                          'fastqs/PJB_S1_R1.fastq',
                          'fastqs/PJB_S1_R2.fastq'])
        self.assertFalse(dircache.exists)
        self.assertFalse(dircache.is_stale)
        self.assertTrue(isinstance(dircache['README.txt'],CacheFile))
        self.assertTrue(isinstance(dircache['fastqs'],CacheFile))
        self.assertEqual(dircache['README.txt'].type,'f')
        self.assertEqual(dircache['fastqs'].type,'d')
        self.assertEqual(dircache['README.txt'].ext,'txt')
        self.assertEqual(dircache['README.txt'].md5,None)
        self.assertEqual(dircache['README.txt'].uncompressed_md5,None)

    def test_create_dircache_with_checksums(self):
        """
        create a new DirCache instance with MD5 checksums
        """
        dircache = DirCache(self.dirn,include_checksums=True)
        self.assertEqual(len(dircache),4)
        self.assertEqual(dircache.files,
                         ['README.txt',
                          'fastqs',
                          'fastqs/PJB_S1_R1.fastq',
                          'fastqs/PJB_S1_R2.fastq'])
        self.assertFalse(dircache.exists)
        self.assertFalse(dircache.is_stale)
        self.assertTrue(isinstance(dircache['README.txt'],CacheFile))
        self.assertTrue(isinstance(dircache['fastqs'],CacheFile))
        self.assertEqual(dircache['README.txt'].type,'f')
        self.assertEqual(dircache['fastqs'].type,'d')
        self.assertEqual(dircache['README.txt'].ext,'txt')
        self.assertEqual(dircache['README.txt'].md5,
                         '257dd8890919396864a586ae0f64e5f5')
        self.assertEqual(dircache['README.txt'].uncompressed_md5,
                         '257dd8890919396864a586ae0f64e5f5')
        self.assertEqual(dircache['fastqs/PJB_S1_R1.fastq'].md5,
                         'c709762462dc109c74825d8c42c4dce8')
        self.assertEqual(dircache['fastqs/PJB_S1_R2.fastq'].md5,
                         '4483943eef534229a9ab426681cdd47c')

    def test_dircache_exists(self):
        """
        check Dircache.exists property works as expected
        """
        # No cache
        self.assertFalse(DirCache(self.dirn).exists)
        # Cache dir but no files
        os.mkdir(os.path.join(self.dirn,'.arqvist'))
        self.assertFalse(DirCache(self.dirn).exists)
        # Cache dir with files
        with open(os.path.join(self.dirn,'.arqvist','files'),'w') as fp:
            fp.write('')
        self.assertTrue(DirCache(self.dirn).exists)

    def test_save_dircache(self):
        """
        create and save DirCache instance
        """
        dircache = DirCache(self.dirn)
        self.assertFalse(dircache.exists)
        dircache.save()
        self.assertTrue(dircache.exists)
        self.assertTrue(os.path.isdir(dircache.cachedir))
        self.assertTrue(os.path.exists(os.path.join(dircache.cachedir,
                                                    'files')))
        self.assertFalse(dircache.is_stale)
        deleted,modified,untracked = dircache.status()
        self.assertEqual(deleted,[])
        self.assertEqual(modified,[])
        self.assertEqual(untracked,[])

    def test_load_dircache(self):
        """
        load DirCache instance from disk
        """
        dircache = DirCache(self.dirn)
        dircache.save()
        del(dircache)
        dircache2 = DirCache(self.dirn)
        self.assertTrue(dircache2.exists)
        self.assertFalse(dircache2.is_stale)
        deleted,modified,untracked = dircache2.status()
        self.assertEqual(deleted,[])
        self.assertEqual(modified,[])
        self.assertEqual(untracked,[])

    def test_dircache_status_change_file(self):
        """
        check 'status' detects changed file compared to DirCache
        """
        # Create dir cache and save
        dircache = DirCache(self.dirn)
        dircache.save()
        del(dircache)
        # Reload data
        dircache2 = DirCache(self.dirn)
        self.assertTrue(dircache2.exists)
        self.assertFalse(dircache2.is_stale)
        deleted,modified,untracked = dircache2.status()
        self.assertEqual(deleted,[])
        self.assertEqual(modified,[])
        self.assertEqual(untracked,[])
        # Modify a file
        utils.make_file('README.txt',dirn=self.dirn,
                        text="README has been updated")
        self.assertTrue(dircache2.is_stale)
        deleted,modified,untracked = dircache2.status()
        self.assertEqual(deleted,[])
        self.assertEqual(modified,['README.txt'])
        self.assertEqual(untracked,[])

    def test_dircache_status_add_file(self):
        """
        check 'status' detects new files compared to DirCache
        """
        # Create dir cache and save
        dircache = DirCache(self.dirn)
        dircache.save()
        del(dircache)
        # Reload data
        dircache2 = DirCache(self.dirn)
        self.assertTrue(dircache2.exists)
        self.assertFalse(dircache2.is_stale)
        deleted,modified,untracked = dircache2.status()
        self.assertEqual(deleted,[])
        self.assertEqual(modified,[])
        self.assertEqual(untracked,[])
        # Add a new directory and file
        analysis = utils.make_subdir(self.dirn,'analysis')
        utils.make_file('analysis.log',dirn=analysis,
                        text="Output from analysis prog v1.0")
        self.assertTrue(dircache2.is_stale)
        deleted,modified,untracked = dircache2.status()
        self.assertEqual(deleted,[])
        self.assertEqual(modified,[])
        self.assertEqual(untracked,['analysis',
                                    'analysis/analysis.log'])

    def test_dircache_status_delete_file(self):
        """
        check 'status' detects deleted files compared to DirCache
        """
        # Create dir cache and save
        dircache = DirCache(self.dirn)
        dircache.save()
        del(dircache)
        # Reload data
        dircache2 = DirCache(self.dirn)
        self.assertTrue(dircache2.exists)
        self.assertFalse(dircache2.is_stale)
        deleted,modified,untracked = dircache2.status()
        self.assertEqual(deleted,[])
        self.assertEqual(modified,[])
        self.assertEqual(untracked,[])
        # Remove an existing file
        os.remove(os.path.join(self.dirn,'README.txt'))
        self.assertTrue(dircache2.is_stale)
        deleted,modified,untracked = dircache2.status()
        self.assertEqual(deleted,['README.txt'])
        self.assertEqual(modified,[])
        self.assertEqual(untracked,[])

    def test_dircache_update(self):
        """
        update of DirCache data on disk
        """
        # Create dir cache and save
        dircache = DirCache(self.dirn)
        dircache.save()
        del(dircache)
        # Reload data and modify items
        dircache2 = DirCache(self.dirn)
        utils.make_file('README.txt',dirn=self.dirn,
                        text="README has been updated")
        analysis = utils.make_subdir(self.dirn,'analysis')
        utils.make_file('analysis.log',dirn=analysis,
                        text="Output from analysis prog v1.0")
        os.remove(os.path.join(self.dirn,'fastqs','PJB_S1_R1.fastq'))
        self.assertTrue(dircache2.is_stale)
        deleted,modified,untracked = dircache2.status()
        self.assertEqual(deleted,['fastqs/PJB_S1_R1.fastq'])
        self.assertEqual(modified,['README.txt'])
        self.assertEqual(untracked,['analysis',
                                    'analysis/analysis.log'])
        # Update the cache on disk
        dircache2.update()
        dircache2.save()
        self.assertFalse(dircache2.is_stale)
        del(dircache2)
        # Reload data and check again
        dircache3 = DirCache(self.dirn)
        self.assertFalse(dircache3.is_stale)
        deleted,modified,untracked = dircache3.status()
        self.assertEqual(deleted,[])
        self.assertEqual(modified,[])
        self.assertEqual(untracked,[])

    def test_dircache_update_checksums(self):
        """
        update DirCache data to include MD5 checksums
        """
        # Create dir cache without checksums
        dircache = DirCache(self.dirn)
        self.assertEqual(dircache['README.txt'].md5,None)
        self.assertEqual(dircache['README.txt'].uncompressed_md5,None)
        # Update with checksums
        dircache.update(include_checksums=True)
        # Check that MD5s have been added
        self.assertEqual(dircache['README.txt'].md5,
                         '257dd8890919396864a586ae0f64e5f5')
        self.assertEqual(dircache['README.txt'].uncompressed_md5,
                         '257dd8890919396864a586ae0f64e5f5')

    def test_dircache_file_with_leading_hash(self):
        """
        DirCache handles files with leading hash in name (e.g. '#test')
        """
        # Create file with leading hash
        utils.make_file('#test',dirn=self.dirn,text="Bad file name?")
        # Create dir cache
        dircache = DirCache(self.dirn)
        dircache.save()
        del(dircache)
        # Reload data and modify items
        dircache2 = DirCache(self.dirn)

    def test_dircache_with_broken_symlink(self):
        """
        DirCache handles broken symlinks
        """
        # Create broken symlink
        utils.make_symlink('bad_link','this/is/missing',dirn=self.dirn)
        # Create dir cache
        dircache = DirCache(self.dirn)
        dircache.save()
        del(dircache)
        # Reload data and modify items
        dircache2 = DirCache(self.dirn)
        deleted,modified,untracked = dircache2.status()
        self.assertEqual(deleted,[])
        self.assertEqual(modified,[])
        self.assertEqual(untracked,[])
        self.assertFalse(dircache2.is_stale)

# CacheFile

class TestCacheFile(unittest.TestCase):
    def test_create_empty_cachefile(self):
        """
        create an 'empty' CacheFile instance
        """
        cachefile = CacheFile('test/file.txt')
        self.assertEqual(cachefile.attributes,FILE_ATTRIBUTES)
        for attr in FILE_ATTRIBUTES:
            if attr == 'relpath':
                self.assertEqual(cachefile[attr],'test/file.txt')
                self.assertEqual(getattr(cachefile,attr),'test/file.txt')
            elif attr == 'basename':
                self.assertEqual(cachefile[attr],'file.txt')
                self.assertEqual(getattr(cachefile,attr),'file.txt')
            elif attr == 'ext':
                self.assertEqual(cachefile[attr],'txt')
                self.assertEqual(getattr(cachefile,attr),'txt')
            elif attr == 'compression':
                self.assertEqual(cachefile[attr],'')
                self.assertEqual(getattr(cachefile,attr),'')
            else:
                self.assertEqual(cachefile[attr],None)
                self.assertEqual(getattr(cachefile,attr),None)

    def test_create_cachefile_with_values(self):
        """
        create a CacheFile instance with some attributes set
        """
        cachefile = CacheFile('test/file.txt.gz',
                              type='f',
                              size='537',
                              timestamp='2016-03-22 13:15:47.955909',
                              mode='0664',
                              owner='1000',
                              group='1001')
        self.assertEqual(cachefile.attributes,FILE_ATTRIBUTES)
        self.assertEqual(cachefile['relpath'],'test/file.txt.gz')
        self.assertEqual(getattr(cachefile,'relpath'),'test/file.txt.gz')
        self.assertEqual(cachefile['basename'],'file.txt.gz')
        self.assertEqual(getattr(cachefile,'basename'),'file.txt.gz')
        self.assertEqual(cachefile['ext'],'txt')
        self.assertEqual(getattr(cachefile,'ext'),'txt')
        self.assertEqual(cachefile['compression'],'gz')
        self.assertEqual(getattr(cachefile,'compression'),'gz')
        self.assertEqual(cachefile['type'],'f')
        self.assertEqual(getattr(cachefile,'type'),'f')
        self.assertEqual(cachefile['size'],537)
        self.assertEqual(getattr(cachefile,'size'),537)
        self.assertEqual(cachefile['timestamp'],
                         dateutil.parser.parse('2016-03-22 13:15:47.955909'))
        self.assertEqual(getattr(cachefile,'timestamp'),
                         dateutil.parser.parse('2016-03-22 13:15:47.955909'))
        self.assertEqual(cachefile['mode'],'0664')
        self.assertEqual(getattr(cachefile,'mode'),'0664')
        self.assertEqual(cachefile['owner'],'1000')
        self.assertEqual(getattr(cachefile,'owner'),'1000')
        self.assertEqual(cachefile['group'],'1001')
        self.assertEqual(getattr(cachefile,'group'),'1001')

    def test_cachefile_is_stale(self):
        """
        check that 'is_stale' method works correctly
        """
        cachefile = CacheFile('test/file.txt',
                              type='f',
                              size='537',
                              timestamp='2016-03-22 13:15:47.955909')
        self.assertFalse(cachefile.is_stale(537,
                        dateutil.parser.parse('2016-03-22 13:15:47.955909')))
        self.assertTrue(cachefile.is_stale(478,
                        dateutil.parser.parse('2016-03-22 13:15:47.955909')))
        self.assertTrue(cachefile.is_stale(537,
                        dateutil.parser.parse('2016-03-22 13:16:22.087889')))
        self.assertTrue(cachefile.is_stale(478,
                        dateutil.parser.parse('2016-03-22 13:16:22.087889')))

