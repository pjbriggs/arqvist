#!/usr/bin/env python
#
#     arqv: CLI utility for managing copies of data
#     Copyright (C) University of Manchester 2016 Peter Briggs
#
import os
import sys
from bcftbx.cmdparse import CommandParser
from .core import ArchiveFile
from .cache import DirCache

#######################################################################
# Main program
#######################################################################

def main(args=None):
    p = CommandParser()
    p.add_command('init')
    p.add_command('status')
    p.add_command('diff')
    for cmd in ('status','diff'):
        p.parser_for(cmd).add_option('-t','--target',action='store',
                                     dest='target_dir',default=None,
                                     help="check against TARGET_DIR")
    p.add_command('update')
    for cmd in ('init','update'):
        p.parser_for(cmd).add_option('-c','--checksums',action='store_true',
                                     dest='checksums',default=False,
                                     help="also generate MDS checksums")
    cmd,options,args = p.parse_args()
    if len(args) == 0:
        dirn = os.getcwd()
    elif len(args) == 1:
        dirn = os.path.abspath(args[0])
    else:
        sys.stderr.write("Usage: %s [DIR]\n" % cmd)
        sys.exit(1)
    if cmd == 'init':
        d = DirCache(dirn,include_checksums=options.checksums)
        if d.exists:
            sys.stderr.write("\n%s: already initialised\n" % dirn)
            sys.exit(1)
        d.save()
    elif cmd == 'status' or cmd == 'diff':
        d = DirCache(dirn)
        if not d.exists:
            sys.stderr.write("\n%s: no cache on disk\n" % dirn)
            sys.exit(1)
        print dirn
        if options.target_dir is None:
            target_dir = dirn
        else:
            target_dir = os.path.abspath(options.target_dir)
            print "\nComparing with %s" % target_dir
        attributes = ['type',
                      'size',
                      'timestamp',
                      'uid',
                      'gid',
                      'mode',]
        deleted,modified,untracked = d.status(target_dir,
                                              attributes)
        if cmd == 'status':
            if not (deleted and modified and untracked):
                print
                print "no differences compared to cache"
            if deleted or modified:
                print
                print "Changes to tracked files:"
                for f in deleted:
                    print "\tdeleted:    %s" % f
                for f in modified:
                    print "\tmodified:   %s" % f
            if untracked:
                print
                print "Untracked files:"
                for f in untracked:
                    print "\t%s" % f
        elif cmd == 'diff':
            for f in modified:
                f0 = os.path.join(target_dir,f)
                af = ArchiveFile(f0)
                print "%s:" % f
                for attr in d[f].compare(f0,attributes):
                    print "\t%s: %s != %s" % (attr,
                                              getattr(af,attr),
                                              d[f][attr])
    elif cmd == 'update':
        d = DirCache(dirn)
        if not d.exists:
            sys.stderr.write("%s: no cache on disk\n")
            sys.exit(1)
        if not d.is_stale and not options.checksums:
            sys.stderr.write("already up to date\n")
        else:
            d.update(include_checksums=options.checksums)
            d.save()
            print "%s: cache updated" % dirn

if __name__ == '__main__':
    main()
