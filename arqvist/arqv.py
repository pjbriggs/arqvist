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
from .cache import locate_cache_dir

#######################################################################
# Main program
#######################################################################

def main(args=None):
    p = CommandParser()
    p.add_command('init',usage="%prog init [directory]")
    p.add_command('status',usage="%prog status")
    p.add_command('diff',usage="%prog diff")
    p.add_command('update',usage="%prog update")
    for cmd in ('status','diff'):
        p.parser_for(cmd).add_option('-t','--target',action='store',
                                     dest='target_dir',default=None,
                                     help="check against TARGET_DIR")
        p.parser_for(cmd).add_option('-c','--checksums',action='store_true',
                                     dest='checksums',default=False,
                                     help="also check MD5 sums")
    for cmd in ('init','update'):
        p.parser_for(cmd).add_option('-c','--checksums',action='store_true',
                                     dest='checksums',default=False,
                                     help="also generate MD5 checksums")
    cmd,options,args = p.parse_args()
    if cmd == 'init':
        if len(args) == 0:
            dirn = os.getcwd()
        elif len(args) == 1:
            dirn = os.path.abspath(args[0])
        else:
            p.error("Usage: init [directory]")
        existing_dirn = locate_cache(dirn)
        if existing_dirn is not None:
            sys.stderr.write("\n%s: already initialised\n" % existing_dirn)
            sys.exit(1)
    else:
        dirn = locate_cache_dir(os.getcwd())
        if dirn is None:
            sys.stderr.write("fatal: Not an arqvist cache (or "
                             "any parent up to %s)\n" % os.sep)
            sys.exit(1)
    if cmd == 'init':
        d = DirCache(dirn,include_checksums=options.checksums)
        d.save()
    elif cmd == 'status' or cmd == 'diff':
        d = DirCache(dirn)
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
        if options.checksums:
            attributes.append('md5')
        deleted,modified,untracked = d.status(target_dir,
                                              attributes)
        if cmd == 'status':
            if not (deleted or modified or untracked):
                print
                print "no differences compared to cache"
            else:
                if target_dir == d.dirn:
                    deleted = d.normalise_relpaths(deleted,
                                                   workdir=os.getcwd())
                    modified = d.normalise_relpaths(modified,
                                                    workdir=os.getcwd())
                    untracked = d.normalise_relpaths(untracked,
                                                     workdir=os.getcwd())
                else:
                    deleted = d.normalise_relpaths(deleted,
                                                   dirn=target_dir,
                                                   abspaths=True)
                    modified = d.normalise_relpaths(modified,
                                                    dirn=target_dir,
                                                    abspaths=True)
                    untracked = d.normalise_relpaths(untracked,
                                                    dirn=target_dir,
                                                    abspaths=True)
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
            if target_dir == d.dirn:
                normalised_paths = d.normalise_relpaths(modified,
                                                        workdir=os.getcwd())
            else:
                normalised_paths = d.normalise_relpaths(modified,
                                                        dirn=target_dir,
                                                        abspaths=True)
            for f,ff in zip(modified,normalised_paths):
                af = ArchiveFile(ff)
                print "-- %s" % ff
                for attr in d[f].compare(ff,attributes):
                    print "   %s: %s != %s" % (attr,
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
