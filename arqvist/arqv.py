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
# Formatting terminal output
#######################################################################

BLUE = '\033[94m'
GREEN = '\033[92m'
ORANGE = '\033[93m'
RED = '\033[91m'
BOLD = '\033[1m'
ENDC = '\033[0m'

def cprint(s,codes):
    if os.isatty(sys.stdout.fileno()):
        try:
            print "%s%s%s" % (''.join(codes),s,ENDC)
            return
        except Exception:
            pass
    print s

#######################################################################
# Main program
#######################################################################

def main(args=None):
    p = CommandParser()
    p.add_command('init',usage="%prog init [directory]")
    p.add_command('status',usage="%prog status [options] <pathspec>")
    p.add_command('diff',usage="%prog diff [options] <pathspec>")
    p.add_command('update',usage="%prog update [options] <pathspec>")
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
        existing_dirn = locate_cache_dir(dirn)
        if existing_dirn is not None:
            sys.stderr.write("\n%s: already initialised\n" % existing_dirn)
            sys.exit(1)
    else:
        dirn = locate_cache_dir(os.getcwd())
        if dirn is None:
            sys.stderr.write("fatal: Not an arqvist cache (or "
                             "any parent up to %s)\n" % os.sep)
            sys.exit(1)
        pathspec = args
    attributes = ['type',
                  'size',
                  'timestamp',
                  'uid',
                  'gid',
                  'mode',
                  'target',]
    if cmd == 'init':
        d = DirCache(dirn,include_checksums=options.checksums)
        d.save()
        dltd,mdfd,untrckd,unrdbl = d.status(dirn)
        if unrdbl:
            sys.stderr.write("\n%s: unreadable files found:\n" % dirn)
            for f in unrdbl:
                sys.stderr.write("\t%s\n" % f)
            sys.exit(1)
    elif cmd == 'status' or cmd == 'diff':
        d = DirCache(dirn)
        if options.target_dir is None:
            target_dir = dirn
        else:
            target_dir = os.path.abspath(options.target_dir)
            print "\nComparing with %s" % target_dir
        pathspec = map(lambda f:
                       os.path.relpath(f,target_dir)
                       if f.startswith(target_dir) else
                       (os.path.relpath(f,dirn)
                        if f.startswith(dirn) else f),
                       [os.path.abspath(f) for f in pathspec])
        if options.checksums:
            missing_checksums = []
            for f in d.files:
                if d[f].type is 'f' and d[f].md5 is None:
                    missing_checksums.append(f)
            if missing_checksums:
                print "\nMissing checksums:"
                for f in missing_checksums:
                    print "\t%s" % f
                sys.stderr.write("\n-c: unsafe, some checksums missing\n")
                sys.exit(1)
            attributes.append('md5')
        deleted,modified,untracked,unreadable = d.status(target_dir,
                                                         pathspec,
                                                         attributes)
        if cmd == 'status':
            if not (deleted or modified or untracked):
                msg = "\nno differences compared to cache"
                if unreadable:
                    msg += " (but unreadable files found)"
                print msg
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
                    cprint("\tdeleted:    %s" % f,BOLD+RED)
                for f in modified:
                    cprint("\tmodified:   %s" % f,BOLD+RED)
            if untracked:
                print
                print "Untracked files:"
                for f in untracked:
                    cprint("\t%s" % f,BOLD+BLUE)
            if unreadable:
                print
                print "Unreadable files:"
                for f in unreadable:
                    cprint("\t%s" % f,BOLD+RED)
        elif cmd == 'diff':
            if not (deleted or modified or untracked):
                sys.exit(0)
            if target_dir == d.dirn:
                normalised_paths = d.normalise_relpaths(modified,
                                                        workdir=os.getcwd())
            else:
                normalised_paths = d.normalise_relpaths(modified,
                                                        dirn=target_dir,
                                                        abspaths=True)
            for f,ff in zip(modified,normalised_paths):
                af = ArchiveFile(ff)
                cprint("arqv-diff a/%s b/%s" % (f,ff),BOLD)
                for attr in d[f].compare(ff,attributes):
                    cprint("old %s %s" % (attr,d[f][attr]),BOLD+GREEN)
                    cprint("new %s %s" % (attr,getattr(af,attr)),BOLD+RED)
    elif cmd == 'update':
        d = DirCache(dirn)
        deleted,modified,untracked,unreadable = d.status(dirn,
                                                          pathspec,
                                                          attributes)
        changed = (deleted or modified or untracked)
        if not d.exists:
            sys.stderr.write("%s: no cache on disk\n")
            sys.exit(1)
        if not changed and not options.checksums:
            sys.stderr.write("already up to date\n")
        else:
            d.update(pathspec,include_checksums=options.checksums)
            d.save()
            print "%s: cache updated" % dirn
        deleted,modified,untracked,unreadable = d.status(dirn)
        if unreadable:
            sys.stderr.write("\n%s: unreadable files found:\n" % dirn)
            for f in unreadable:
                sys.stderr.write("\t%s\n" % f)
            sys.exit(1)

if __name__ == '__main__':
    main()
