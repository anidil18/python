#!/usr/bin/env python
"""
  Epic script to rename scary uppercase extensions (like .JPG) to
  more friendly lowercase (like .jpg).
"""

from optparse import OptionParser
import os
import sys

def recursive_walk(path):
    for root, dirs, files in os.walk(path):
        for name in files:
            yield os.path.join(root, name)
 
def simple_walk(path):
    for name in os.listdir(path):
        if os.path.isfile(name):
            yield os.path.join(path, name)
 
def fix(name):
    base, ext = os.path.splitext(name)
    return base + ext.lower()
 
def main():
    parser = OptionParser(usage='usage: %prog [options] [DIR...]')
    parser.add_option('-d', '--no-recursion', action='store_false',
                      dest='recursion', default=True,
                      help="Don't recurse into subdirectories")
    parser.add_option('-n', '--dry-run', action='store_true',
                      dest='dry_run', default=False,
                      help="Dry run: test it without actually renaming anything")

    options, args = parser.parse_args()
     
    if len(args) == 0:
        paths = [os.getcwd()]
    else:
        paths = args

    if options.recursion:
        walker = recursive_walk
    else:
        walker = simple_walk

    dry_run = options.dry_run

    n_renamed = 0
    n_errors = 0

    for path in paths:
        for name in walker(path):
            new_name = fix(name)
            if name != new_name:
                if os.path.exists(new_name):
                    print ("** Warning: destination '%s' already exists" % new_name, file=sys.stderr)
                    n_errors += 1
                else:
                    print (new_name)
                    if not dry_run:
                        os.rename(name, new_name)
                    n_renamed += 1

    print ("%d files renamed successfully, with %d errors" % (n_renamed, n_errors), file=sys.stderr)

    if n_errors > 0:
        sys.exit(1)

if __name__ == '__main__':
    main()
