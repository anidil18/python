#!/usr/bin/env python3
"""Test script"""

from __future__ import unicode_literals
from __future__ import print_function

import sys
import subprocess


def main(argv):
  """Main()"""

  cfg = {
      'variables': {
          'database': None,
          'app': None,
          'user': 'SYS'},
      'stage': ['deploy'],
      'script': [],
      'jira': None}

  if 'db' in cfg['variables']:
    print('aa')

if __name__ == "__main__":
  main(sys.argv[1:])
