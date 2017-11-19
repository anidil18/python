#!/usr/bin/env python3
"""Test script"""

from __future__ import unicode_literals
from __future__ import print_function

import sys
import yaml


def main(argv):

  config_file = 'oracle-ci.yml'

  try:
    with open(config_file, 'r') as stream:
      cfg = yaml.load(stream)
      for f in cfg['script']:
        print(f)
  except (IOError, yaml.YAMLError) as exc:
    print(exc)
    raise


if __name__ == "__main__":
  main(sys.argv[1:])
