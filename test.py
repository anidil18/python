#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
from __future__ import print_function

import sys
import yaml


def main(argv):

  config_file = 'config.yml'

  with open(config_file) as stream:
      try:
        config = yaml.load(stream)
        print(config)
      except yaml.YAMLError as exc:
        print(exc)
        raise
        

if __name__ == "__main__":
  main(sys.argv[1:])
