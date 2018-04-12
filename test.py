#!/usr/bin/env python3
"""Test script"""

from __future__ import unicode_literals
from __future__ import print_function

import sys
import subprocess
from collections import Counter
import yaml


def counter(a):
  return sorted(Counter(a))


def main(argv):
  """Main()"""

  ora_errors = ['ORA-001', 'ORA-002', 'ORA-001']
  for key, value in sorted(counter(ora_errors).items()):
    print(key, value)


if __name__ == "__main__":
  main(sys.argv[1:])
