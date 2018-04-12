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

  ora_errors = ["SP2-0003: Ill-formed ACCEPT command starting as LANSWER FORMAT A1 PROMPT 'Do you wish to continue anyway? (y\\n): '", 'ORA-00942: table or view does not exist', 'ORA-00942: table or view does not exist', 'ORA-00942: table or view does not exist']
  for key, value in counter(ora_errors).items():
    print(key, value)


if __name__ == "__main__":
  main(sys.argv[1:])
