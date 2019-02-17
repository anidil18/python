#!/usr/bin/env python3
"""Test script"""

from __future__ import unicode_literals
from __future__ import print_function

import sys

DEFAULT_REST_CLONE_PARAMETER = [
    'current_user', 'method_name', 'backup_name', 'rman_until_time',
    'schedule_at_timespec', 'step'
]

def main(argv):
  """Main()"""

  (target_sg, target_devs) = None
  print(target_devs)


if __name__ == "__main__":
  main(sys.argv[1:])
