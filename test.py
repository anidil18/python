#!/usr/bin/env python3
"""Test script"""

from __future__ import unicode_literals
from __future__ import print_function

import sys
import subprocess
import yaml


def main(argv):
  """Main()"""
  cmd = 'echo blabla'
  output = subprocess.Popen(cmd, shell=True, close_fds=True)
  print("output Popen: {}".format(output))

  output = subprocess.check_output(cmd.split())
  print("output check_output: {}".format(output))


if __name__ == "__main__":
  main(sys.argv[1:])
