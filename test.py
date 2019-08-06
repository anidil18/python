#!/usr/bin/env python3
"""Test script"""

from __future__ import unicode_literals
from __future__ import print_function

import sys
import os
import time
import multiprocessing


def worker():
  print('start')
  time.sleep(2)
  print('end')


def main():
  """Main()"""

  for i in range(5):
    p = multiprocessing.Process(target=worker)
    p.start()


if __name__ == "__main__":
  main()
