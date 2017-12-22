#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Create searchable PDF

Usage:
  create_searchable_pdf.py [options] <pdffile>...
  create_searchable_pdf.py -h | --help
  create_searchable_pdf.py --version

Options:
  -h --help                   Show this screen.
  --version                   Show version.
  --nocheck                   Do not check if pdf file is already searchable.
  -r DPK --resolution=DPI     DPI resolution for OCR tiff file [default: 300]
"""

import subprocess
import sys
import logging
import os
import time
import tempfile
import shutil
from docopt import docopt
try:
  from os import scandir, walk
except ImportError:
  from scandir import scandir, walk

# debug logging
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)  # logging.DEBUG

# pdf file suffix
FILE_SUFFIX = "ocr"

"""
External links:

- http://www.morethantechnical.com/2013/11/21/\
creating-a-searchable-pdf-with-opensource-tools-ghostscript-hocr2pdf-and-tesseract-ocr/
- https://gist.github.com/udokmeci/f0805cca0548a87d5560
- https://github.com/tesseract-ocr/tesseract/wiki/ImproveQuality
"""

""" OCR
- minimalne 300 DPI pro TIFF, jinak ma tessaract mizerny vysledky
"""

"""
requirements.txt:

brew rm tesseract
brew install tesseract --with-all-languages
brew install ghostscript
"""


def run_command(arglist):
  """run os command
     :param arglist: os program with parameters to run
  """
  logging.debug("running %s", ' '.join(arglist))
  try:
    sp = subprocess.Popen(args=arglist,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)

  except:
    print(sys.exc_info())
    sys.exit("error executing ('%s')" % arglist[0])

  sp.wait()

  no_of_lines = 0
  for line in sp.stdout.readlines():
    no_of_lines += 1
    logging.debug(line)

  logging.debug("Number of output lines: %s", no_of_lines)
  return no_of_lines


def check_if_searchable(file):
  """check if the tile is already processed

  pokud pdf obsahuje fonty, pak ho povazuj za jiz konvertovane
  """

  arglist = ['pdffonts', file]
  no_of_lines = run_command(arglist)

  if no_of_lines >= 3:
    print("file %s seems to be searchable, skipping..." % file)
    return False
  return True


def split_pdf_to_images(file, tempdir, resolution):
  """ split dpf into TIFF pages for OCR processing
  Preved pdf soubor do png obrazku pro dalsi konverzi do OCR
  """

  tiff_file = os.path.join(tempdir, os.path.basename(file) + ".tif")

  logging.debug("tiff: %s" % tiff_file)

  arglist = ['gs',
             "-dSAFER",
             "-dBATCH",
             "-dNOPAUSE",
             "-sDEVICE=tiffgray",
             "-r%s" % resolution,
             "-sOutputFile=" + tiff_file,
             file]

  run_command(arglist)


def tesseract(file, tempdir):
  """ run OCR on pdf
  tesseract -l CES page.tif file_searchable pdf
  """

  tiff_file = os.path.join(tempdir, os.path.basename(file) + ".tif")

  basename, _fileext = os.path.splitext(file)

  # output pdf file set to name "searchable"
  arglist = [
      'tesseract', "-l", "CES", tiff_file,
      os.path.join(tempdir, "_".join(basename.split())), "pdf"
  ]
  run_command(arglist)


def pdf_reduce_size(file, tempdir):
  """reduce PDF size"""

  basename, _fileext = os.path.splitext(file)
  input_file = os.path.join(tempdir, "_".join(basename.split()) + '.pdf')
  output_file = "_".join(basename.split()) + '_' + FILE_SUFFIX + '.pdf'

  cmd = '''gs -sDEVICE=pdfwrite -dCompatibilityLevel=1.4 -dPDFSETTINGS=/ebook
           -dNOPAUSE -dQUIET -dBATCH -sOutputFile={} {}
        '''.format(output_file, input_file)
  run_command(cmd.split())


def extract_text(pdffile):
  """ extract text from pdffile
  pdftotext -enc UTF-8 <file> -
  """
  arglist = ['pdftotext',
             "-enc",
             "UTF-8",
             pdffile,
             "-"]
  run_command(arglist)


def main(arguments):

  logging.debug("args: %s", arguments)

  skipped_files = []
  processed_files = []

  for pdffile in arguments['<pdffile>']:
    print("processing: %s" % pdffile)

    if not os.path.isfile(pdffile):
      raise ValueError('file does not exists', pdffile)

    try:
      # vytvor working directory
      tempdir = tempfile.mkdtemp()
      logging.debug("working dir: %s", tempdir)

      # check if file is already searchable
      if not arguments['--nocheck']:
        do_convert = check_if_searchable(pdffile)
      else:
        do_convert = True

      if do_convert:

        # split PDF into images to temporary directory
        split_pdf_to_images(pdffile, tempdir, arguments['--resolution'])

        # run tesseract to OCR extract text
        tesseract(pdffile, tempdir)

        pdf_reduce_size(pdffile, tempdir)

        # rename origin file
        new_filename = pdffile + '_' + time.strftime("%Y%m%d_%H%M%S")
        logging.debug("rename %s to %s" % (pdffile, new_filename))
        os.rename(pdffile, new_filename)

        # extract text from PDF
        extract_text(pdffile)
        processed_files.append(pdffile)

      else:
        # add file to skipped_files
        skipped_files.append(pdffile)

    finally:
      # uklid po sobe docasny soubory
      logging.debug("working dir cleanup: %s", tempdir)
      shutil.rmtree(tempdir)

  # print finall summary
  print("processed files: %s" % processed_files)
  print("skipped files: %s" % skipped_files)


if __name__ == "__main__":
  arguments = docopt(__doc__)
  main(arguments)
