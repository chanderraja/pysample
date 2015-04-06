#!/usr/bin/python3
__author__ = 'craja@icbiome.com'

import sys

import sample_db
import sample_analysis


def main(argv):
    cnx = sample_db.open()
    sample_analysis.run(cnx)
    sample_db.close(cnx)

if __name__ == "__main__":
    main(sys.argv[1:])
