#!/bin/python
import fileinput
import argparse

def args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--file", nargs="+", required=True)
    return parser.parse_args()

def process(args):
    with fileinput.input(files=args.files) as f:

        for line in f:

if __name__ == "__main__":
    pargs = args()
    print("Parsing %d files into db (%s)" % (len(pargs.file), pargs.db))