"""drops a column from a CSV file"""
#!env python

import argparse
import pandas as pd

parser = argparse.ArgumentParser(description="Drop a column from a CSV file")
parser.add_argument("inputcsvfile", help="CSV file to drop column from")
parser.add_argument("outputcsvfile", help="CSV file to drop column from")
parser.add_argument("column", help="column to drop")
args = parser.parse_args()

df = pd.read_csv(args.inputcsvfile)
if args.column in df.columns:
    df.drop(columns=[args.column], inplace=True)
df.to_csv(args.outputcsvfile, index=False)
