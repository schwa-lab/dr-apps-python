
import argparse
from functools import partial

ArgumentParser = partial(argparse.ArgumentParser, add_help=False)

STRING_AP = ArgumentParser()
STRING_AP.add_mutually_exclusive_group()
STRING_AP.add_argument('string', help='A string to evaluate')
STRING_AP.add_argument('-f', '--file', type=lambda x: argparse.FileType('rb')(x).read(), help='A file to read as the string to evaluate')
