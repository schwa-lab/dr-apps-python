
import sys
import argparse
from functools import partial
from brownie.importing import import_string
from .api import DECORATE_METHOD

ArgumentParser = partial(argparse.ArgumentParser, add_help=False)

ISTREAM_AP = ArgumentParser(add_help=False)
ISTREAM_AP.add_argument('--in-file', metavar='PATH', dest='in_stream', type=argparse.FileType('rb'), default=sys.stdin, help='The input file (default: STDIN)')

DESERIALISE_AP = ArgumentParser(parents=(ISTREAM_AP,), add_help=False)
DESERIALISE_AP.add_argument('--doc-class', metavar='CLS', dest='doc_class', type=import_string, help='Import path to the Document class for the input.  If available, doc.{0}() will be called for each document on the stream.'.format(DECORATE_METHOD))

OSTREAM_AP = ArgumentParser(add_help=False)
OSTREAM_AP.add_argument('--out-file', metavar='PATH', dest='out_stream', type=argparse.FileType('wb'), default=sys.stdout, help='The output file (default: STDOUT)')

def get_evaluator_ap():
  from api import add_subparsers, Evaluator
  res = ArgumentParser(add_help=False)
  add_subparsers(res, sorted(Evaluator.CLASSES.items()), 'eval_cls', title='evaluators')
  return res
