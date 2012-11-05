
import sys
import argparse
import gzip
from functools import partial
from brownie.importing import import_string
from .api import DECORATE_METHOD

ArgumentParser = partial(argparse.ArgumentParser, add_help=False)

class SuffixDependentType(object):
  def __init__(self, suffix_fns):
    if hasattr(suffix_fns, 'items'):
      suffix_fns = suffix_fns.items()
    suffix_fns = list(suffix_fns)
    suffix_fns.sort(key=lambda (suf, fn): -len(suf))
    self.suffix_fns = suffix_fns
  
  def __call__(self, arg):
    for suf, fn in self.suffix_fns:
      if arg.endswith(suf):
        return fn(arg)
    raise argparse.ArgumentTypeError('{0!r} has an unknown suffix (not one of {1!r})'.format(arg, [suf for suf, fn in self.suffix_fns]))


DrInputType = SuffixDependentType({'': argparse.FileType('rb'), '.gz': gzip.open, '.drz': gzip.open})
open_write_gz = partial(gzip.open, mode='wb')
DrOutputType = SuffixDependentType({'': argparse.FileType('wb'), '.gz': open_write_gz, '.drz': open_write_gz})


ISTREAM_AP = ArgumentParser(add_help=False)
ISTREAM_AP.add_argument('--in-file', metavar='PATH', dest='in_stream', type=DrInputType, default=sys.stdin, help='The input file (default: STDIN)')

DESERIALISE_AP = ArgumentParser(parents=(ISTREAM_AP,), add_help=False)
DESERIALISE_AP.add_argument('--doc-class', metavar='CLS', dest='doc_class', type=import_string, help='Import path to the Document class for the input.  If available, doc.{0}() will be called for each document on the stream.'.format(DECORATE_METHOD))

OSTREAM_AP = ArgumentParser(add_help=False)
OSTREAM_AP.add_argument('--out-file', metavar='PATH', dest='out_stream', type=DrOutputType, default=sys.stdout, help='The output file (default: STDOUT)')

def get_evaluator_ap(extra={}):
  from api import add_subparsers, Evaluator
  res = ArgumentParser(add_help=False)
  add_subparsers(res, sorted(extra.items()) + sorted(Evaluator.CLASSES.items()), 'eval_cls', title='evaluators')
  return res
