#!/usr/bin/env python
from operator import attrgetter
import sys
import argparse
from brownie.importing import import_string
from schwa.dr import Reader
from schwa.dr import Writer



class SubParsed(object):
  arg_parsers = ()

  @classmethod
  def get_arg_parsers(cls):
    return cls.arg_parsers

  def __init__(self, argparser, args):
    self.args = args


def add_subparsers(parser, library, cls_arg, **kwargs):
  subparsers = parser.add_subparsers(**kwargs)
  for name, cls in library:
    subp = subparsers.add_parser(name, parents=cls.get_arg_parsers(), help=cls.__doc__)
    subp.set_defaults(**{cls_arg: cls})


class Evaluator(SubParsed):
  def __call__(self, doc):
    raise NotImplementedError()

  def as_boolean(self, doc):
    res = self(doc)
    if hasattr(res, 'strip'):
      res = res.strip()
      if res.lower() == 'false':
        res = False
    return bool(res)


class App(SubParsed):
  def __call__(self):
    raise NotImplementedError()

  @property
  def stream_reader(self):
    return Reader(self.args.doc_class).stream(self.args.in_stream)

  @property
  def stream_writer(self):
    return Writer(self.args.out_stream)


class DjangoTemplater(Evaluator):
  pass


EVALUATORS = {
  'django': DjangoTemplater,
}


ISTREAM_AP = argparse.ArgumentParser(add_help=False)
ISTREAM_AP.add_argument('--in-file', metavar='PATH', dest='in_stream', type=argparse.FileType('rb'), default=sys.stdin, help='The input file (default: STDIN)')

DESERIALISE_AP = argparse.ArgumentParser(parents=(ISTREAM_AP,), add_help=False)
DESERIALISE_AP.add_argument('--doc-class', dest='doc_class', type=import_string, help='Import path to the Document class for the input')

OSTREAM_AP = argparse.ArgumentParser(add_help=False)
OSTREAM_AP.add_argument('--out-file', metavar='PATH', dest='out_stream', type=argparse.FileType('wb'), default=sys.stdout, help='The output file (default: STDOUT)')


EVALUATOR_AP = argparse.ArgumentParser(add_help=False)
add_subparsers(EVALUATOR_AP, sorted(EVALUATORS.items()), 'eval_cls', title='evaluators')


class FormatApp(App):
  """
  Print out a formatted evaluation of each document.
  """
  # e.g. dr format json
  #      dr format django '{% if ... %}'
  arg_parsers = (EVALUATOR_AP, DESERIALISE_AP)

  def __call__(self):
    evaluator = self.evaluator
    for doc in self.stream_reader:
      print evaluator(doc)


class FilterApp(App):
  """
  Filter the documents using an evaluator.
  """
  arg_parsers = (EVALUATOR_AP, DESERIALISE_AP, OSTREAM_AP)

  def __call__(self):
    evaluator = self.evaluator
    writer = self.stream_writer
    for doc in self.stream_reader:
      if evaluator.as_boolean(doc):
        # TODO: avoid re-serialising
        writer.write_doc(doc)


class SelectApp(App):
  """
  Select only (or remove) specified fields on each document.
  """
  arg_parsers = (DESERIALISE_AP, OSTREAM_AP)
  pass
  # TODO: can this be done in Python API?


class RenameApp(App):
  """
  Rename specified fields or stores.
  """
  arg_parsers = (ISTREAM_AP, OSTREAM_AP)
  pass
  # TODO: can this be done in Python API?


class FoldsApp(App):
  """
  Split a stream into multiple files.
  """
  arg_parsers = (ISTREAM_AP,)
  pass
  # TODO: avoid desiralising


class CatApp(App):
  """
  Concatenate docrep files.
  """
  pass
  arg_parsers = (ISTREAM_AP, OSTREAM_AP)
  # TODO: avoid deserialising


class CountApp(App):
  """
  Count the number of documents or annotations in named stores.
  """
  # TODO: options to choose stores to count
  # TODO: avoid desiralising?
  count_arg_parser = argparse.ArgumentParser(add_help=False)
  count_arg_parser.add_argument('count_stores', metavar='STORE', nargs='*', help='Sum the length of the specified store')
  count_arg_parser.add_argument('-d', '--docs', dest='count_docs', action='store_true', help='Count the number of documents (default without fields specified)')
  count_arg_parser.add_argument('-a', '--all', dest='count_all', action='store_true', help='Count docs and elements in all stores found on the first document')
  count_arg_parser.add_argument('-e', '--each-doc', dest='show_each', default=False, action='store_true', help='Show counts for each doc')
  count_arg_parser.add_argument('--no-total', dest='show_total', default=True, action='store_false', help='Hides total count across all documents')
  count_arg_parser.add_argument('--no-header', dest='show_header', default=True, action='store_false', help='Hides the field names displayed with more than one field output')
  count_arg_parser.add_argument('--sep', dest='field_sep', default='\t', help='Output field separator')
  arg_parsers = (count_arg_parser, DESERIALISE_AP,)

  def __init__(self, argparser, args):
    if args.count_all and (args.count_docs or args.count_stores):
      argparser.error('--all flag may not be used in conjunction with --docs or store names')

    if not (args.count_docs or args.count_stores or args.count_all):
      args.count_docs = True
    if args.count_all:
      args.count_docs = True
    elif 1 == len(args.count_stores) + (1 if args.count_docs else 0):
      args.show_header = False

    if not (args.show_each or args.show_header or args.show_total):
      argparser.error('Nothing to display')

    super(CountApp, self).__init__(argparser, args)

  def __call__(self):
    for i, doc in enumerate(self.stream_reader):
      if not i:
        names, extractors = self._get_counters(doc)
        totals = [0] * len(extractors)
        if self.args.show_header:
          print self.args.field_sep.join(names)

      doc_counts = [extract(doc) for extract in extractors]
      for i, c in enumerate(doc_counts):
        totals[i] += c
      if self.args.show_each:
        print self._fmt_counts(doc_counts)

    try:
      if self.args.show_total:
        print self._fmt_counts(totals) + (self.args.field_sep + 'TOTAL' if self.args.show_each else '')
    except NameError:
      print >> sys.stderr, "No documents to count"

  def _fmt_counts(self, counts):
    return self.args.field_sep.join(str(c) for c in counts)
  
  def _get_counters(self, doc):
    names = []
    extractors = []
    if self.args.count_all:
      # FIXME: problematic reflection
      self.args.count_stores = sorted(doc._dr_stores.keys())
    if self.args.count_docs:
      names.append('docs')
      extractors.append(self._doc_counter)
    for store in self.args.count_stores:
      names.append(store)
      extractors.append(self._make_store_counter(store))
    return names, extractors

  @staticmethod
  def _doc_counter(doc):
    return 1
  
  @staticmethod
  def _make_store_counter(attr):
    get_store = attrgetter(attr)
    def count(doc):
      return len(get_store(doc))
    return count


APPS = {
  'count': CountApp,
  'filter': FilterApp,
  'fmt': FormatApp,
}

def main(args):
  parser = argparse.ArgumentParser()
  add_subparsers(parser, sorted(APPS.items()), 'app_cls', title='apps')
  args = parser.parse_args()
  args.app_cls(parser, args)()


if __name__ == '__main__':
    main(sys.argv[1:])
