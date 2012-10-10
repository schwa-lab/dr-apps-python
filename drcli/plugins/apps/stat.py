"""
Apps to get basic statistics and meta-data from documents.
"""
import sys
from operator import attrgetter
from collections import defaultdict
from drcli.api import App
from drcli.util import read_raw_docs
from drcli.appargs import ArgumentParser, DESERIALISE_AP, argparse


def get_store_names(doc):
    return (tup[0] for tup in doc.stores)

class CountApp(App):
  """
  Count the number of documents or annotations in named stores.
  """
  # TODO: options to choose stores to count
  # TODO: avoid desiralising?
  count_arg_parser = ArgumentParser()
  count_arg_parser.add_argument('-s', '--store', metavar='ATTR', dest='count_stores', action='append', default=[], help='Count the specified store')
  count_arg_parser.add_argument('-d', '--docs', dest='count_docs', action='store_true', help='Count the number of documents (default without fields specified)')
  count_arg_parser.add_argument('-a', '--all', dest='count_all', action='store_true', help='Count docs and elements in all stores found on the first document')
  count_arg_parser.add_argument('--every', dest='show_interval', type=int, metavar='N', help='Show counts every N docs')
  count_arg_parser.add_argument('-e', '--every1', dest='show_interval', action='store_const', const=1, help='Show counts every doc')
  count_arg_parser.add_argument('--no-subtotal', dest='show_subtotal', default=True, action='store_false', help='Hides total count per input file')
  count_arg_parser.add_argument('--no-total', dest='show_total', default=True, action='store_false', help='Hides total count across all documents')
  count_arg_parser.add_argument('--no-header', dest='show_header', default=True, action='store_false', help='Hides the field names displayed with more than one field output')
  count_arg_parser.add_argument('--sep', dest='field_sep', default='\t', help='Output field separator')
  count_arg_parser.add_argument('-c', '--cumulative', default=False, action='store_true', help='Show cumulative counts')
  count_arg_parser.add_argument('files', nargs='*', type=argparse.FileType('rb'), help='Specify files by name rather than standard input')
  arg_parsers = (count_arg_parser, ISTREAM_AP,)

  def __init__(self, argparser, args):
    if args.count_all and (args.count_docs or args.count_stores):
      argparser.error('--all flag may not be used in conjunction with --docs or store names')

    if not (args.count_docs or args.count_stores or args.count_all):
      args.count_docs = True
    if args.count_all:
      args.count_docs = True
    elif 1 == len(args.count_stores) + (1 if args.count_docs else 0):
      args.show_header = False

    if not args.files:
      args.files = [args.in_stream]
    if len(args.files) <= 1:
      args.show_subtotal = False

    if not (args.show_interval or args.show_header or args.show_total or args.show_subtotal):
      argparser.error('Nothing to display')

    if args.cumulative and not args.show_interval and not args.show_subtotal:
      argparser.error('--cumulative may not apply without --every or per-file subtotals')

    super(CountApp, self).__init__(argparser, args)

  def __call__(self):
    i = 0
    for in_file in self.args.files:
      if i and not self.args.cumulative:
        subtotals = [0] * len(extractors)
      for doc in read_raw_docs(in_file):
        if not i:
          names, extractors = self._get_counters(doc)
          totals = [0] * len(extractors)
          subtotals = [0] * len(extractors)
          if self.args.show_header:
            print self.args.field_sep.join(names)

        doc_counts = [extract(doc) for extract in extractors]
        for j, c in enumerate(doc_counts):
          subtotals[j] += c
          totals[j] += c
        if self.args.show_interval and (i + 1) % self.args.show_interval == 0:
          if self.args.cumulative:
            print self._fmt_counts(totals)
          else:
            print self._fmt_counts(doc_counts)

        i += 1

      if self.args.show_subtotal:
        print self._fmt_counts(subtotals) + (self.args.field_sep + in_file.name)

    try:
      if self.args.show_total:
        print self._fmt_counts(totals) + (self.args.field_sep + 'TOTAL' if self.args.show_interval or self.args.show_subtotal else '')
    except NameError:
      print >> sys.stderr, "No documents to count"

  def _fmt_counts(self, counts):
    return self.args.field_sep.join(str(c) for c in counts)
  
  def _get_counters(self, doc):
    names = []
    extractors = []
    if self.args.count_all:
      self.args.count_stores = sorted(get_store_names(stores))
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
    def count(doc):
      for name, klass, nelem in doc.stores:
          if name == attr:
            return nelem
      return 0
    return count


class ListStoresApp(App):
  """
  List the stores available in the corpus.
  Where multiple documents are input, also indicates the number of documents where they appear.
  """
  # Extend to list fields, and fields on stored types
  ls_arg_parser = ArgumentParser()
  ls_arg_parser.add_argument('-e', '--each-doc', dest='show_each', default=False, action='store_true', help='List stores for each doc')
  arg_parsers = (ls_arg_parser, DESERIALISE_AP,)

  def __call__(self):
    counter = defaultdict(int)
    for i, doc in enumerate(self.raw_stream_reader):
      names = list(get_store_names(doc))
      if self.args.show_each:
        print ' '.join(sorted(names))
      for name in names:
        counter[name] += 1
    try:
      if i == 1:
        fmt = '{name}'
      else:
        fmt = '{name}\t{count}'
    except NameError:
      print >> sys.stderr, "No documents found"
    for k, v in sorted(counter.items(), key=lambda (k, v): (-v, k)):
      print fmt.format(name=k, count=v)


CountApp.register_name('count')
ListStoresApp.register_name('ls')
