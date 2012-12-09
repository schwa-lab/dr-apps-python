"""
Apps to get basic statistics and meta-data from documents.
"""
from __future__ import print_function
import sys
from collections import defaultdict
import datetime
from drcli.api import App
from drcli.util import read_raw_docs
from drcli.appargs import ArgumentParser, ISTREAM_AP, DESERIALISE_AP, DrInputType


def get_store_names(doc):
    return (tup[0] for tup in doc.stores)

def iso_now():
    return datetime.datetime.now().isoformat()


class CountFormatter(object):
  AGG_SUM = 'sum'
  AGG_AVG = 'average'
  ALL = 'all'
  FILE = 'file'
  COUNT_BYTES = 'bytes'
  COUNT_ELEMENTS = 'elements'

  def __init__(self, args, out):
    self.args = args
    self.out = out

  def set_fields(self, names):
    pass

  def add_row(self, counts, ind, agg=None, filename=None, unit=COUNT_ELEMENTS):
    pass

  def start(self):
    pass

  def finish(self):
    pass


class CountTableFormatter(CountFormatter):
  def set_fields(self, names):
    if self.args.show_header:
      header = self.args.field_sep.join(names)
      if self.args.timestamp:
        header = iso_now() + self.args.field_sep
      print(header, file=self.out)

  def add_row(self, counts, ind, agg=None, filename=None, unit=CountFormatter.COUNT_ELEMENTS):
    base = self._fmt_counts(counts)
    suffix = None
    if ind == self.FILE:
      suffix = filename
    elif ind == self.ALL:
      if agg == self.AGG_SUM:
        suffix = 'TOTAL'
      elif agg == self.AGG_AVG:
        suffix = 'AVERAGE'
      else:
        raise ValueError('Unknown aggregate for total: %r' % agg)
    print(base + (self.args.field_sep + suffix if suffix else ''), file=self.out)

  def _fmt_counts(self, counts):
    res = self.args.field_sep.join(str(c) for c in counts)
    if self.args.timestamp:
      res = iso_now() + self.args.field_sep + res
    return res


class CountJsonFormatter(CountFormatter):
  def __init__(self, *args, **kwargs):
    super(CountJsonFormatter, self).__init__(*args, **kwargs)
    import json
    self._dumps = json.dumps

  def set_fields(self, names):
    self._fields = [self._dumps(name) for name in names]

  def add_row(self, counts, ind, agg=None, filename=None, unit=CountFormatter.COUNT_ELEMENTS):
    if self._row_added:
      self.out.write(',\n')
    self.out.write('{')
    self.out.write(', '.join('%s: %s' % tup for tup in zip(self._fields, counts)))
    self.out.write(', "_meta": {"after": %s' % self._dumps(ind))
    if ind == self.FILE:
      self.out.write(', "file": %s' % self._dumps(filename))
    if agg:
      self.out.write(', "agg": %s' % self._dumps(agg))
    if unit != CountFormatter.COUNT_ELEMENTS:
      self.out.write(', "unit": %s' % self._dumps(unit))
    if self.args.timestamp:
      self.out.write(', "time": %s' % self._dumps(iso_now()))
    self.out.write('}}')
    self.out.flush()
    self._row_added = True

  def start(self):
    self._row_added = False
    print('[', file=self.out)

  def finish(self):
    print('\n]', file=self.out)


class CountApp(App):
  """
  Count the number of documents or annotations in named stores.
  
  Examples:
    %(prog)s
        # display the number of documents found on standard input
    %(prog)s *.dr
        # list the number of documents in each .dr file and their total
    %(prog)s -a
        # display the number of elements in each store
    %(prog)s -s tokens
        # display the total number of elements in the 'tokens' store
    %(prog)s -ds tokens
        # same with document count
    %(prog)s -ds tokens -s sentences
        # same with number of 'sentences' elements
    %(prog)s -ea
        # display the number of elements in each store per document
    %(prog)s -eac
        # display the cumulative number of elements in each store per document
    %(prog)s -eacj
        # the same with output in JSON rather than a table
    %(prog)s -tcv10
        # every 10 documents, display the time and number of documents processed
    %(prog)s -aj --average --bytes
        # display as JSON the average and total number of bytes consumed by each store
  """
  count_arg_parser = ArgumentParser()
  count_arg_parser.add_argument('-s', '--store', metavar='ATTR', dest='count_stores', action='append', default=[], help='Count the specified store')
  count_arg_parser.add_argument('-d', '--docs', dest='count_docs', action='store_true', help='Count the number of documents (default without stores specified)')
  count_arg_parser.add_argument('-a', '--all', dest='count_all', action='store_true', help='Count docs and elements in all stores found on the first document')
  count_arg_parser.add_argument('-v', '--every', dest='show_interval', type=int, metavar='N', help='Show counts every N docs')
  count_arg_parser.add_argument('-e', '--every1', dest='show_interval', action='store_const', const=1, help='Show counts every doc')
  count_arg_parser.add_argument('--bytes', dest='count_bytes', action='store_true', default=False, help='Count the number of bytes for each store, rather than the number of elements')
  count_arg_parser.add_argument('--no-subtotal', dest='show_subtotal', default=True, action='store_false', help='Hides total count per input file')
  count_arg_parser.add_argument('--no-total', dest='show_total', default=True, action='store_false', help='Hides total count across all documents')
  count_arg_parser.add_argument('--average', dest='show_average', default=False, action='store_true', help='Show an average size per document')
  count_arg_parser.add_argument('--no-header', dest='show_header', default=True, action='store_false', help='Hides the field names displayed by --fmt-table with more than one field output')
  count_arg_parser.add_argument('-c', '--cumulative', default=False, action='store_true', help='Show cumulative counts')
  count_arg_parser.add_argument('-t', '--timestamp', action='store_true', default=False, help='Output the time with each count')
  count_arg_parser.add_argument('--sep', dest='field_sep', default='\t', help='Output field separator (with --fmt-table)')
  count_arg_parser.add_argument('--fmt-table', dest='formatter_cls', action='store_const', const=CountTableFormatter, default=CountTableFormatter, help='Format output as a table (default)')
  count_arg_parser.add_argument('-j', '--fmt-json', dest='formatter_cls', action='store_const', const=CountJsonFormatter, help='Format output as JSON')
  count_arg_parser.add_argument('files', nargs='*', type=DrInputType, help='Specify files by name rather than standard input')
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

    if not (args.show_interval or args.show_header or args.show_total or args.show_subtotal or args.show_average):
      argparser.error('Nothing to display')

    if args.cumulative and not args.show_interval and not args.show_subtotal:
      argparser.error('--cumulative may not apply without --every or per-file subtotals')

    self.formatter = args.formatter_cls(args, sys.stdout)

    super(CountApp, self).__init__(argparser, args)

  def __call__(self):
    consts = CountFormatter
    unit = consts.COUNT_BYTES if self.args.count_bytes else consts.COUNT_ELEMENTS
    self.formatter.start()

    i = 0
    for in_file in self.args.files:
      if i and not self.args.cumulative:
        subtotals = [0] * len(extractors)
      for doc in read_raw_docs(in_file):
        if not i:
          names, extractors = self._get_counters(doc)
          totals = [0] * len(extractors)
          subtotals = [0] * len(extractors)
          self.formatter.set_fields(names)

        doc_counts = [extract(doc) for extract in extractors]
        for j, c in enumerate(doc_counts):
          subtotals[j] += c
          totals[j] += c
        if self.args.show_interval and (i + 1) % self.args.show_interval == 0:
          if self.args.cumulative:
            self.formatter.add_row(totals, i, agg=consts.AGG_SUM, filename=in_file.name, unit=unit)
          else:
            self.formatter.add_row(doc_counts, i, filename=in_file.name, unit=unit)

        i += 1

      if self.args.show_subtotal:
        try:
          self.formatter.add_row(subtotals, consts.FILE, agg=consts.AGG_SUM, filename=in_file.name, unit=unit)
        except NameError:
          print("No documents to count", file=sys.stderr)

    try:
      if self.args.show_total:
        self.formatter.add_row(totals, consts.ALL, agg=consts.AGG_SUM, unit=unit)
      if self.args.show_average:
        self.formatter.add_row([x / i for x in totals], consts.ALL, agg=consts.AGG_AVG, unit=unit)
    except NameError:
      print("No documents to count", file=sys.stderr)
    self.formatter.finish()
  
  def _get_counters(self, doc):
    names = []
    extractors = []
    if self.args.count_all:
      self.args.count_stores = sorted(get_store_names(doc))
      if self.args.count_bytes:
        self.args.count_stores.insert(0, '__meta__')
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
  
  def _make_store_counter(self, attr):
    if not self.args.count_bytes:
      def count(doc):
        for name, klass, nelem in doc.stores:
            if name == attr:
              return nelem
        return 0
    else:
      # TODO: use wire count, relying on Joel's patches to msgpack-python
      import msgpack
      def count(doc):
        if attr == '__meta__':
          return len(msgpack.packb(doc.doc))
        for i, (name, klass, nelem) in enumerate(doc.stores):
            if name == attr:
              return len(msgpack.packb(doc.instances[i]))
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
        print(' '.join(sorted(names)))
      for name in names:
        counter[name] += 1
    try:
      if i == 1:
        fmt = '{name}'
      else:
        fmt = '{name}\t{count}'
    except NameError:
      print("No documents found", out=sys.stderr)
    for k, v in sorted(counter.items(), key=lambda (k, v): (-v, k)):
      print(fmt.format(name=k, count=v))


CountApp.register_name('count')
ListStoresApp.register_name('ls')
