import sys
from operator import attrgetter
from drcli.api import App
from drcli.appargs import ArgumentParser, DESERIALISE_AP


class CountApp(App):
  """
  Count the number of documents or annotations in named stores.
  """
  # TODO: options to choose stores to count
  # TODO: avoid desiralising?
  count_arg_parser = ArgumentParser()
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

CountApp.register_name('count')
