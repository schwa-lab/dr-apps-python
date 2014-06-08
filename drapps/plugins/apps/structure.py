"""
Apps to restructure a corpus.
"""
from io import BytesIO
from collections import defaultdict
from itertools import islice
from collections import deque
from six.moves import range, zip
import six
from schwa import dr
from drapps.api import App
from drapps.appargs import DESERIALISE_AP, OSTREAM_AP, ISTREAM_AP, ArgumentParser


def SelectField(s):
  # TODO: ensure valid variable names
  parts = tuple(s.split('.'))
  if len(parts) == 1:
    return (None,) + parts
  if len(parts) == 2:
    return parts
  raise ValueError('Maximum one . may appear in fields')


class SelectApp(App):
  """
  Select only (or remove) specified fields on each document.
  """
  field_list_ap = ArgumentParser()
  field_list_ap.add_argument('fields', nargs='+', type=SelectField, help='Fields or stores to include (or exclude with -x). These are attributes on the document by default. When taking the form Class.field, Class objects will be similarly processed to retain or exclude given fields.')
  field_list_ap.add_argument('-x', '--exclude', action='store_true', default=False, help='Treat all fields listed as those to exclude rather than to retain.')
  arg_parsers = (field_list_ap, ISTREAM_AP, OSTREAM_AP)

  def __init__(self, argparser, args):
    field_dict = defaultdict(set)
    for klass, field in (args.fields or ()):
      field_dict[klass].add(field)
    args.doc_fields = field_dict[None]
    args.annot_fields = dict(field_dict)
    if args.exclude:
      self._perform = self._perform_exclude
    else:
      self._perform = self._perform_select
    super(SelectApp, self).__init__(argparser, args)

  def __call__(self):
    # FIXME: externalise reflection methods ... or avoid it by just deleting attributes
    reader, writer = self.stream_reader_writer
    for doc in reader:
      for store in six.itervalues(doc._dr_stores):
        try:
          fields = self.args.annot_fields[store.klass_name]
        except KeyError:
          continue
        self._perform(fields, store._klass._dr_s2p, store._klass._dr_fields)

      if self.args.doc_fields:
        self._perform(self.args.doc_fields, doc._dr_s2p, doc._dr_fields, doc._dr_stores)
      writer.write(doc)

  def _perform_exclude(self, fields, *attr_dicts):
    # FIXME: work for non-identity s2p maps, if necessary
    for attr_dict in attr_dicts:
      for f in fields:
        try:
          del attr_dict[f]
        except KeyError:
          pass

  def _perform_select(self, fields, *attr_dicts):
    # FIXME: work for non-identity s2p maps, if necessary
    for attr_dict in attr_dicts:
      for f in set(attr_dict) - fields:
        try:
          del attr_dict[f]
        except KeyError:
          pass


def RenameField(s):
  if s.count('=') != 1:
    raise ValueError('Argument must contain exactly one =')
  new, old = s.split('=')
  klass, new = SelectField(new)
  return klass, new, old


class RenameApp(App):
  """
  Rename specified fields or stores.
  """
  # TODO: rename annotation classes
  rename_list_ap = ArgumentParser()
  rename_list_ap.add_argument('renames', nargs='+', type=RenameField, help='Rename description of form [Class.]new_name=old_name')
  arg_parsers = (rename_list_ap, ISTREAM_AP, OSTREAM_AP)

  def __init__(self, argparser, args):
    rename_dict = defaultdict(set)
    for klass, new, old in (args.renames or ()):
      rename_dict[klass].add((new, old))
    args.renames = dict(rename_dict)
    super(RenameApp, self).__init__(argparser, args)

  def __call__(self):
    # FIXME: externalise reflection methods
    reader, writer = self.stream_reader_writer
    for doc in reader:
      classes = {None: doc.__class__}
      classes.update((store.klass_name, store._klass) for store in six.itervalues(doc._dr_stores))
      for klass_name, klass in six.iteritems(classes):
        try:
          renames = self.args.renames[klass_name]
        except KeyError:
          continue

        relevant = []
        for new, old in renames:
          try:
            del klass._dr_s2p[old]
          except KeyError:
            pass
          else:
            relevant.append((new, old))
        # s2p isn't used in Writer at present, but we'll update it just in case
        klass._dr_s2p.update(relevant)

        fields = klass._dr_fields.copy()
        fields.update(getattr(klass, '_dr_stores', ()))
        for new, old in relevant:
          fields[old].serial = new

      writer.write(doc)


class Compose(App):
  """
  Given two document streams of equal length, output their pairwise composition.
  """
  pass
  # TODO: can this be done in Python API?


class CatApp(App):
  """
  Concatenate docrep files.
  """
  pass
  arg_parsers = (ISTREAM_AP, OSTREAM_AP)
  # TODO: avoid deserialising


def subset_type(str_val):
    if ':' not in str_val:
      val = int(str_val)
      return slice(val, val + 1)
    start, stop = str_val.split(':')
    if start:
      start = int(start)
    else:
      start = 0
    if stop:
      stop = int(stop)
    else:
      stop = None
    return slice(start, stop)


class SubsetApp(App):
  """
  Extract documents by non-negative index or slice (a generalisation of head).

  Behaviour is undefined for overlapping slices.
  """
  arg_parser = ArgumentParser()
  arg_parser.add_argument('slices', nargs='+', type=subset_type, help='Non-negative slices in Python-like notation, e.g. 0, 5, :10, 5:10, 5:')
  arg_parsers = (arg_parser, ISTREAM_AP, OSTREAM_AP)

  @staticmethod
  def gen_subsets(it, *slices):
    if not slices:
      for obj in it:
        yield obj
    starts = {sl.start for sl in slices}
    if None in starts:
      starts.add(0)
    stops = {sl.stop for sl in slices}
    if None in stops:
      pairs = enumerate(it)
    else:
      pairs = zip(range(max(stops)), it)

    yielding = False
    for i, obj in pairs:
      yielding = (yielding and i not in stops) or i in starts
      if yielding:
        yield obj

  def _run(self, *slices):
    # TODO: avoid desiralising
    writer = self.raw_stream_writer
    reader = self.raw_stream_reader
    for doc in self.gen_subsets(reader, *slices):
      writer.write(doc)

  def __call__(self):
    self._run(*self.args.slices)


class HeadApp(SubsetApp):
  """
  Extract the first n documents, optionally after a skipped quantity.
  
  Examples:
    %(prog)s           # extract the first document from STDIN
    %(prog)s -n5       # extract the first five documents
    %(prog)s -s5 -n5   # extract the 6th to 10th documents
    %(prog)s -is5      # extract all but the first 5 documents
  """
  # TODO: handle multiple input sources
  head_arg_parser = ArgumentParser()
  head_arg_parser.add_argument('-n', '--ndocs', metavar='COUNT', type=int, default=1, help='The number of documents to extract (default: %(default)s)')
  head_arg_parser.add_argument('-s', '--skip', type=int, default=0, help='The number of documents to skip before extracting')
  arg_parsers = (head_arg_parser, ISTREAM_AP, OSTREAM_AP)

  def __call__(self):
    self._run(slice(self.args.skip, self.args.skip + self.args.ndocs))


class TailApp(App):
  """
  Extract the last n documents.

  Examples:
    %(prog)s          # extract the last document
    %(prog)s -n5      # extract the last five documents
  """
  # TODO: handle multiple input sources
  tail_arg_parser = ArgumentParser()
  tail_arg_parser.add_argument('-n', '--ndocs', metavar='COUNT', type=int, default=1, help='The number of documents to extract (default: %(default)s)')
  tail_arg_parser.add_argument('-s', '--skip', type=int, default=0, help='The number of documents to skip after extracting')
  arg_parsers = (tail_arg_parser, ISTREAM_AP, OSTREAM_AP)

  def __call__(self):
    # TODO: avoid desiralising
    # TODO: avoid keeping deserialised objects in memory
    writer = self.raw_stream_writer
    reader = self.raw_stream_reader
    buf = deque(maxlen=self.args.ndocs + self.args.skip)
    for doc in reader:
      buf.append(doc)
    for doc in islice(buf, self.args.ndocs):
      writer.write(doc)


class GenerateApp(App):
  """
  Generate empty documents.
  """
  ndocs_ap = ArgumentParser()
  ndocs_ap.add_argument('ndocs', nargs='?', metavar='COUNT', type=int, default=float('inf'), help='The number of documents to generate (default: infinity)')
  arg_parsers = (ndocs_ap, OSTREAM_AP)

  def __call__(self):
    empty = BytesIO()
    writer = dr.Writer(empty, dr.Doc)
    writer.write(dr.Doc())
    empty = empty.getvalue()
    
    out = self.args.out_stream
    i = 0
    while i < self.args.ndocs:
      out.write(empty)
      i += 1


###SelectApp.register_name('select')
###RenameApp.register_name('rename')
HeadApp.register_name('head')
TailApp.register_name('tail')
SubsetApp.register_name('subset')
GenerateApp.register_name('generate')
