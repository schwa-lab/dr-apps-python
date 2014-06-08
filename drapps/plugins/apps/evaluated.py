"""
Apps which evaluate a function for each doc and act upon the result.
"""
from __future__ import print_function
from io import BytesIO
from schwa import dr
from drapps.api import App, Evaluator
from drapps.appargs import DESERIALISE_AP, OSTREAM_AP, get_evaluator_ap, ArgumentParser

class FormatApp(App):
  """
  Print out a formatted evaluation of each document.
  """
  # e.g. dr format json
  #      dr format django '{% if ... %}'
  arg_parsers = (get_evaluator_ap(), DESERIALISE_AP)

  def __call__(self):
    evaluator = self.evaluator
    for i, doc in enumerate(self.stream_reader):
      print(evaluator(doc, i))


class GrepApp(App):  # grep
  """
  Filter the documents using an evaluator.
  A string consisting of only whitespace or the word 'false' evaluates to false.
  """
  arg_parsers = (get_evaluator_ap(), DESERIALISE_AP, OSTREAM_AP)

  def __call__(self):
    evaluator = self.evaluator
    reader, writer = self.stream_reader_writer
    for i, doc in enumerate(reader):
      if evaluator.as_boolean(doc, i):
        # TODO: avoid re-serialising
        writer.write(doc)


class RandomEvaluator(Evaluator):
  """Distribute to each of k folds"""
  ap = ArgumentParser()
  ap.add_argument('--seed', dest='rand_seed', type=int, default=None)
  arg_parsers = (ap,)

  def __init__(self, argparser, args):
    super(RandomEvaluator, self).__init__(argparser, args)
    import random
    self.gen_random = random.Random(self.args.rand_seed).random

  def __call__(self, doc, ind):
      return self.gen_random()


class SortApp(App):
  """
  Sort the documents using an evaluator.
  """
  arg_parsers = (get_evaluator_ap({'random': RandomEvaluator}), DESERIALISE_AP, OSTREAM_AP)

  def __call__(self):
    reader, schema = self.get_reader_and_schema()
    tmp_out = BytesIO()
    tmp_writer = dr.Writer(tmp_out, schema)
    evaluator = self.evaluator
    items = []
    for i, doc in enumerate(reader):
      # TODO: avoid re-serialising
      doc_key = evaluator(doc, i)
      tmp_writer.write(doc)
      doc_data = tmp_out.getvalue()
      tmp_out.truncate(0)
      items.append((doc_key, doc_data))

    items.sort()
    for doc_key, doc_data in items:
      self.args.out_stream.write(doc_data)


class SetFieldApp(App):
  """
  Set a named field on each document to a value.
  """
  field_name_ap = ArgumentParser()
  field_name_ap.add_argument('field_name', help='The field name to set')
  arg_parsers = (field_name_ap, get_evaluator_ap(), DESERIALISE_AP, OSTREAM_AP)

  def __call__(self):
    attr = self.args.field_name
    evaluator = self.evaluator
    reader, writer = self.stream_reader_writer
    for i, doc in enumerate(reader):
      if attr not in doc._dr_s2p:
        # TODO: externalise reflection methods
        doc._dr_s2p[attr] = attr
        doc._dr_fields[attr] = dr.Field(serial=attr)
      setattr(doc, attr, evaluator(doc, i))
      writer.write(doc)


class KFoldsEvaluator(Evaluator):
  """Distribute to each of k folds"""
  ap = ArgumentParser()
  ap.add_argument('kfolds', type=int)
  arg_parsers = (ap,)

  def __call__(self, doc, ind):
    return ind % self.args.kfolds


class FoldsApp(App):
  """
  Split a stream into k files, or a separate file for each key determined per doc.
  To perform stratified k-fold validation, first sort the corpus by the stratification label.

  If the evaluation returns a list, the document is written to each key in the list.
  """
  multioutput_ap = ArgumentParser()
  multioutput_ap.add_argument('-t', '--template', dest='path_tpl', default='fold{n:03d}.dr', help='A template for output paths (default: %(default)s). {n} substitutes for fold number, {key} for evaluation output.')
  multioutput_ap.add_argument('--overwrite', action='store_true', default=False, help='Overwrite an output file if it already exists.')
  multioutput_ap.add_argument('--sparse', action='store_true', default=False, help='Use append mode to write files, and close the handle between writes')
  multioutput_ap.add_argument('--make-dirs', action='store_true', default=False, help='Make directories when necessary')
  arg_parsers = (DESERIALISE_AP, multioutput_ap, get_evaluator_ap({'k': KFoldsEvaluator}),)

  def __init__(self, argparser, args):
    if '{' not in args.path_tpl:
      argparser.error('Output path template must include a substitution (e.g. {n:02d} or {key})')
    super(FoldsApp, self).__init__(argparser, args)
    if self.args.sparse:
      if self.args.overwrite:
        argparser.error('--overwrite does not apply with --sparse')
      if isinstance(self.evaluator, KFoldsEvaluator):
        argparser.error('k-folds cannot be used with --sparse')
      if any(expr in args.path_tpl for expr in ('{n}', '{n!', '{n:')): # FIXME: use regexp
        argparser.error('--sparse must use filenames templated by key')

  def __call__(self):
    # TODO: clean up!!
    evaluator = self.evaluator
    if isinstance(evaluator, KFoldsEvaluator):
      # avoid full deserialisation
      #TODO: make more generic
      reader = self.raw_stream_reader
      from drapps.util import RawDocWriter
      make_writer = RawDocWriter
    else:
      reader, schema = self.get_reader_and_schema()
      make_writer = lambda out: dr.Writer(out, schema)

    if self.args.make_dirs:
      def fopen(path, mode):
        dirname = os.path.dirname(path)
        if not os.path.exists(dirname):
          cur = ''
          for part in dirname.split(os.path.sep):
            cur += part
            if part and not os.path.exists(cur):
              os.mkdir(cur)
            cur += os.path.sep
        return open(path, mode)
    else:
      fopen = open

    def new_writer(key):
        fold_num = len(writers)
        path = self.args.path_tpl.format(n=fold_num, key=key)
        import sys, os.path
        if not self.args.overwrite and os.path.exists(path):
          print('Path {0} already exists. Use --overwrite to overwrite.'.format(path), file=sys.stderr)
          sys.exit(1)
        print('Writing fold {k} to {path}'.format(k=fold_num, path=path), file=sys.stderr)
        return make_writer(fopen(path, 'wb'))

    if self.args.sparse:
      get_writer = lambda key: make_writer(fopen(self.args.path_tpl.format(key=key), 'ab'))
    else:
      writers = {}
      def get_writer(key):
        try:
          writer = writers[key]
        except KeyError:
          writer = writers[key] = new_writer(key)
        return writer

    for i, doc in enumerate(reader):
      val = evaluator(doc, i)
      for key in val if isinstance(val, list) else (val,):
        writer = get_writer(key)
        writer.write(doc)


FormatApp.register_name('format')
GrepApp.register_name('grep')
SortApp.register_name('sort')
FoldsApp.register_name('folds')
###SetFieldApp.register_name('set')
