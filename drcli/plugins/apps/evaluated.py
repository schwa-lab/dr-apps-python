"""
Apps which evaluate a function for each doc and act upon the result.
"""
from StringIO import StringIO
from schwa import dr
from drcli.api import App, Evaluator
from drcli.appargs import DESERIALISE_AP, OSTREAM_AP, get_evaluator_ap

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
      print evaluator(doc, i)


class FilterApp(App):
  """
  Filter the documents using an evaluator.
  A string consisting of only whitespace or the word 'false' evaluates to false.
  """
  arg_parsers = (get_evaluator_ap(), DESERIALISE_AP, OSTREAM_AP)

  def __call__(self):
    evaluator = self.evaluator
    writer = self.stream_writer
    for i, doc in enumerate(self.stream_reader):
      if evaluator.as_boolean(doc, i):
        # TODO: avoid re-serialising
        writer.write_doc(doc)


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
    tmp_out = StringIO()
    tmp_writer = dr.Writer(tmp_out)
    evaluator = self.evaluator
    items = []
    for i, doc in enumerate(self.stream_reader):
      # TODO: avoid re-serialising
      doc_key = evaluator(doc, i)
      tmp_writer.write_doc(doc)
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
    writer = self.stream_writer
    for i, doc in enumerate(self.stream_reader):
      if attr not in doc._dr_s2p:
        # TODO: externalise reflection methods
        doc._dr_s2p[attr] = attr
        doc._dr_fields[attr] = dr.Field(serial=attr)
      setattr(doc, attr, evaluator(doc, i))
      writer.write_doc(doc)


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
  """
  multioutput_ap = ArgumentParser()
  multioutput_ap.add_argument('-t', '--template', dest='path_tpl', default='fold{n:03d}.dr', help='A template for output paths (default: %(default)s). {n} substitutes for fold number, {key} for evaluation output.')
  multioutput_ap.add_argument('--overwrite', action='store_true', default=False, help='Overwrite an output file if it already exists.')
  arg_parsers = (ISTREAM_AP, multioutput_ap, get_evaluator_ap({'k': KFoldsEvaluator}),)

  def __init__(self, argparser, args):
    if '{' not in args.path_tpl:
      argparser.error('Output path template must include a substitution (e.g. {n:02d} or {key})')
    super(FoldsApp, self).__init__(argparser, args)

  # TODO: avoid desrialising in the k folds case...?
  def __call__(self):
    writers = {}
    def new_writer(key):
        fold_num = len(writers)
        path = self.args.path_tpl.format(n=fold_num, key=key)
        import sys, os.path
        if not self.args.overwrite and os.path.exists(path):
          print >> sys.stderr, 'Path {0} already exists. Use --overwrite to overwrite.'.format(path)
          sys.exit(1)
        print >> sys.stderr, 'Writing fold {k} to {path}'.format(k=fold_num, path=path)
        return dr.Writer(open(path, 'wb'))

    evaluator = self.evaluator
    for i, doc in enumerate(self.stream_reader):
      key = evaluator(doc, i)
      try:
        writer = writers[key]
      except KeyError:
        writer = writers[key] = new_writer(key)
      writer.write_doc(doc)


FormatApp.register_name('format')
FilterApp.register_name('filter')
SortApp.register_name('sort')
FoldsApp.register_name('folds')
SetFieldApp.register_name('set')
