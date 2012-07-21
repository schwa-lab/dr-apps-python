"""
Apps which evaluate a function for each doc and act upon the result.
"""
from StringIO import StringIO
from schwa import dr
from drcli.api import App
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


class SortApp(App):
  """
  Sort the documents using an evaluator.
  """
  arg_parsers = (get_evaluator_ap(), DESERIALISE_AP, OSTREAM_AP)

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
      print >> self.args.out_stream, doc_data


FormatApp.register_name('format')
FilterApp.register_name('filter')
SortApp.register_name('sort')
