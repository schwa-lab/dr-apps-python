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

FormatApp.register_name('format')
FilterApp.register_name('filter')
SortApp.register_name('sort')
SetFieldApp.register_name('set')
