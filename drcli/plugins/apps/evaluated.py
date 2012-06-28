"""
Apps which evaluate a function for each doc and act upon the result.
"""
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
    for doc in self.stream_reader:
      print evaluator(doc)


class FilterApp(App):
  """
  Filter the documents using an evaluator.
  A string consisting of only whitespace or the word 'false' evaluates to false.
  """
  arg_parsers = (get_evaluator_ap(), DESERIALISE_AP, OSTREAM_AP)

  def __call__(self):
    evaluator = self.evaluator
    writer = self.stream_writer
    for doc in self.stream_reader:
      if evaluator.as_boolean(doc):
        # TODO: avoid re-serialising
        writer.write_doc(doc)


FormatApp.register_name('format')
FilterApp.register_name('filter')
