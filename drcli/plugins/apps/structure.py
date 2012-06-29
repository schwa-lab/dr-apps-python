"""
Apps to restructure a corpus.
"""
from itertools import izip
from drcli.api import App
from drcli.appargs import DESERIALISE_AP, OSTREAM_AP, ISTREAM_AP, ArgumentParser

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


class HeadApp(App):
  """
  Extract the first n documents.
  """
  head_arg_parser = ArgumentParser()
  head_arg_parser.add_argument('-n', '--ndocs', metavar='count', type=int, default=1, help='The number of documents to extract (default: %(default)s)')
  arg_parsers = (head_arg_parser, ISTREAM_AP, OSTREAM_AP)

  def __call__(self):
    # TODO: avoid desiralising
    writer = self.stream_writer
    for i, doc in izip(xrange(self.args.ndocs), self.stream_reader):
      writer.write_doc(doc)


HeadApp.register_name('head')
