
from drcli.api import App
from drcli.appargs import DESERIALISE_AP, OSTREAM_AP, ISTREAM_AP

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
