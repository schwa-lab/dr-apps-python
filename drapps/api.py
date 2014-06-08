import textwrap
import argparse
import itertools
from schwa.dr import Reader
from schwa.dr import Writer

DECORATE_METHOD = 'drcli_decorate'


class SubParsed(object):
  arg_parsers = ()
  CLASSES = None # Overwrite as dict in subtypes

  @classmethod
  def get_arg_parsers(cls):
    return cls.arg_parsers

  @classmethod
  def register_name(cls, name, reg_cls=None):
    reg_cls = reg_cls or cls # allows to be used as wrapper with partial
    if name in cls.CLASSES:
      raise ValueError('Cannot assign %r as name for %r: already assigned to %r' % (name, reg_cls, cls.CLASSES[name]))
    cls.CLASSES[name] = reg_cls
    return reg_cls

  def __init__(self, argparser, args):
    self.args = args


def add_subparsers(parser, library, cls_arg, **kwargs):
  subparsers = parser.add_subparsers(**kwargs)
  for name, cls in library:
    doc = textwrap.dedent(cls.__doc__ or '')
    subp = subparsers.add_parser(name, parents=cls.get_arg_parsers(), help=doc.strip().split('\n')[0], description=doc, formatter_class=argparse.RawDescriptionHelpFormatter)
    subp.set_defaults(**{cls_arg: cls})


class Evaluator(SubParsed):
  CLASSES = {}

  def __call__(self, doc, ind):
    raise NotImplementedError('{0}.__call__ is undefined'.format(self.__class__.__name__))

  def as_boolean(self, doc, ind):
    res = self(doc, ind)
    if hasattr(res, 'strip'):
      res = res.strip()
      if res.lower() == 'false':
        res = False
    return bool(res)


class App(SubParsed):
  CLASSES = {}
  def __init__(self, argparser, args):
    super(App, self).__init__(argparser, args)
    eval_cls = getattr(args, 'eval_cls', None)
    if eval_cls:
      self.evaluator = eval_cls(argparser, args)

  def __call__(self):
    raise NotImplementedError('{0}.__call__ is undefined'.format(self.__class__.__name__))

  def get_reader_and_schema(self, stream=None):
    if stream is None:
      stream = self.args.in_stream
    doc_cls = getattr(self.args, 'doc_class', None)
    decorate = getattr(doc_cls, DECORATE_METHOD, lambda doc: None)
    reader = Reader(stream, doc_cls, doc_cls is None)
    def docs():
      for doc in reader:
        decorate(doc)
        yield doc
    x, y = itertools.tee(docs())
    try:
      next(x)  # ensures reader.doc_schema is non-None
    except StopIteration:
      return (), None
    return y, reader.doc_schema

  @property
  def stream_reader(self):
    docs, schema = self.get_reader_and_schema()
    return docs

  @property
  def stream_reader_writer(self):
    docs, schema = self.get_reader_and_schema()
    return docs, Writer(self.args.out_stream, schema)

  def get_stream_readers(self, *streams):
    for stream in streams:
      yield stream, self.get_reader_and_schema(stream)[0]

  @property
  def raw_stream_reader(self):
      from .util import read_raw_docs
      return read_raw_docs(self.args.in_stream)

  @property
  def raw_stream_writer(self):
      from .util import RawDocWriter
      return RawDocWriter(self.args.out_stream)



