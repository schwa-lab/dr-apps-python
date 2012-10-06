import sys
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
    doc = cls.__doc__ or ''
    subp = subparsers.add_parser(name, parents=cls.get_arg_parsers(), help=doc.strip().split('\n')[0], description=doc)
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

  @property
  def stream_reader(self):
    stream, docs = self.get_stream_readers(self.args.in_stream).next()
    return docs

  def _read_stream(self, stream, doc_cls, decorate):
    for doc in Reader(doc_cls).stream(stream):
      decorate(doc)
      yield doc

  def get_stream_readers(self, *streams):
    doc_cls = getattr(self.args, 'doc_class', None)
    decorate = getattr(doc_cls, DECORATE_METHOD, lambda doc: None)
    for stream in streams:
      yield stream, self._read_stream(stream, doc_cls, decorate)

  @property
  def stream_writer(self):
    return Writer(self.args.out_stream)

  @property
  def raw_stream_reader(self):
      from .util import read_raw_docs
      return read_raw_docs(self.args.in_stream)

  @property
  def raw_stream_writer(self):
      from .util import RawDocWriter
      return RawDocWriter(self.args.out_stream)



