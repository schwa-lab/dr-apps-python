from schwa.dr import Reader
from schwa.dr import Writer


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
    subp = subparsers.add_parser(name, parents=cls.get_arg_parsers(), help=cls.__doc__.strip().split('\n')[0], usage=cls.__doc__)
    subp.set_defaults(**{cls_arg: cls})


class Evaluator(SubParsed):
  CLASSES = {}

  def __call__(self, doc):
    raise NotImplementedError()

  def as_boolean(self, doc):
    res = self(doc)
    if hasattr(res, 'strip'):
      res = res.strip()
      if res.lower() == 'false':
        res = False
    return bool(res)


class App(SubParsed):
  CLASSES = {}

  def __call__(self):
    raise NotImplementedError()

  @property
  def stream_reader(self):
    return Reader(self.args.doc_class).stream(self.args.in_stream)

  @property
  def stream_writer(self):
    return Writer(self.args.out_stream)




