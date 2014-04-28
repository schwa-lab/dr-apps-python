
from __future__ import print_function
from collections import namedtuple
import sys
import functools
from drapps.api import App, add_subparsers, SubParsed
from drapps.appargs import ArgumentParser, DESERIALISE_AP, OSTREAM_AP


Names = namedtuple('Names', 'local qual serial')


class SrcGenLang(SubParsed):
  CLASSES = {}

  @staticmethod
  def split_name(name):
    try:
      module, name = name.rsplit('.', 1)
    except ValueError:
      return None, name
    return module, name

  def get_names(self, schema, default_module=None):
    """Returns (local_name, meta name, serial)"""
    module, name = self.split_name(schema.name)
    if module == default_module:
      module = None
    # TODO: any necessary escaping
    return Names(name, module + '.' + name if module else None, schema.serial if schema.serial != name else None)

  def find_multistore_klasses(self, doc_schema):
    seen = set()
    for store in doc_schema.stores():
      klass = store.stored_type
      if klass in seen:
        yield klass
      else:
        seen.add(klass)

  def __call__(self, klasses, stores):
    raise NotImplementedError('{0}.__call__ is undefined'.format(self.__class__.__name__))


class GenPy(SrcGenLang):
  """Generate Python declarations"""

  def __call__(self, schema):
    doc_module, doc_name = self.split_name(schema.name)
    get_names = functools.partial(self.get_names, default_module=doc_module)

    self._ambiguous_klasses = set(self.find_multistore_klasses(schema))

    self._print('from schwa import dr')
    self._print()

    for klass in schema.klasses():
      self._define_ann(klass, get_names)
    self._define_ann(schema, get_names, is_doc=True)
  
  def _define_ann(self, klass, get_names, is_doc=False):
    # TODO: help/docstrings
    super_cls = 'dr.Doc' if is_doc else 'dr.Ann'
    knames = get_names(klass)
    self._print('class {0}({1}):'.format(knames.local, super_cls))
    if knames.qual:
      self._print('class Meta:', indent=1)
      self._print('name = {0}'.format(knames.qual), indent=2)
      self._print()

    if is_doc:
      for store in klass.stores():
        snames = get_names(store)
        type_name = get_names(store.stored_type).local
        serial = ', serial={0!r}'.format(snames.serial) if snames.serial else ''
        self._print('{0} = dr.Store({1!r}{2})'.format(snames.local, type_name, serial), indent=1)
      if klass.stores:
        self._print()

    for field in klass.fields():
      fnames = get_names(field)
      name = fnames.local
      serial = ', serial={0!r}'.format(fnames.serial) if fnames.serial else ''
      
      if field.is_pointer:
        targ_type = field.points_to.stored_type
        targ_name = get_names(targ_type).local
        if targ_type in self._ambiguous_klasses:
          targ_args = '{0!r}, store={1!r}'.format(targ_name, get_names(field.points_to).local)
        else:
          targ_args = '{0!r}'.format(targ_name)
      else:
        targ_args = ''

      if field.is_slice:
        self._print('{0} = dr.Slice({1}{2})'.format(name, targ_args, serial), indent=1)

      elif field.is_pointer:
        self._print('{0} = dr.Pointer{s}({1}{2})'.format(name, targ_args, serial, s='s' if field.is_collection else ''), indent=1)

      elif field.is_self_pointer:
        self._print('{0} = dr.SelfPointer{s}({1})'.format(name, serial, s='s' if field.is_collection else ''), indent=1)

      else:
        self._print('{0} = dr.Field({1})'.format(name, serial), indent=1)

    self._print()
    self._print()


  def _print(self, obj='', indent=0):
    print(self.args.indent * indent + str(obj), file=self.args.out_stream)


###class GenJava(SrcGenLang):
###  """Generate Java declarations"""
###  pass


###class GenCpp(SrcGenLang):
###  """Generate C++ declarations"""
###  pass


GenPy.register_name('python')
###GenJava.register_name('java')
###GenCpp.register_name('cpp')


class SrcGenerator(App):
  """
  Generate source code for declaring types as instantiated in a given corpus, assuming headers are identical throughout.
  """
  srcgen_ap = ArgumentParser()
  add_subparsers(srcgen_ap, sorted(SrcGenLang.CLASSES.items()), 'gen_cls', title='target languages')
  srcgen_ap.add_argument('--doc-name', default='Document', help='The name of the document class (default: %(default)r)')
  srcgen_ap.add_argument('--indent', default='  ', help='The indent text (defaukt: %(default)r)')
  arg_parsers = (srcgen_ap, DESERIALISE_AP, OSTREAM_AP)

  def __init__(self, argparser, args):
    super(SrcGenerator, self).__init__(argparser, args)
    self.generate = args.gen_cls(argparser, args)

  def __call__(self):
    doc = self.stream_reader.next()
    schema = doc._dr_rt.copy_to_schema() #### WARNING: using private
    self.generate(schema)


SrcGenerator.register_name('srcgen')
