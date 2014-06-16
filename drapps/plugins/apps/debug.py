
from __future__ import print_function
import msgpack
import pprint
import json
import ast
from schwa import dr
from schwa.dr.constants import FieldType
from drapps.api import App
from drapps.util import read_raw_docs
from drapps.appargs import ArgumentParser, ISTREAM_AP, OSTREAM_AP, DESERIALISE_AP

META_TYPE = 0

FORMATTERS = {
  'json': json.dumps,
  'pprint': pprint.pformat,
}

class DumpApp(App):
  """
  Debug: unpack the stream and pretty-print it.
  """
  dump_ap = ArgumentParser()
  dump_ap.add_argument('-m', '--human', dest='human_readable', action='store_true', default=False, help='Reinterpret the messages to be more human-readable by integrating headers into content.')
  dump_ap.add_argument('-n', '--numbered', action='store_true', default=False, help='In --human mode, add a \'#\' field to each annotation, indicating its ordinal index')
  dump_ap.add_argument('-d', '--headers', dest='hide_instances', action='store_true', default=False, help='Show headers only, hiding any instances')
  dump_ap.add_argument('-j', '--json', dest='format', action='store_const', const='json', default='pprint', help='Output valid JSON')
  arg_parsers = (dump_ap, ISTREAM_AP, OSTREAM_AP)

  def dump(self, obj):
    print(self.format(obj), file=self.args.out_stream)

  def __call__(self):
    self.format = FORMATTERS[self.args.format]
    unpacker = msgpack.Unpacker(self.args.in_stream)
    if self.args.human_readable:
      unpacker = self._integrate_names(unpacker)
    elif self.args.hide_instances:
      unpacker = self._headers_only(unpacker)
    first = True
    for obj in unpacker:
      if self.args.format == 'json':
        print('[' if first else ',', file=self.args.out_stream)
      self.dump(obj)
      first = False
    if self.args.format == 'json':
      print(']')

  def _headers_only(self, unpacker):
    for doc in read_raw_docs(unpacker):
      yield doc.version
      yield doc.klasses
      yield doc.stores

  def _integrate_names(self, unpacker):
    for i, doc in enumerate(read_raw_docs(unpacker)):
      obj = {}
      obj['__version__'] = doc.version
      store_defs = list(self._process_store_defs(doc.stores, doc.klasses))
      obj['__meta__'] = self._process_annot(doc.doc, doc.klasses[META_TYPE][1])
      if self.args.numbered:
          obj['#'] = i
      for (store_name, store), instances in zip(store_defs, doc.instances):
        obj[store_name] = store
        if not self.args.hide_instances:
          store['items'] = [self._process_annot(item, store['fields']) for item in instances]
          if self.args.numbered:
            for j, item in enumerate(store['items']):
              item['#'] = j
        store['fields'] = dict(self._fields_to_dict(store['fields'], store_defs))
      yield obj

  def _process_store_defs(self, msg, types):
    for name, typ, size in msg:
      try:
        type_name, type_fields = types[typ]
      except IndexError:
        # for robustness to broken data
        type_name, type_fields = '??MissingType={0}'.format(typ), ()
      yield name, {'type': type_name, 'fields': type_fields, 'count': size}

  def _process_annot(self, msg, fields):
    return dict((fields[fnum][FieldType.NAME], val) for fnum, val in msg.iteritems())

  TRAIT_NAMES = {
    FieldType.IS_SLICE: 'is slice',
    FieldType.IS_SELF_POINTER: 'is self-pointer',
    FieldType.IS_COLLECTION: 'is collection',
  }

  def _fields_to_dict(self, fields, store_defs, trait_names=TRAIT_NAMES):
    for field in fields:
      name = None
      traits = {}
      for k, v in field.items():
        if k == FieldType.NAME:
          name = v
        elif k == FieldType.POINTER_TO:
          traits['points to'], store_data = store_defs[v]
        elif k in trait_names:
          traits[trait_names[k]] = v
        else:
          traits[k] = v
      yield name, traits


class HackHeaderApp(App):
  """
  Debug: rewrite header components of given documents using Python literal input
  """
  hack_ap = ArgumentParser()
  hack_ap.add_argument('--klasses', default=None, help='Overwrites the entire klasses header with the given list')
  hack_ap.add_argument('-k', '--klass', default=[], action='append', help='Overwrites a klass definition, specified with <name|num>=[<new_name>,<field list>]')
  hack_ap.add_argument('-f', '--field', default=[], action='append', help='Overwrites a field definition, specified with <klass-name|num>.<field-name|num>[+]=<map> (use += for update semantics)')
  arg_parsers = (hack_ap, ISTREAM_AP, OSTREAM_AP)

  def __init__(self, argparser, args):
    super(HackHeaderApp, self).__init__(argparser, args)

    def parse(s, exp_type):
      try:
        res = ast.literal_eval(s)
      except (SyntaxError, ValueError):
        argparser.error('{0} is not a valid Python literal'.format(s))
      if exp_type is not None and type(res) != exp_type:
        argparser.error('{0} does not evaluate to type {1}'.format(s, exp_type))
      return res
    
    self.operations = []

    if args.klasses:
      self.operations.append((self._set_klasses, {'value': parse(args.klasses, list)}))

    for arg in args.klass:
      try:
        key, value = arg.split('=', 1)
      except ValueError:
        argparser.error('Expected <name>=<value>, got {0}'.format(arg))
      try:
        key = int(key)
      except ValueError:
        pass
      value = parse(value, list)
      if len(value) != 2:
        argparser.error('Expected a list of length 2, got {0}'.format(value))
      self.operations.append((self._set_klass, {'klass': key, 'value': value}))

    for arg in args.field:
      try:
        key, value = arg.split('=', 1)
        kname, fname = key.split('.')
      except ValueError:
        argparser.error('Expected <kname>.<fname>=<value>, got {0}'.format(arg))
      if fname.endswith('+'):
        fname = fname[:-1]
        update = True
      else:
        update = False
      try:
        kname = int(kname)
      except ValueError:
        pass
      try:
        fname = int(fname)
      except ValueError:
        pass
      value = parse(value, dict)
      self.operations.append((self._set_field, {'klass': kname, 'field': fname, 'value': value, 'update': update}))
    
    if not self.operations:
      argparser.error('Nothing to do!')

  def _set_klasses(self, klasses, stores, value):
    klasses[:] = value

  def _set_klass(self, klasses, stores, klass, value):
    if klass == len(klasses):
      klasses.append(value)
    for knum, (kname, fields) in enumerate(klasses):
      if klass in (knum, kname):
        klasses[knum] = value
        return
    raise ValueError('Could not find class {0}'.format(klass))

  def _set_field(self, klasses, stores, klass, field, value, update=False):
    for knum, (kname, fields) in enumerate(klasses):
      if klass not in (knum, kname):
        continue

      if field == len(fields):
        fields.append({})

      for fnum, fdef in enumerate(fields):
        fname = fdef.get(FieldType.NAME)
        if field in (fnum, fname):
          if update:
            fields[fnum].update(value)
          else:
            fields[fnum] = value
          return

    raise ValueError('Could not find field {1} in class {0}'.format(klass, field))
  
  def __call__(self):
    writer = self.raw_stream_writer
    for doc in self.raw_stream_reader:
      for fn, kwargs in self.operations:
        fn(doc.klasses, doc.stores, **kwargs)
      writer.write(doc)


DumpApp.register_name('dump')
HackHeaderApp.register_name('hackheader')
