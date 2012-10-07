
import msgpack
import pprint
import ast
from schwa import dr
from schwa.dr.constants import FIELD_TYPE_NAME, FIELD_TYPE_POINTER_TO, FIELD_TYPE_IS_SLICE, FIELD_TYPE_IS_SELF_POINTER
from drcli.api import App
from drcli.appargs import ArgumentParser, ISTREAM_AP, OSTREAM_AP, DESERIALISE_AP

META_TYPE = 0

class DumpApp(App):
  """
  Debug: unpack the stream and pretty-print it.
  """
  dump_ap = ArgumentParser()
  dump_ap.add_argument('-m', '--human', dest='human_readable', action='store_true', default=False, help='Reinterpret the messages to be more human-readable by integrating headers into content.')
  dump_ap.add_argument('-n', '--numbered', action='store_true', default=False, help='In --human mode, add a \'#\' field to each annotation, indicating its ordinal index')
  arg_parsers = (dump_ap, ISTREAM_AP, OSTREAM_AP)

  def dump(self, obj):
    pprint.pprint(obj, self.args.out_stream)

  def __call__(self):
    unpacker = msgpack.Unpacker(self.args.in_stream)
    if self.args.human_readable:
      unpacker = self._integrate_names(unpacker)
    for obj in unpacker:
      self.dump(obj)

  def _integrate_names(self, unpacker):
    while True:
      obj = {}
      types = unpacker.unpack()
      if types is None:
        # No new header
        break
      elif isinstance(types, int):
        obj['__version__'] = types
        types = unpacker.unpack()
      store_defs = list(self._process_store_defs(unpacker.unpack(), types))
      nbytes = unpacker.unpack()
      obj['__meta__'] = self._process_annot(unpacker.unpack(), types[META_TYPE][1])
      for store_name, store in store_defs:
        nbytes = unpacker.unpack()
        store['items'] = [self._process_annot(item, store['fields']) for item in unpacker.unpack()]
        if self.args.numbered:
          for i, item in enumerate(store['items']):
            item['#'] = i
        store['fields'] = dict(self._fields_to_dict(store['fields'], store_defs))
        # store.pop('fields')
        obj[store_name] = store
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
    return dict((fields[fnum][FIELD_TYPE_NAME], val) for fnum, val in msg.iteritems())

  def _fields_to_dict(self, fields, store_defs):
    for field in fields:
      name = field.pop(FIELD_TYPE_NAME)
      try:
        field['points to'], store_data = store_defs[field.pop(FIELD_TYPE_POINTER_TO)]
      except KeyError:
        pass
      try:
        field['is slice'] = field.pop(FIELD_TYPE_IS_SLICE)
      except KeyError:
        pass
      try:
        field['is self-pointer'] = field.pop(FIELD_TYPE_IS_SELF_POINTER)
      except KeyError:
        pass
      yield name, field


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
        fname = fdef.get(FIELD_TYPE_NAME)
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
