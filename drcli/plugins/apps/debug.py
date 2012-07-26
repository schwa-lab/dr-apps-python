
import msgpack
import pprint
import json
from schwa import dr
from schwa.dr.constants import FIELD_TYPE_NAME
from drcli.api import App
from drcli.appargs import ArgumentParser, ISTREAM_AP, OSTREAM_AP, DESERIALISE_AP

META_TYPE = 0

class DumpApp(App):
  """
  Debug: unpack the stream and pretty-print it.
  """
  dump_ap = ArgumentParser()
  dump_ap.add_argument('-m', '--human', dest='human_readable', action='store_true', default=False, help='Reinterpret the messages to be more human-readable by integrating headers into content.')
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
      store_defs = self._process_store_defs(unpacker.unpack(), types)
      nbytes = unpacker.unpack()
      obj['__meta__'] = self._process_annot(unpacker.unpack(), types[META_TYPE][1])
      for store_name, store in store_defs:
        nbytes = unpacker.unpack()
        store['items'] = [self._process_annot(item, store['fields']) for item in unpacker.unpack()]
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


DumpApp.register_name('dump')
