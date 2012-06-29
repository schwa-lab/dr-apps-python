
import msgpack
import pprint
from drcli.api import App
from drcli.appargs import ISTREAM_AP, OSTREAM_AP


class DumpApp(App):
  """
  Debug: unpack the stream and pretty-print it.
  """
  arg_parsers = (ISTREAM_AP, OSTREAM_AP)

  def dump(self, obj):
    pprint.pprint(obj, self.args.out_stream)

  def __call__(self):
    unpacker = msgpack.Unpacker(self.args.in_stream)
    for obj in unpacker:
      self.dump(obj)


DumpApp.register_name('dump')
