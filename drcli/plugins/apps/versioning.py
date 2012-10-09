from collections import defaultdict
import itertools
import msgpack
from schwa.dr.constants import FIELD_TYPE_IS_SLICE
from drcli.appargs import ArgumentParser, ISTREAM_AP, OSTREAM_AP

class UpgradeVersionApp(App):
  """Upgrade wire format"""

  MAX_VERSION = 2
  ver_ap = ArgumentParser()
  ver_ap.add_argument('-t', '--target', dest='target_version', metavar='VERSION', default=MAX_VERSION, type=int, help='The target version number')
  # TODO: add arguments to save output to input file
  arg_parsers = (ver_ap, ISTREAM_AP, OSTREAM_AP)

  def __call__(self):
    unpacker = msgpack.Unpacker(self.args.in_stream)
    while self.process_doc(unpacker, self.args.out_stream):
      pass

  def process_doc(self, messages, out):
    try:
      version = messages.next()
    except StopIteration:
      return False
    if not isinstance(version, int):
      # Put the first message back on:
      messages = itertools.chain((version,), messages)
      version = 1

    for version in range(version, self.args.target_version):
      messages = getattr(self, 'update_to_v{0}'.format(version + 1))(messages)

    msgpack.pack(self.args.target_version, out) # update functions do not output version
    for msg in messages:
      msgpack.pack(msg, out)

    return True

  def update_to_v2(self, messages):
    """
    Performs the following changes:
    * Replaces is_slice value TRUE with NULL
    * Replaces slice stop from absolute to relative offset
    """
    # TODO: accept options to make certain fields self-pointers
    slice_fields = defaultdict(set)
    meta_klass = None
    try:
      klasses = messages.next()
    except StopIteration, e:
      self._ended_early(self, e)

    for knum, (name, fields) in enumerate(klasses):
      if name == '__meta__':
        meta_klass = knum
      for fnum, fdef in enumerate(fields):
        if fdef.get(FIELD_TYPE_IS_SLICE):
          # None is the new True
          fdef[FIELD_TYPE_IS_SLICE] = None
          slice_fields[knum].add(fnum)
    yield klasses # changed
    del klasses

    try:
      stores = messages.next()
    except StopIteration:
      self._ended_early(self, e)
    yield stores # unchanged

    for knum in itertools.chain((meta_klass,), (k for name, k, size in stores)):
      try:
        nbytes = messages.next()
        instances = messages.next()
      except StopIteration:
        self._ended_early(self, e)

      if knum not in slice_fields:
        # unchanged
        yield nbytes
        yield instances
        continue
      
      inst_iter = (instances,) if isinstance(instances, dict) else instances
      ksl_fields = slice_fields[knum]
      for instance in inst_iter:
        for f in ksl_fields:
          val = instance.get(f)
          if val:
            instance[f] = (val[0], val[1] - val[0])

      # changed
      yield len(msgpack.packb(instances))
      yield instances

  def _ended_early(self, exc):
    raise ValueError('Messages ended mid-document!')


UpgradeVersionApp.register_name('upgrade')
