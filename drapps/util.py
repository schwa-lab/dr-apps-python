from collections import namedtuple
import msgpack

RawDoc = namedtuple('RawDoc', ('version', 'klasses', 'stores', 'doc', 'instances'))

class RawDoc(object):
  __slots__ = ('version', 'klasses', 'stores', '_doc', '_instances', '_doc_packed', '_instances_packed')
  
  def __init__(self, version, klasses, stores, doc, instances, packed=True):
    self.version = version
    self.klasses = klasses
    self.stores = stores
    if packed:
      self._doc_packed = doc
      self._instances_packed = instances
      self._doc = self._instances = None
    else:
      self._doc = doc
      self._instances = instances
      self._doc_packed = self._instances_packed = None

  @classmethod
  def from_stream(cls, unpacker, on_end='error'):
    if on_end not in ('error', 'break'):
        raise ValueError('on_end must be "error" or "break"')
    if not hasattr(unpacker, 'unpack'):
      unpacker = msgpack.Unpacker(unpacker, use_list=True)
    unpack = unpacker.unpack
    read_bytes = unpacker.read_bytes
    try:
      while True:
        try:
          klasses = unpack()
        except msgpack.OutOfData:
          return
        if isinstance(klasses, int):
          version = klasses
          klasses = unpack()
        else:
          version = 1
        stores = unpack()
        yield cls(version, klasses, stores, read_bytes(unpack()), [read_bytes(unpack()) for i in range(len(stores))], packed=True)
    except msgpack.OutOfData:
      if on_end == 'error':
        raise

  def write(self, out):
    if self.version != 1:
      msgpack.pack(self.version, out)
    msgpack.pack(self.klasses, out)
    msgpack.pack(self.stores, out)
    doc = self.doc_packed
    msgpack.pack(len(doc), out)
    out.write(doc)
    for insts in self.instances_packed:
      msgpack.pack(len(insts), out)
      out.write(insts)

  def _get_doc(self):
    if self._doc is None:
      self._doc = msgpack.unpackb(self._doc_packed)
    return self._doc

  def _set_doc(self, doc):
    self._doc = doc
    self._doc_packed = None

  doc = property(_get_doc, _set_doc)

  def _get_instances(self):
    if self._instances is None:
      self._instances = [msgpack.unpackb(store) for store in self._instances_packed]
    return self._instances

  def _set_instances(self, instances):
    self._instances = instances
    self._instances_packed = None

  instances = property(_get_instances, _set_instances)

  def _get_doc_packed(self):
    if self._doc_packed is None:
      self._doc_packed = msgpack.packb(self._doc)
    return self._doc_packed

  def _set_doc_packed(self, doc):
    self._doc_packed = doc
    self._doc = None

  doc_packed = property(_get_doc_packed, _set_doc_packed)

  def _get_instances_packed(self):
    if self._instances_packed is None:
      self._instances_packed = [msgpack.packb(store) for store in self._instances]
    return self._instances_packed

  def _set_instances_packed(self, instances):
    self._instances_packed = instances
    self._instances = None

  instances_packed = property(_get_instances_packed, _set_instances_packed)


def read_raw_docs(unpacker, on_end='error'):
  return RawDoc.from_stream(unpacker, on_end=on_end)

def write_raw_doc(out, doc):
  doc.write(out)

class RawDocWriter(object):
  def __init__(self, out):
    self.out = out

  def write(self, doc):
    doc.write(self.out)


def import_string(name):
  path, base = name.rsplit('.', 1)
  return getattr(__import__(path, globals=None, fromlist=[base]), base)
