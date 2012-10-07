from collections import namedtuple
import msgpack

RawDoc = namedtuple('RawDoc', ('version', 'klasses', 'stores', 'doc', 'instances'))

def read_raw_docs(unpacker):
    if not hasattr(unpacker, 'unpack'):
        unpacker = msgpack.Unpacker(unpacker, use_list=True)
    unpack = unpacker.next
    while True: # unpacker throws StopIteration
        klasses = unpack()
        if isinstance(klasses, int):
            version = klasses
            klasses = unpack()
        else:
            version = 1
        stores = unpack()
        yield RawDoc(version, klasses, stores, unpack() and unpack(), [unpack() and unpack() for i in range(len(stores))])

def write_with_nbytes(out, obj):
    packed = msgpack.packb(obj)
    msgpack.pack(len(packed), out)
    out.write(packed)

def write_raw_doc(out, doc):
    if doc.version != 1:
        msgpack.pack(doc.version, out)
    msgpack.pack(doc.klasses, out)
    msgpack.pack(doc.stores, out)
    write_with_nbytes(out, doc.doc)
    for insts in doc.instances:
        write_with_nbytes(out, insts)

class RawDocWriter(object):
    def __init__(self, out):
        self.out = out

    def write(self, doc):
        write_raw_doc(self.out, doc)
