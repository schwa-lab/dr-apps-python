# vim: set et nosi ai ts=2 sts=2 sw=2:
# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals
import collections
import itertools

import msgpack
from schwa.dr.constants import FieldType
import six
from six.moves import range

from drapps.api import App
from drapps.appargs import ArgumentParser, ISTREAM_AP, OSTREAM_AP


class UpgradeVersionApp(App):
  """Upgrade wire format"""

  MAX_VERSION = 3
  ver_ap = ArgumentParser()
  ver_ap.add_argument('-t', '--target', dest='target_version', metavar='VERSION', default=MAX_VERSION, type=int, help='The target version number')
  # TODO: add arguments to save output to input file
  arg_parsers = (ver_ap, ISTREAM_AP, OSTREAM_AP)

  def __call__(self):
    unpacker = msgpack.Unpacker(self.args.in_stream, use_list=True, encoding=None)
    out = self.args.out_stream
    if six.PY3:
      out = out.buffer
    while self.process_doc(unpacker, out):
      pass

  def process_doc(self, messages, out):
    try:
      version = next(messages)
    except StopIteration:
      return False
    if not isinstance(version, int):
      # Put the first message back on:
      messages = itertools.chain((version,), messages)
      version = 1

    for version in range(version, self.args.target_version):
      messages = getattr(self, 'update_to_v{0}'.format(version + 1))(messages)

    msgpack.pack(self.args.target_version, out, use_bin_type=True)  # update functions do not output version
    for msg in messages:
      msgpack.pack(msg, out, use_bin_type=True)

    return True

  def update_to_v2(self, messages):
    """
    Performs the following changes:
    * Replaces is_slice value TRUE with NULL
    * Replaces slice stop from absolute to relative offset
    """
    # TODO: accept options to make certain fields self-pointers
    slice_fields = collections.defaultdict(set)
    meta_klass = None
    try:
      klasses = next(messages)
    except StopIteration as e:
      self._ended_early(self, e)

    for knum, (name, fields) in enumerate(klasses):
      if name == '__meta__':
        meta_klass = knum
      for fnum, fdef in enumerate(fields):
        if fdef.get(FieldType.IS_SLICE):
          # None is the new True
          fdef[FieldType.IS_SLICE] = None
          slice_fields[knum].add(fnum)
    yield klasses  # changed
    del klasses

    try:
      stores = next(messages)
    except StopIteration:
      self._ended_early(self, e)
    yield stores  # unchanged

    for knum in itertools.chain((meta_klass,), (k for name, k, size in stores)):
      try:
        nbytes = next(messages)
        instances = next(messages)
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

  def _upgrade_obj_to_v2(self, obj):
    if isinstance(obj, list):
      for i, x in enumerate(obj):
        obj[i] = self._upgrade_obj_to_v2(x)
    elif isinstance(obj, dict):
      new_obj = {}
      for k, v in obj.iteritems():
        new_obj[self._upgrade_obj_to_v2(k)] = self._upgrade_obj_to_v2(v)
      obj = new_obj
    elif isinstance(obj, str):
      try:
        obj = obj.decode('utf-8')
      except UnicodeDecodeError:
        pass
    return obj

  def update_to_v3(self, messages):
    """
    Tries to decode as UTF-8 all values that were the old MessagePack string type. If they
    successfully decode, write them back out as a new MessagePack UTF-8 type; otherwise write them
    out as a new MesagePack bytes type.
    """
    klasses = next(messages)
    assert isinstance(klasses, list)

    stores = next(messages)
    assert isinstance(stores, list)

    doc_instance_nbytes = next(messages)
    assert isinstance(doc_instance_nbytes, int)
    doc_instance = next(messages)
    assert isinstance(doc_instance, dict)

    all_instance_groups = []
    for i in range(len(stores)):
      instance_nbytes = next(messages)
      assert isinstance(instance_nbytes, int)
      instance_groups = next(messages)
      assert isinstance(instance_groups, list)
      all_instance_groups.append(instance_groups)

    klasses = self._upgrade_obj_to_v2(klasses)
    yield klasses

    stores = self._upgrade_obj_to_v2(stores)
    yield stores

    doc_instance = self._upgrade_obj_to_v2(doc_instance)
    yield len(msgpack.packb(doc_instance, use_bin_type=True))
    yield doc_instance

    for instance_groups in all_instance_groups:
      instance_groups = self._upgrade_obj_to_v2(instance_groups)
      yield len(msgpack.packb(instance_groups, use_bin_type=True))
      yield instance_groups


UpgradeVersionApp.register_name('upgrade')
