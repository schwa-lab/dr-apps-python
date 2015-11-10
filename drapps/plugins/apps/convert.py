# vim: set et nosi ai ts=2 sts=2 sw=2:
# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals
import argparse
from functools import partial
from operator import attrgetter
import sys

import six

from drapps.api import App
from drapps.appargs import DESERIALISE_AP, ArgumentParser, import_string


DEFAULT_TOKENS_STORE = 'tokens'


class _AppendSliceField(argparse._AppendAction):
  def __init__(self, **kwargs):
    self.slice_formatter = kwargs.pop('slice_fmt')
    kwargs['metavar'] = 'REV_ALL_ATTR[.FIELD]'
    super(_AppendSliceField, self).__init__(**kwargs)

  def __call__(self, parser, namespace, values, option_string=None):
    try:
      rev_attr, sub_attr = values.split('.', 1)
    except:
      rev_attr = values
      sub_attr = None

    extractor = self.slice_formatter(rev_attr, sub_attr)
    super(_AppendSliceField, self).__call__(parser, namespace, extractor, option_string)


class _SliceFormatter(object):
  def __init__(self, rev_attr, sub_attr):
    self.rev_attr = rev_attr
    self.get_value = attrgetter(sub_attr)

  def get_slice_data(self, tok):
    d = getattr(tok, self.rev_attr)
    if isinstance(d, list):
        assert 0 <= len(d) <= 1, 'Non-mutually-exclusive rev_attr {} has multiple values {}. Policy undefined!'.format(self.rev_attr, d)
        return d[0]
    else:
        return d


class _IOB(_SliceFormatter):
  IOB1 = REPEAT_B = 1
  IOB2 = ALWAYS_B = 2

  def __init__(self, rev_attr, sub_attr, mode=IOB2):
    self.mode = mode
    super(_IOB, self).__init__(rev_attr, sub_attr)

  def begin_sentence(self):
    self.prev_val = None

  def __call__(self, tok):
    try:
      slice_data = self.get_slice_data(tok)
    except AttributeError:
      slice_data = None
    if slice_data is None:
      ptr = None
    else:
      ptr, offset, roffset = slice_data

    if ptr is None:
      self.prev_val = None
      return 'O'

    val = self.get_value(ptr)
    if offset == 0 and (self.mode == self.ALWAYS_B or val == self.prev_val):
      tag = 'B'
    else:
      tag = 'I'

    self.prev_val = val
    return '{0}-{1}'.format(tag, val)


class _BILOU(_SliceFormatter):
  # possible TODO: merge with _IOB, using options like always_b, always_e
  def __init__(self, rev_attr, sub_attr, tags='BILOU'):
    self.B, self.I, self.L, self.O, self.U = tags
    self.tag_map = {(True, False): self.B, (False, False): self.I, (False, True): self.L, (True, True): self.U}
    super(_BILOU, self).__init__(rev_attr, sub_attr)

  def __call__(self, tok):
    try:
      slice_data = self.get_slice_data(tok)
    except AttributeError:
      slice_data = None
    if slice_data is None:
      ptr = None
    else:
      ptr, offset, roffset = slice_data

    if ptr is None:
      self.prev_val = None
      return self.O

    val = self.get_value(ptr)
    tag = self.tag_map[not offset, not roffset]
    return '{0}-{1}'.format(tag, val)


def get_norm(tok):
  return tok.norm or tok.raw


def get_raw(tok):
  return tok.raw or tok.norm


def fmt_separator(sep):
  def join_gen(items):
    items = iter(items)
    yield next(items)
    for item in items:
      yield sep
      yield item
  return join_gen


class SetCandcAction(argparse.Action):
  def __call__(self, parser, namespace, values, option_string=None):
    namespace.fmt_docs = fmt_separator('\n\n')
    namespace.fmt_sents = fmt_separator('\n')
    namespace.fmt_toks = fmt_separator(' ')
    namespace.fmt_fields = fmt_separator('|')
    namespace.clean_field = lambda s: s.replace(' ', '_')


class _SuperSentence(object):
    """Mimicks a sentence for the --ignore-sents option"""
    span = slice(None, None)


class WriteConll(App):
  """Writes documents in CONLL format, or a format which similarly lists fields separated by some delimiter.

  Example invocation:
  `cat docs.dr | dr conll --doc-class some.module.Document --norm -f pos --iob1 chunk.tag`
  For `--iob1 'chunk.tag'` to work, this assumes some.module.Document.drcli_decorate includes the following decoration:
    reverse_slices('chunks', 'tokens', 'span', all_attr='chunk')
  """

  annotations_ap = ArgumentParser()
  annotations_ap.add_argument('--tok-store', dest='get_tokens', default=attrgetter('tokens'), type=attrgetter, help='Specify a particular Token store (default: tokens)')
  annotations_ap.add_argument('--sent-store', dest='get_sentences', default=attrgetter('sentences'), type=attrgetter, help='Specify a particular Sentence store (default: sentences)')
  annotations_ap.add_argument('--sent-tok-slice', dest='get_sent_tok_slice', default=attrgetter('span'), type=attrgetter, help='The field on Sentence objects which indicates its slice over tokens (default: span)')
  annotations_ap.add_argument('--ignore-sents', dest='get_sentences', action='store_const', const=lambda doc: (_SuperSentence(),), help='List all tokens as if in a single sentence')

  # TODO: use streams instead of string operations
  formatting_ap = ArgumentParser()
  formatting_ap.add_argument('--field-sep', dest='fmt_fields', default=fmt_separator('\t'), type=fmt_separator, help='Separator between fields (default: tab)')
  formatting_ap.add_argument('--tok-sep', dest='fmt_toks', default=fmt_separator('\n'), type=fmt_separator, help='Separator between tokens (default: newline)')
  formatting_ap.add_argument('--sent-sep', dest='fmt_sents', default=fmt_separator('\n\n'), type=fmt_separator, help='Separator between sentences (default: double-newline)')
  formatting_ap.add_argument('--doc-sep', dest='fmt_docs', default=fmt_separator('\n\n#BEGIN-DOC\n\n'), type=fmt_separator, help='Separator between documents (default: #BEGIN-DOC)')
  formatting_ap.add_argument('--candc', action=SetCandcAction, nargs=0, help='Use default C&C tagger format')

  field_list_ap = ArgumentParser()
  field_list_ap.add_argument('--norm', dest='field_extractors', const=get_norm, action='append_const', help='Output the normal token form')
  field_list_ap.add_argument('--raw', dest='field_extractors', const=get_raw, action='append_const', help='Output the raw token form')
  field_list_ap.add_argument('-f', '--field', dest='field_extractors', type=attrgetter, action='append', help='Output the specified field')
  field_list_ap.add_argument('--fn', dest='field_extractors', type=import_string, action='append', help='Output the result of a function given a token')

  # Slice fields:
  field_list_ap.add_argument('--iob1', dest='field_extractors', action=_AppendSliceField, slice_fmt=partial(_IOB, mode=_IOB.IOB1), help='Outputs IOB1 given the name of an attribute resulting from reverse_slices(.., all_attr=MY_ATTR)')
  field_list_ap.add_argument('--iob2', dest='field_extractors', action=_AppendSliceField, slice_fmt=partial(_IOB, mode=_IOB.IOB2), help='Outputs IOB2 given the name of an attribute resulting from reverse_slices(.., all_attr=MY_ATTR)')
  field_list_ap.add_argument('--bilou', dest='field_extractors', action=_AppendSliceField, slice_fmt=_BILOU, help='Outputs BILOU given the name of an attribute resulting from reverse_slices(.., all_attr=MY_ATTR)')
  field_list_ap.add_argument('--bmewo', dest='field_extractors', action=_AppendSliceField, slice_fmt=partial(_BILOU, tags='BMEOW'), help='Outputs BMEWO given the name of an attribute resulting from reverse_slices(.., all_attr=MY_ATTR)')
  # TODO: allow decorators to be specified on the command-line
  arg_parsers = (field_list_ap, formatting_ap, annotations_ap, DESERIALISE_AP)

  def __init__(self, argparser, args):
    if not args.field_extractors:
      argparser.error('At least one field extractor is required')
    if not hasattr(args, 'clean_field'):
      args.clean_field = lambda s: s
    super(WriteConll, self).__init__(argparser, args)

  def __call__(self):
    self.write_flattened(sys.stdout.write, self.args.fmt_docs(self.process_doc(doc) for doc in self.stream_reader))

  def write_flattened(self, write, iterable):
    for fragment in iterable:
      if isinstance(fragment, six.string_types):
        write(fragment)
      else:
        self.write_flattened(write, fragment)

  def process_doc(self, doc):
    token_store = self.args.get_tokens(doc)
    return self.args.fmt_sents(self.begin_sentence() or self.process_sent(sent, token_store) for sent in self.args.get_sentences(doc))

  def process_sent(self, sent, tok_store):
    return self.args.fmt_toks(self.process_tok(tok) for tok in tok_store[self.args.get_sent_tok_slice(sent)])

  def process_tok(self, tok):
    return self.args.fmt_fields(self.args.clean_field(str(extr(tok))) for extr in self.args.field_extractors)

  def begin_sentence(self):
    # TODO: should only need to do these checks once per instance
    for extr in self.args.field_extractors:
      f = getattr(extr, 'begin_sentence', None)
      if f:
        f()


WriteConll.register_name('conll')
