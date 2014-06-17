# vim: set et nosi ai ts=2 sts=2 sw=2:
# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

from schwa import dr

from drapps.api import Evaluator
from drapps.evalargs import STRING_AP, ArgumentParser
from drapps.util import import_string


class PythonImportEval(Evaluator):
  """Evaluate an imported Python function with arguments (doc, ind)"""
  import_ap = ArgumentParser()
  import_ap.add_argument('function', type=import_string, help='The import string to a function')
  arg_parsers = (import_ap,)

  def __call__(self, doc, ind):
    return self.args.function(doc, ind)


class PythonCodeEval(Evaluator):
  """Evaluate a Python expression where `doc', `ind' and `dr' are in scope"""
  arg_parsers = (STRING_AP,)

  def __init__(self, argparser, args):
    super(PythonCodeEval, self).__init__(argparser, args)
    self.code = compile(args.string, '__arg__', 'eval')

  def __call__(self, doc, ind):
    return eval(self.code, globals(), {'doc': doc, 'ind': ind, 'dr': dr})


PythonImportEval.register_name('fn')
PythonCodeEval.register_name('py')
