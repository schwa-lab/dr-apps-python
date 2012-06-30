
from schwa import dr
from drcli.api import Evaluator
from drcli.evalargs import ArgumentParser, STRING_AP
from brownie.importing import import_string


class PythonImportEval(Evaluator):
  import_ap = ArgumentParser()
  import_ap.add_argument('function', type=import_string, help='The import string to a function accepting arguments (doc, ind)')
  arg_parsers = (import_ap,)

  def __call__(self, doc, ind):
    return self.args.function(doc, ind)


class PythonCodeEval(Evaluator):
  arg_parsers = (STRING_AP,)

  def __init__(self, argparser, args):
    super(PythonCodeEval, self).__init__(argparser, args)
    self.code = compile(args.string, '__arg__', 'eval')

  def __call__(self, doc, ind):
    return eval(self.code, globals(), {'doc': doc, 'ind': ind, 'dr': dr})


PythonImportEval.register_name('fn')
PythonCodeEval.register_name('py')
