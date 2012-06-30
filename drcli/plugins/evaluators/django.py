try:
  from django.conf import settings
  try:
      settings.configure()
  except RuntimeError:
      pass # assume already configured
  from django.template import Template, Context
except ImportError:
  Template = None

from drcli.api import Evaluator
from drcli.evalargs import STRING_AP


class DjangoEvaluator(Evaluator):
  """Render a Django template where `doc' and `ind' are in context"""
  arg_parsers = (STRING_AP,)

  def __init__(self, argparser, args):
    self.template = Template(args.string)

  def __call__(self, doc, ind):
    return self.template.render(Context(dict(doc=doc, ind=ind), autoescape=False))


if Template:
  DjangoEvaluator.register_name('django')
