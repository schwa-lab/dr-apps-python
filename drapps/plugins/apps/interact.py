# vim: set et nosi ai ts=2 sts=2 sw=2:
# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals
import os

from drapps.api import App, DECORATE_METHOD
from drapps.appargs import ArgumentParser, DrInputType, argparse, import_string


class ShellApp(App):
  """
  Loads the given input file into a Python shell as the variable `docs`

  Examples:
    %(prog)s -c 'for doc in docs: do_something()'
        # executes the given code on `docs` read with automagic from standard input
    %(prog)s -o out.dr -c 'for doc in docs: do_something() and write_doc(doc)'
        # same, writing the documents to out.dr
    %(prog)s path.dr
        # open an interactive Python shell with `docs` read from path.dr with automagic
    %(prog)s --doc-class pkg.module.DocSchema path.dr
        # same, but using the specified schema

  """
  SHELLS = ('ipython', 'bpython', 'python')
  ap = ArgumentParser()
  ap.add_argument('-s', '--shell', default=None, help='One of {0} (default: try these in order)'.format(SHELLS))
  ap.add_argument('--doc-class', metavar='CLS', dest='doc_class', type=import_string, default=None, help='Import path to the Document class for the input.  If available, doc.{0}() will be called for each document on the stream.'.format(DECORATE_METHOD))
  ap.add_argument('-o', '--out-file', type=argparse.FileType('wb'), default=None, help='The output file, written to by `write_doc`')
  ap.add_argument('-c', '--code', default=None, help='Execute the specified code (before opening an interactive session if -i is also used)')
  ap.add_argument('-i', '--interactive', default=False, action='store_true', help='Use an interactive shell even if -c is supplied')
  ap.add_argument('in_file', type=DrInputType, nargs='?', default=None, help='The input file')
  arg_parsers = (ap,)

  def __init__(self, argparser, args):
    args.interactive = args.interactive or args.code is None
    if args.interactive and not args.in_file:
      argparser.error('Cannot read documents from STDIN in interactive mode. Please provide a path to the documents.')
    if not args.in_file:
      import sys
      args.in_file = sys.stdin
    super(ShellApp, self).__init__(argparser, args)

  def __call__(self):
    local = self.build_locals()
    if self.args.code:
      exec(self.args.code, local)  # XXX: this is actually using globals, not locals
    if not self.args.interactive:
      return

    tmp = local
    local = self.run_startup()
    local.update(tmp)

    shells = [self.args.shell] if self.args.shell else self.SHELLS
    for shell in shells:
      try:
        return getattr(self, 'run_' + shell)(local)
      except ImportError as e:
        pass
    raise e

  def build_locals(self):
    res = {'__name__': '__main__'}
    from schwa import dr
    reader, schema = self.get_reader_and_schema(self.args.in_file)
    res.update({'dr': dr, 'docs': reader})
    if self.args.out_file:
      res['write_doc'] = dr.Writer(self.args.out_file, schema).write
    return res

  def run_startup(self):
    res = {'__name__': '__main__'}
    pythonrc = os.environ.get('PYTHONSTARTUP')
    if pythonrc and os.path.isfile(pythonrc):
      with open(pythonrc, 'rU') as f:
        try:
          exec(f.read(), res)
        except NameError:
          pass
    try:
      exec('import user', res)
    except ImportError:
      pass
    return res

  def run_ipython(self, local):
    try:
      from IPython.terminal.embed import TerminalInteractiveShell
      shell = TerminalInteractiveShell(user_ns=local)
      shell.mainloop()
    except ImportError:
      # IPython < 0.11
      # Explicitly pass an empty list as arguments, because otherwise
      # IPython would use sys.argv from this script.
      from IPython.Shell import IPShell
      shell = IPShell(argv=[], user_ns=local)
      shell.mainloop()

  def run_bpython(self, local):
    import bpython
    bpython.embed(locals_=local)

  def run_python(self, local):
    import code
    try:
      import readline
    except ImportError:
      pass
    else:
      import rlcompleter
      readline.set_completer(rlcompleter.Completer(local).complete)
      readline.parse_and_bind('tab:complete')
    code.interact(local=local)


ShellApp.register_name('shell')
