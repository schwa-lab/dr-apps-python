
import os
from brownie.importing import import_string
from drcli.api import App, DECORATE_METHOD
from drcli.appargs import ArgumentParser, argparse


class ShellApp(App):
  """Loads the given input file into a Python shell as the variable `docs`"""
  # TODO: support -c, -i: -c should have access to docs, but precede startup scripts run with -i

  SHELLS = ('ipython', 'bpython', 'python')
  ap = ArgumentParser()
  ap.add_argument('-s', '--shell', default=None, help='One of {0} (default: try these in order)'.format(SHELLS))
  ap.add_argument('--doc-class', metavar='CLS', dest='doc_class', type=import_string, default=None, help='Import path to the Document class for the input.  If available, doc.{0}() will be called for each document on the stream.'.format(DECORATE_METHOD))
  ap.add_argument('in_file', type=argparse.FileType('rb'), help='The input file')
  arg_parsers = (ap,)

  def __call__(self):
    local = self.build_locals()
    shells = [self.args.shell] if self.args.shell else self.SHELLS
    for shell in shells:
      try:
        return getattr(self, 'run_' + shell)(local)
      except ImportError, e:
        pass
    raise e

  def build_locals(self):
    res = {'__name__': '__main__'}

    pythonrc = os.environ.get("PYTHONSTARTUP")
    if pythonrc and os.path.isfile(pythonrc):
      try:
        exec open(pythonrc) in res
      except NameError:
        pass
    exec 'import user' in res

    from schwa import dr
    res.update({'dr': dr, 'docs': dr.Reader(self.args.doc_class).stream(self.args.in_file)})
    return res

  def run_ipython(self, local):
    try:
      from IPython.frontend.terminal.embed import TerminalInteractiveShell
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
