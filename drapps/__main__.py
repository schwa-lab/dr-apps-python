#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
import os.path
import sys
import imp
import argparse
try:
  import argcomplete
except ImportError:
  argcomplete = None
from .api import App, add_subparsers


def load_plugins(dir):
  for f in os.listdir(dir):
    module_name, ext = os.path.splitext(f)
    if ext == '.py':
      imp.load_source('arbitrary', os.path.join(dir, f))


def main(args=None):
  prog = None
  if args is None:
    args = sys.argv[1:]
    cmd = os.path.basename(sys.argv[0])
    if cmd.startswith('dr-'):
      if cmd.endswith('-py'):
        args.insert(0, cmd[3:-3])
      else:
        args.insert(0, cmd[3:])
      prog = 'dr'
  load_plugins(os.path.join(os.path.dirname(__file__), 'plugins/evaluators'))
  load_plugins(os.path.join(os.path.dirname(__file__), 'plugins/apps'))
  parser = argparse.ArgumentParser(prog=prog)
  add_subparsers(parser, sorted(App.CLASSES.items()), 'app_cls', title='apps')
  if argcomplete is not None:
    argcomplete.autocomplete(parser)
  args = parser.parse_args(args)
  args.app_cls(parser, args)()


if __name__ == '__main__':
  main()
