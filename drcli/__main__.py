#!/usr/bin/env python
import os.path
import sys
import imp
import argparse
from api import App, add_subparsers


def load_plugins(dir):
  for f in os.listdir(dir):
    module_name, ext = os.path.splitext(f)
    if ext == '.py':
      imp.load_source('arbitrary', os.path.join(dir, f))


def main(args):
  load_plugins(os.path.join(os.path.dirname(__file__), 'plugins/evaluators'))
  load_plugins(os.path.join(os.path.dirname(__file__), 'plugins/apps'))
  parser = argparse.ArgumentParser()
  add_subparsers(parser, sorted(App.CLASSES.items()), 'app_cls', title='apps')
  args = parser.parse_args()
  args.app_cls(parser, args)()


if __name__ == '__main__':
  main(sys.argv[1:])
