This package provides a collection of tools for working with docrep data, driven by the `dr` command provided by [`libschwa`](https://github.com/schwa-lab/libschwa), and using its [Python API](https://pypi.python.org/pypi/libschwa-python).
It can be installed with:
```bash
pip install git+https://github.com/schwa-lab/dr-apps-python
```

This library provides tools that exploit Python schema models, or which uses Python to express arbitrary functions (e.g. `format`, `grep-py`, `folds`), as well as a tool for easy use of the Python interactive shell to inspect or operate on docrep data (`shell`).

As this library preceded the tools implemented in `libschwa` in C++, it also overlaps in the latter's functionality; this may be deprecated over time.

It currently provides the following commands on top of libschwa's:

```
    dr conll            Writes documents in CONLL format, or a format which
                        similarly lists fields separated by some delimiter.
    dr count-py         Count the number of documents or annotations in named
                        stores.
    dr dump             Debug: unpack the stream and pretty-print it.
    dr folds            Split a stream into k files, or a separate file for
                        each key determined per doc.
    dr format           Print out a formatted evaluation of each document.
    dr generate         Generate empty documents.
    dr grep-py          Filter the documents using an evaluator.
    dr hackheader       Debug: rewrite header components of given documents
                        using Python literal input
    dr ls               List the stores available in the corpus.
    dr offsets          List the byte offset at the start of each document.
    dr shell            Loads the given input file into a Python shell as the
                        variable `docs`
    dr sort             Sort the documents using an evaluator.
    dr srcgen           Generate source code for declaring types as
                        instantiated in a given corpus, assuming headers are
                        identical throughout.
    dr subset           Extract documents by non-negative index or slice (a
                        generalisation of head).
    dr upgrade          Upgrade wire format
```

Use `dr <cmd> --help` for more details.
