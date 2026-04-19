# qusql-py-mysql-type-plugin

A [mypy](https://mypy.readthedocs.io/) plugin for [qusql-mysql-type](https://pypi.org/project/qusql-mysql-type/) that type-checks MySQL queries at static analysis time.

Enable it in your `pyproject.toml`:

```toml
[mypy]
plugins = qusql_mysql_type_plugin
```

See the [qusql-mysql-type documentation](https://pypi.org/project/qusql-mysql-type/) for how to write typed queries.

See also the examples:

- [`examples/qusql-py-mysql-type-notes`](../examples/qusql-py-mysql-type-notes) -
  simple introductory CLI with uv setup
- [`examples/qusql-py-mysql-type-books`](../examples/qusql-py-mysql-type-books) -
  library catalog with JOINs, enums, dates, and an idempotent migration pattern


## Development

To test locally, run e.g.:
```sh
rm -f *.whl && \
python3 -m pip wheel -e . && \
pip install --user --force-reinstall *.whl --break-system-packages
```
