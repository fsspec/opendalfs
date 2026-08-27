# Contributing to the documentation

The documentation source is Markdown parsed by MyST and built by Sphinx.

## Build the site

Install the locked development dependencies, then run the strict build:

```console
just install
just docs
just docs-examples
```

The strict Sphinx build writes the site to `docs/_build/html`.
`docs-examples` executes every Python block in the documentation in the order
it appears on its page.

Use the live-reload server while editing:

```console
just docs-serve
```

## Choose the page type

Keep each page focused on one kind of reader need:

- A tutorial leads a new user to a working result.
- A how-to guide solves a specific task.
- An explanation describes how or why the adapter behaves as it does.
- A reference page records options, protocols, compatibility, or API details.

Do not turn a reference page into a walkthrough or place a long conceptual
aside inside a task recipe. Link between pages when a reader needs both.

## Document an integration

Add or update an integration test before claiming compatibility. An integration
page should include:

1. The library's public fsspec entry form.
2. A small example using the same form as the test.
3. The operations covered by CI.
4. Known limits that affect the example.
5. A link to the test file and the library's documentation.

Use "CI-tested" for evidence from the repository. Avoid broader terms such as
"fully supported" unless the project defines and verifies that scope.

## Link to upstream references

OpenDAL owns service configuration and backend capability details. fsspec owns
the generic filesystem contract. Link to those references instead of copying
tables that will drift.

Sphinx intersphinx mappings are available for Python, fsspec, OpenDAL, pandas,
Dask, and Xarray. Prefer cross-references where the upstream inventory contains
the target.
