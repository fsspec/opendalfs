# User guide

The user guide starts with a credential-free example, then covers the complete
path from choosing an OpenDAL service to operating it through fsspec in
production.

::::{grid} 1 2 2 2
:gutter: 3

:::{grid-item-card} Quickstart
:link: quickstart
:link-type: doc

Install `opendalfs` and complete a small read and write workflow.
:::

:::{grid-item-card} Connect to storage
:link: connecting-to-storage
:link-type: doc

Choose between direct construction, installed OpenDAL URLs, and opt-in S3
routing.
:::

:::{grid-item-card} Work with files
:link: common-file-operations
:link-type: doc

Use fsspec operations, mappings, async methods, and caching.
:::

:::{grid-item-card} Run in production
:link: production
:link-type: doc

Configure retries and concurrent writes, then validate service capabilities.
:::
::::

```{toctree}
:hidden:

quickstart
connecting-to-storage
common-file-operations
production
```
