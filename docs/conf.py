from importlib.metadata import version as package_version

project = "opendalfs"
author = "opendalfs contributors"
copyright = "2026, opendalfs contributors"  # noqa: A001

release = package_version("opendalfs")
version = release

extensions = [
    "myst_parser",
    "numpydoc",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
    "sphinx_design",
]

autosummary_generate = True
autodoc_typehints = "description"
numpydoc_show_class_members = False
myst_heading_anchors = 3
myst_enable_extensions = ["colon_fence"]

exclude_patterns = ["_build"]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "fsspec": ("https://filesystem-spec.readthedocs.io/en/latest/", None),
    "opendal": ("https://opendal.apache.org/docs/python/", None),
    "pandas": ("https://pandas.pydata.org/docs/", None),
    "dask": ("https://docs.dask.org/en/stable/", None),
    "xarray": ("https://docs.xarray.dev/en/stable/", None),
}

html_theme = "pydata_sphinx_theme"
html_title = "opendalfs"
html_theme_options = {
    "github_url": "https://github.com/fsspec/opendalfs",
    "navigation_with_keys": True,
    "show_toc_level": 2,
    "use_edit_page_button": True,
}
html_context = {
    "github_user": "fsspec",
    "github_repo": "opendalfs",
    "github_version": "main",
    "doc_path": "docs",
}
