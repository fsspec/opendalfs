def pytest_configure(config):
    # register an additional marker
    config.addinivalue_line(
        "markers",
        "need_capability(*capability): mark test to run only on named capability",
    )
