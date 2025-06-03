import inspect
import functools

def generate_blocking_methods(cls):
    """
    Class decorator that automatically creates blocking versions of async methods.

    For each async method that starts with '_', creates a non-underscore
    blocking version that uses the sync version from self.fs.
    """
    async_methods = []

    # Find all async methods that start with underscore
    for name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
        if name.startswith('_') and inspect.iscoroutinefunction(method):
            # Skip any private methods with double underscore
            if name.startswith('__'):
                continue

            # Get the method name without underscore
            blocking_name = name[1:]

            # Skip if the blocking version is already defined
            if hasattr(cls, blocking_name):
                continue

            async_methods.append((name, blocking_name, method))

    # Now add the blocking versions
    for async_name, blocking_name, async_method in async_methods:
        # Create a factory function that captures the current values
        def make_blocking_method(async_name, blocking_name):
            @functools.wraps(async_method)
            def blocking_method(self, *args, **kwargs):
                fs_method = getattr(self.fs, blocking_name)
                return fs_method(*args, **kwargs)

            # Add docstring
            blocking_method.__doc__ = f"Synchronous version of {async_name}"
            return blocking_method

        # Create the method with the factory and add it to the class
        blocking_impl = make_blocking_method(async_name, blocking_name)
        setattr(cls, blocking_name, blocking_impl)

    return cls
