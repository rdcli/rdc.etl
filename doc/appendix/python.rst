Just enough python
==================

You're interested in data processing, but you're feeling like some of the python constructions used in this doc are
cryptic? Let me try to give a simplistic overview of python specific stuff you'll have to use.


Decorators
::::::::::

Decorators are a shorthand syntax for a very common pattern.

.. code-block:: python

    def uppercase_decorator(f):
        def decorated_function(*args, **kwargs):
            return f(*args, **kwargs).toupper()
        return decorated_function

    def hello(name='World'):
        return 'Hello, {0}'.format(name)

    hello = uppercase_decorator(hello)

This is a bit cumbersome and hard to read, so the decorator syntax can come and help.

.. code-block:: python

    @uppercase_decorator
    def hello(name='World'):
        return 'Hello, {0}'.format(name)

A decorator is a callable that takes another callable as its first parameter and returns a "decorated" version of
the former, probably adding some behavior. The previous example adds an "uppercase the former callable result", which
is as useless as the decorated `hello` function.

In rdc.etl, we use decorator for various common operations. Although their use is recommended for better readability,
there is no obligation.


Decorators in rdc.etl: Transformations
--------------------------------------

Most transformation classes can be used in three different ways. You can instantiate them, subclass them, or use them
as decorators. Using it as a decorator means that it will be instantiated, passing as first parameter the callable
defined just after.

.. code-block:: python

    @Transform
    def my_transform_implementation(hash, channel):
        hash['transformed'] = True
        yield hash

The resulting value of `my_transform_implementation` is an instance of the Transform class, and this code is
equivalent to the following.

.. code-block:: python

    def my_transform_implementation(hash, channel):
        hash['transformed'] = True
        yield hash

    my_transform_implementation = Transform(my_transform_implementation)

However, you have to know a few pitfalls when using this:

* You're getting an instance of `Transform`, which is not re-useable. The first Job into which you'll add this
  instance will bind input and output channels, and further adds to other jobs (or to the same job at another place
  in the graph) will fail.
* You can't pass more than one argument to the constructor.

To avoid those pitfalls, you're advised to use `TransformBuilder` decorator, demonstrated in the next paragraph.

Decorators in rdc.etl: Transform builder
----------------------------------------

To create reusable transformations, you should use `TransformBuilder`.

.. code-block:: python

    @TransformBuilder(Transform, input_channels=(STDIN, STDIN2, ))
    def MyReusableTransform(hash, channel):
        if channel == STDIN:
            hash['from_channel'] = 1
        elif channel == STDIN2:
            hash['from_channel'] = 2
        else:
            raise NotImplementedError('Unknown channel')
        yield hash

The resulting value of `MyReusableTransform` is a `type` subclass, also known as a regular class definition, that
you can instantiate at will.

.. code-block:: python

    instance1 = MyReusableTransform()
    instance2 = MyReusableTransform()
    instance3 = MyReusableTransform()
    # ... you got it, right ?


Return values, iterators, generators
::::::::::::::::::::::::::::::::::::

In python, all callables have a return value. If it is not provided explicitly, it will be `None`.

.. code-block:: python

    def explicit_none():
        return None

    def implicit_none():
        return

    def even_more_implicit_none():
        pass

Some systems, like `rdc.etl` transformations, needs to have more than one return value, and it is important that values
can be passed as soon as each is computed. If we had to wait for thousands of entries to be processed before a return
value can be sent, the system would not be efficient at all.

Generators are one solution.

.. code-block:: python

    def my_slow_range(start, stop):
        for i in xrange(start, stop):
            time.sleep(1)
            yield i

This is a generator. Python, and you, recognize generators because they use the `yield` keyword. A generator can't
have a `return` statement (try it).

When called, a generator will _not_ execute the function body, but return an iterable object. Each time the iterator
next() method is called, the execution of the function will be resumed where it has been left (at the beginning for
example if it is the first call to next()), and run until a yield statement is encountered.

.. code-block:: python

    for i in my_slow_range(0, 10):
        print i



