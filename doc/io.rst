Input / Output
==============

..todo:: TODO write and schematize this

Thread runs self.transform.step()

If transform is not yet initialized, transform runs initialize().

Transform reads from input multiplexer.

Transform runs transform(data, channel).

Transform sends each yielded data, channel to output demultiplexer.

When all inputs are terminated (meaning input multiplexer is terminated too), transform runs finalize(), then sends
the EndOfStream special token to the output multiplexer.


IReadable defines something we can read from.

IWritable defines something we can write into.

OutputMultiplexer is an IWritable that will decode each data put() into it into a (data, channel) tuple, and send data
to targeted IWritable for the given channel.

InputMultiplexer is a IReadable that allows to get() data from a list of channel-assigned IReadable, and returns a
(data, channel) tuple for use in transformations.

Input implements IReadable and IWritable to make the glue between InputMultiplexer and OutputMultiplexer. While it's
"owned" by the InputMultiplexer, OutputMultiplexer gets a reference to use as "targets" (via the IWritable interface).

Transformation - Input side
:::::::::::::::::::::::::::

 _ _ _                ___________       ________________
|_|_|_| (stdin) ---> |           |     |                |
 _ _ _               | Input MUX | --> | Transformation |
|_|_|_| (stdin2) --> |___________|     |________________|

The input side owns the input queues (Input), that contains the real data. Of course, stdin and stdin2 are just an
example and a transformation may have only one input, or a huge list of inputs. Most transformations will only have one
input though.

Queues have no idea of who will put() data, and thus we use the `Begin` token to declare ourselves.

Transformation - Output side
::::::::::::::::::::::::::::

 ________________       _____________
|                |     |             | --> (stdout)
| Transformation | --> | Output DMUX |
|________________|     |_____________| --> (stderr)

stdout/stderr are "named" channels, and on the output dmux side there may be a list of input queue references registered
as "targets" for those.

