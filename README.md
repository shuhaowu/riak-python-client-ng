riak-python-client-ng (next generation)
=======================================

Another try at implementing a riak-python-client with as little baggage from
the previous 2 clients as possible.

Prerequisites:

 1. [requests](http://docs.python-requests.org/en/latest/)
 2. [protobuf](http://code.google.com/p/protobuf/)

To enable protobuf support, execute the commands:

    $ cd protosrc
    $ ./sync_protobuf_src_with_basho
    $ ./compile_protobuf

If there is ever an update in https://github.com/basho/riak_pb/tree/master/src 
you could update again. 
