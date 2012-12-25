# -*- coding: utf-8 -*-
# Copyright 2012 Shuhao Wu <shuhao@shuhaowu.com>
# Copyright 2010 Rusty Klophaus <rusty@basho.com>
# Copyright 2010 Justin Sheehy <justin@basho.com>
# Copyright 2009 Jay Baird <jay@mochimedia.com>
#
# This file is provided to you under the Apache License,
# Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain
# a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import base64
import random
import platform
import threading
import os

class Transport(object):
    """Lowest level of API which handles the transports,
    which handles communicating with the server.

    All protocals should implement this class, according to the specification
    in the docstrings.

    Some basic formats:
        - Link: (bucket, key, tag) <- 3 item tuple
        - 2i:   (field, value)     <- 2 item tuple

    Format for a riak object:
        {
            "key": key, # Optional, not present during a get request
            "vtag": vtag, # Optional, only if riak returns
            "vclock": vclock, # Optional, only if riak returns
            "deleted": True/False, # Only in PBC
            "content-type": content_type,
            "last-modified": last_modified,
            "meta": meta # meta that you defined
            "links": links # riak links in the format list above
            "indexes": indexes # indexes are listed with a dictionary with key: [index1, index2]
            "data": The data in a string format
        }
    """

    def __init__(self, cm=None, client_id=None):
        """Initialize a new transport class.

        Note that subclass that implements this should have all arguments be
        keyword arguments. cm and client_id should always exists.

        :param cm: Connection Manager Instance.
        :param client_id: A client ID.
        """
        raise NotImplementedError

    @classmethod
    def random_client_id(cls):
        return "py2_%s" % base64.b64encode(str(random.randint(1, 0x40000000)))

    @classmethod
    def fixed_client_id(cls):
        machine = platform.node()
        process = os.getpid()
        thread = threading.currentThread().getName()
        return base64.b64encode("%s|%s|%s" % (machine, process, thread))

    def ping(self):
        """Check if server is alive.

        :rtype: Returns a boolean.
        """
        raise NotImplementedError

    def get(self, bucket, key, r=None, vclock=None, headers=None, **params):
        """Get from the database.

        :param bucket: The bucket name
        :type bucket: string
        :param key: The key name
        :type key: string
        :param r: The R value, defaults to None, which is the db default
        :type r: integer
        :param vclock: The riak vector clock value, ignored with PBC client
        :type vclock: string
        :param headers: Additional header parameters, check
                        http://docs.basho.com/riak/latest/references/apis/http/HTTP-Fetch-Object/
                        for details. Example: `headers={"Accept": "multipart/mixed"}`
                        Note: this parameter is ignored with the pbc client.
        :type headers: dict
        :param params: Additional optional parameters. Check the same link as
                       headers. Does not include r or vclock
        :rtype: Returns a dictionary. This dictionary can be a little messy:
                "status" : "ok" or "multiple_choice" or "not_modified"
                "headers" : the http response headers if the transport is http
                "siblings": [object_data], object_data has the format of the
                            riak object listed in the Transport class's
                            docstring.
        """
        raise NotImplementedError

    def put(self, bucket, key, content, content_type, meta=None, indexes=None,
            links=None, w=None, vclock=None, headers=None, **params):

        """Store object into the database

        Note: You can pass a key of None into the db as Riak could generate keys

        :param bucket: The bucket name.
        :type bucket: string
        :param key: The key. If None, Riak will generate and return a key.
        :type key: string or None
        :param content: The content/body/entity for the PUT/POST request.
        :type content: string
        :param content_type: The content type of `content`. Something like
                             `"application/json"`
        :type content_type: string
        :param meta: The meta headers. The dictionary keys will be appended to
                     the string `"X-Riak-Meta-"`
        :type meta: dict
        :param indexes: The indexes as an iterable of 2-item tuples in the
                        format of `(field, value)`
        :type indexes: iterable of 2 item tuples
        :param links: The links as an interable of 3-item tuples in the format
                      of `(bucket, key, tag)`. `tag` can be `None` if not needed
        :type links: iterable of 3 item tuples
        :param w: The w value. Defaults to None, which is the db default
        :param vclock: The riak vector clock
        :type vclock: string
        :param headers: A dictionary of headers. Check
                        http://docs.basho.com/riak/latest/references/apis/http/HTTP-Store-Object/
                        for details. Again, these key values will be appended to
                        X-Riak-Meta-
        :type headers: dict
        :param params: Any optional parameters as indicated in the same page.
        :rtype: A dictionary of things returned from Riak. This dictionary will
                always include 'key', which is the key of the new/updated object
                If returnbody=True, the response will look exactly like the get
                return, except with the added 'key' field. (i.e. the dictionary
                will now contain a "key" and a "siblings" field)
        """
        raise NotImplementedError

    def delete(self, bucket, key, **params):
        """Deletes an object from the database.

        :param bucket: The bucket name.
        :param key: The key name
        :param params: Check
                       http://docs.basho.com/riak/latest/references/apis/http/HTTP-Delete-Object/
                       for optional parameters
        :rtype: boolean. True if deleted (204 or 404). Never returns false as
                an error will be raised instead (HTTPRequestError)
        """
        raise NotImplementedError

    def index(self, bucket, field, start, end=None):
        """Perform an indexing operation.

        :param bucket: The bucket name
        :param field: The field name
        :param start: The start value
        :param end: The end value. Defaults to None. If left as None, start w
                    be used as an exact value.
        :rtypes: A list of keys.
        """
        raise NotImplementedError

    def walk_link(self, bucket, key, link_phases):
        """Walks link.

        :param bucket: The bucket of the object performing the link walk on
        :param key: The key of the object performing the link walk on
        :param link_phases: A list of link phases, which are a 3-tuple consists
                            of (bucket, tag, keep), where bucket is the bucket
                            to restrict links to, the tag, and keep indicates
                            whether to return results from this phase.
                            For more info: http://docs.basho.com/riak/latest/references/apis/http/HTTP-Link-Walking/
        :rtype: A list of objects similar to what get returns.
        """
        raise NotImplementedError

    def mapreduce(self, inputs, query, timeout=None):
        """Map reduces on the database.

        :param input: The input
        :param query: The query dictionary
        :param timeout: Timeout values.
        :rtype: The JSON that riak returns"""
        raise NotImplementedError

    def get_keys(self, bucket):
        """Gets a list of keys from the database.

        Not recommended for production as it is very very slow! Requires
        traversing through ALL keys in the cluster regardless of the bucket"s
        key size.

        :param bucket: The bucket name
        :rtype: A list of keys.
        """
        raise NotImplementedError

    def get_buckets(self):
        """Get a list of bucket from the database.

        Note recommended for production as it is very very slow! Requires
        traversing through the entire set of keys.

        :rtype: A list of buckets from the database"""
        raise NotImplementedError

    def get_bucket_properties(self, bucket):
        """Get a list of bucket properties.

        :param bucket: The bucket name
        :rtype: A dictionary of bucket properties.
        """
        raise NotImplementedError

    def set_bucket_properties(self, bucket, properties):
        """Sets bucket properties. Raises an error if fails.

        :param bucket: The bucket name
        :param properties: A dictionary of properties
        """
        raise NotImplementedError

    def stats(self):
        """Get Riak status.

        See http://docs.basho.com/riak/latest/references/apis/http/HTTP-Status/
        for details.

        :rtype: The JSON that is the Riak status"""
        raise NotImplementedError

    class SolrTransport(object):
        def add_index(self, index, docs):
            """Add index to a Riak Search cluster. Only works under HTTP.
            From the solr interface.

            :param index: The index name
            :type index: string
            :param docs: A list of documents to be indexed by Riak Search
            :type docs: A list of dictionary containing the documents. (dict)
                        Dictionary must include id.
            """
            raise NotImplementedError

        def delete_index(self, index, docs=None, queries=None):
            """Delete indexed documents from the solr interface

            :param index: The index name
            :param docs: A list of document ids.
            :param queries: using queries to delete.
            """
            raise NotImplementedError

        def search(self, index, query, params={}): # should be okay. params should not be modified
            """Perform a query from the solr interface

            :param index: The index name
            :param query: The query
            :param params: The parameters on top query.
            """
            raise NotImplementedError
