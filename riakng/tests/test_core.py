# -*- coding: utf-8 -*-
# Copyright 2012 Shuhao Wu <shuhao@shuhaowu.com>
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

from __future__ import absolute_import

import unittest

from ..core.http import HTTPTransport
from ..core.pbc import PBCTransport
from ..core.exceptions import RequestError
from . import cleanup_bucket_keys

# (bucket, key) pair and to be deleted when all tests are ran.
# For the times when shit fails. That and we also need to get the test
# server going
bucket_key_cleanups = [] # mmmm global variables

class CoreFeatureTests(object):
    """This class is to be extended and the transports are to be filled in.
    """

    def test_ping(self):
        self.assertTrue(self.transport.ping())

    def test_get_put_delete(self):
        bucket, key = "test_bucket", "test_get_key"
        bucket_key_cleanups.append((bucket, key))

        # Put something into the db
        r = self.transport.put(bucket, key, "hello world", "text/plain")
        self.assertTrue("key" in r)
        self.assertEquals(key, r["key"])
        self.assertTrue("status" in r)
        self.assertEquals("no_content", r["status"])
        self.assertTrue("headers" in r)

        # Now retrieve it...
        r = self.transport.get(bucket, key)
        self.assertEquals("ok", r["status"])
        self.assertTrue("headers" in r)

        sibling = r.get("siblings")[0]
        self.assertEquals("hello world", sibling["data"])

        self.assertTrue(sibling.get("vtag"))
        self.assertTrue(sibling.get("vclock"))
        self.assertTrue(sibling.get("content-type"))
        self.assertTrue(sibling.get("last-modified"))

        self.assertTrue(self.transport.delete(bucket, key))

        with self.assertRaises(RequestError):
            self.transport.get(bucket, key)

    def test_put_no_key(self):
        bucket, key = "test_bucket", None
        r = self.transport.put(bucket, key, "look ma no key!", "text/plain")
        self.assertTrue("key" in r)
        key = r["key"]
        bucket_key_cleanups.append((bucket, key))
        self.assertEquals("created", r["status"])

        r = self.transport.get(bucket ,key)
        self.assertEquals("ok", r["status"])
        self.assertEquals("look ma no key!", r["siblings"][0]["data"])

    def test_put_return_body(self):
        bucket, key = "test_bucket", "test_return_body"
        bucket_key_cleanups.append((bucket, key))
        r = self.transport.put(bucket, key, "returning body", "text/plain",
                               returnbody=True)
        self.assertEquals("ok", r["status"])
        self.assertTrue("data" in r["siblings"][0])
        self.assertEquals("returning body", r["siblings"][0]["data"])

    def test_put_with_indexes(self):
        bucket, key = "test_bucket", "test_put_indexes"
        bucket_key_cleanups.append((bucket, key))

        self.transport.put(bucket, key, "indexes!", "text/plain",
            indexes=[("field1_bin", "test"), ("field2_int", 2)])

        r = self.transport.get(bucket, key)
        self.assertEquals("indexes!", r["siblings"][0]["data"])
        self.assertEquals({"field1_bin": ["test"], "field2_int": [2]},
                          r["siblings"][0]["indexes"])

    def test_put_with_links(self):
        bucket, key1 = "test_bucket", "test_put_links"
        bucket_key_cleanups.append((bucket, key1))

        key2 = "test_linked"
        bucket_key_cleanups.append((bucket, key2))

        self.transport.put(bucket, key2, "linked", "text/plain")

        self.transport.put(bucket, key1, "links!", "text/plain",
            links=[(bucket, key2, bucket)])

        r = self.transport.get(bucket, key1)
        self.assertEquals([(bucket, key2, bucket)], r["siblings"][0]["links"])

    def test_put_with_meta(self):
        bucket, key = "test_bucket", "test_put_meta"
        bucket_key_cleanups.append((bucket, key))

        self.transport.put(bucket, key, "metas!", "text/plain",
                meta={"somemeta": 1, "someothermeta": "lol"})

        r = self.transport.get(bucket, key)
        self.assertEquals("lol", r["siblings"][0]["meta"]["someothermeta"])
        self.assertEquals("1", r["siblings"][0]["meta"]["somemeta"]) # An unfornate side effect

    def test_index_operation_single(self):
        bucket, key1 = "test_bucket", "test_key1"
        key2 = "test_key2"
        bucket_key_cleanups.append((bucket, key1))
        bucket_key_cleanups.append((bucket, key2))

        self.transport.put(bucket, key1, "a", "text/plain",
                indexes=[("email_bin", "test@example.com")])
        self.transport.put(bucket, key2, "b", "text/plain",
                indexes=[("email_bin", "test@example.com")])

        keys = self.transport.index(bucket, "email_bin", "test@example.com")
        self.assertEquals(2, len(keys))
        self.assertTrue(key1 in keys)
        self.assertTrue(key2 in keys)

    def test_index_operation_range(self):
        bucket, key1, key2 = "test_bucket", "test_key1", "test_key2"
        bucket_key_cleanups.append((bucket, key1))
        bucket_key_cleanups.append((bucket, key2))

        self.transport.put(bucket, key1, "a", "text/plain",
                indexes=[("field_int", 2)])
        self.transport.put(bucket, key2, "b", "text/plain",
                indexes=[("field_int", 4)])

        keys = self.transport.index(bucket, "field_int", 2, 3)
        self.assertEquals(1, len(keys))
        self.assertEquals(key1, keys[0])

        keys = self.transport.index(bucket, "field_int", 2, 5)
        self.assertEquals(2, len(keys))
        self.assertTrue(key1 in keys)
        self.assertTrue(key2 in keys)

    def test_mapreduce(self):
        raise NotImplementedError("Implement this unittest!")

    def test_get_buckets(self):
        raise NotImplementedError("Implement this unittest!")

    def test_set_bucket_properties(self):
        raise NotImplementedError("Implement this unittest!")

    def test_get_bucket_properties(self):
        raise NotImplementedError("Implement this unittest!")

    def test_stats(self):
        raise NotImplementedError("Implement this unittest!")

    def test_get_keys(self):
        bucket, key1, key2, key3 = "test_get_keys_bucket", \
                                   "test_key1", "test_key2", "test_key3"

        self.transport.put(bucket, key1, "a", "text/plain")
        self.transport.put(bucket, key2, "a", "text/plain")
        self.transport.put(bucket, key3, "a", "text/plain")

        keys = self.transport.get_keys(bucket)
        self.assertEquals(3, len(keys))
        keys.sort()
        self.assertEquals(key1, keys[0])
        self.assertEquals(key2, keys[1])
        self.assertEquals(key3, keys[2])

    def tearDown(self):
        clean_up_bucket_keys(bucket_key_cleanups)

class PBCCoreTests(unittest.TestCase, CoreFeatureTests):
    def setUp(self):
        if not hasattr(self, "transport"):
            self.transport = PBCTransport()

class HTTPCoreTests(unittest.TestCase, CoreFeatureTests):
    def setUp(self):
        if not hasattr(self, "transport"):
            self.transport = HTTPTransport()

    def test_link_walk(self):
        bucket, key1, key2, key3 = "test_bucket", "test_key1", \
                                   "test_key2", "test_key3"
        bucket_key_cleanups.append((bucket, key1))
        bucket_key_cleanups.append((bucket, key2))
        bucket_key_cleanups.append((bucket, key3))

        self.transport.put(bucket, key1, "test1", "text/plain")
        self.transport.put(bucket, key2, "test2", "text/plain",
                links=[(bucket, key1, bucket)])
        self.transport.put(bucket, key3, "test3", "text/plain",
                links=[(bucket, key2, bucket), (bucket, key1, bucket)])

        l = self.transport.walk_link(bucket, key1, [("_", "_", True)])
        self.assertEquals(1, len(l))
        self.assertEquals(0, len(l[0]))

        l = self.transport.walk_link(bucket, key2, [("_", "_", True)])
        self.assertEquals(1, len(l))
        self.assertEquals(1, len(l[0]))
        self.assertEquals(key1, l[0][0]["key"])

        l = self.transport.walk_link(bucket, key3, [("_", "_", True)])
        self.assertEquals(1, len(l))
        self.assertEquals(2, len(l[0]))
        l[0].sort(key=lambda x: x["key"])
        self.assertEquals(key1, l[0][0]["key"])
        self.assertEquals(key2, l[0][1]["key"])

        l = self.transport.walk_link(bucket, key3, [("_", "_", True), ("_", "_", True)])
        self.assertEquals(2, len(l))
        self.assertEquals(2, len(l[0]))
        self.assertEquals(1, len(l[1]))
        l[0].sort(key=lambda x: x["key"])
        self.assertEquals(key1, l[0][0]["key"])
        self.assertEquals(key2, l[0][1]["key"])
        self.assertEquals(key1, l[1][0]["key"])

if __name__ == "__main__":
    unittest.main(verbosity=2)
    clean_up_bucket_keys(bucket_key_cleanups)

