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
import requests
import re
from urllib import quote_plus

from .transport import Transport
from .exceptions import HTTPRequestError, RiakngError

_links_regex = re.compile("</([^/]+)/([^/]+)/([^/]+)>; ?riaktag=\"([^\']+)\"")
def _parse_links(linkstext):
    if not linkstext:
        return []
    links = []
    for link in linkstext.strip().split(","):
        matches = _links_regex.match(link.strip())
        if matches is not None:
            # the regex magic forms 4 groups: ("riak", bucket, key, tag)
            # RiakLink is a 3-tuple: (bucket, key, tag)
            links.append((matches.group(2), matches.group(3), matches.group(4)))
    return links

# O.o who designed this format? Seems like me circa grade 10
# The end ", " will are to connect them together, if you don't like it, [:-2].
# BUG: (???) What happens if they bucket or key has commas in them?
_riak_link_format = "</riak/{bucket}/{key}>; riaktag=\"{tag}\", "

class HTTPTransport(Transport):

    def __init__(self, client_id=None, host="127.0.0.1", port=8098,
                 schema="http"):
        self._client_id = client_id or self.random_client_id()
        self._host = host
        self._port = str(port)
        self._schema = schema
        self._url_prefix = self._schema + "://" + self._host + ":" + self._port

    def ping(self):
        response = requests.get(self._url_prefix + "/ping")
        return response.status_code == 200

    def _parse_object(self, headers, content):
        r = {}
        r["data"] = content
        r["vclock"] = headers["x-riak-vclock"]
        r["links"] = _parse_links(headers["link"])
        r["meta"] = meta = {}
        r["indexes"] = indexes = {}
        for header_key, header_value in headers.iteritems():
            if header_key.startswith("x-riak-meta-"):
                meta[header_key[12:]] = header_value
            elif header_key.startswith("x-riak-index"):
                # TODO: Fix issue with splitting index values with ,
                # Note that this is a riak bug... currently no one is taking
                # a look at it, though.
                i = header_value.split(", ")
                if header_key.endswith("_int"):
                    i = list(map(int, i)) # list is for py3k compat
                indexes[header_key[13:]] = i
        return r

    def _parse_siblings(self, content):
        siblings = content.strip().split("\n")
        r = {"data": siblings[1:]}
        return r

    def _assert_response_code(self, response, expected):
        if response.status_code not in expected:
            raise HTTPRequestError("{0}: {1}".format(response.status_code,
                                                     response.reason),
                                   response.status_code)

    __GET_STATUS = {200: "ok", 300: "multiple_choice", 304: "not_modified"}
    def get(self, bucket, key, r=None, vclock=None, headers=None, **params):
        # TODO: implement get all siblings in one request
        # Check http://docs.basho.com/riak/latest/references/apis/http/HTTP-Fetch-Object/
        if headers is None: headers = {}
        url = "/riak/{bucket}/{key}".format(bucket=quote_plus(bucket),
                                            key=quote_plus(key))

        params.update({"r" : r, "vclock" : vclock})
        response = requests.get(self._url_prefix + url, params=params,
                                headers=headers)

        self._assert_response_code(response, self.__GET_STATUS)

        r = {
            "headers" : response.headers,
            "status" : self.__GET_STATUS[response.status_code]
            }

        if response.status_code == 200:
            r.update(self._parse_object(response.headers, response.text))
        elif response.status_code == 300:
            if headers.get("Accept") == "multipart/mixed":
                # TODO: Get rid of this. Consult:
                # http://docs.basho.com/riak/latest/references/apis/http/HTTP-Fetch-Object/
                raise NotImplementedError("lolwut. I don't know how to handle this yet")
            r.update(self._parse_siblings(response.text))
        else:
            r["data"] = None # No content for 304 not modified

        return r

    __PUT_STATUS = {
            201: "created",
            200: "ok",
            204: "no_content",
            300: "multiple_choice"
    }
    def put(self, bucket, key, content, content_type, meta=None, indexes=None,
            links=None, w=None, vclock=None, headers=None, **params):

        if headers is None:
            headers = {}

        if key is None:
            key = ""

        headers["Content-Type"] = content_type

        if meta:
            for header, value in meta.iteritems():
                headers["X-Riak-Meta-{0}".format(header)] = value

        if indexes:
            index_headers = {}
            for field_key, field_value in indexes:
                values = index_headers.setdefault(
                    "X-Riak-Index-{0}".format(field_key),
                    [],
                )
                values.append(str(field_value))
            for field_key, values in index_headers.iteritems():
                index_headers[field_key] = ", ".join(values)

            headers.update(index_headers)

        if links:
            headers["Link"] = ""
            for link_bucket, link_key, tag in links:
                headers["Link"] += _riak_link_format.format(bucket=link_bucket,
                                                            key=link_key,
                                                            tag=tag)

            # This gets rid of the final ,<space>
            headers["Link"] = headers["Link"][:-2]

        if vclock:
            headers["X-Riak-Vclock"] = vclock

        params.update({"w": w})
        if params.get("returnbody", False):
            params["returnbody"] = "true"

        url = "/riak/{bucket}"
        if key:
            url += "/{key}"

        url = url.format(bucket=quote_plus(bucket), key=quote_plus(key))

        if not key:
            response = requests.post(self._url_prefix + url, content,
                                    params=params, headers=headers)
        else:
            response = requests.put(self._url_prefix + url, content,
                                   params=params, headers=headers)

        self._assert_response_code(response, self.__PUT_STATUS)

        r = {
            "headers": response.headers,
            "status": self.__PUT_STATUS[response.status_code]
        }

        location = response.headers["location"]
        if location:
            # the string after the last slash is the key in http api
            r["key"] = location[location.rindex("/")+1:]
        else:
            if key is None:
                raise RiakngError("Server didn't respond with a key. Check riak for bugs..")
            r["key"] = key

        if response.status_code == 300:
            # TODO: I don't know how to trigger this, but I assume siblings
            #       happens here. Can't be sure. Confirm please.
            r.update(self._parse_siblings(response.text))
        elif response.status_code in (200, 201) and \
                params.get("returnbody", False):
            r.update(self._parse_object(response.headers, response.text))
        # 204 is the other case, but since there's no content?
        return r

    def delete(self, bucket, key, **params):
        url = "/riak/{bucket}/{key}".format(bucket=quote_plus(bucket),
                                            key=quote_plus(key))

        response = requests.delete(self._url_prefix + url, params=params)
        self._assert_response_code(response, (204, 404))
        return True
