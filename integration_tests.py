#!/usr/bin/env python3
#
# Copyright © 2020 Collabora Ltd
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# SPDX-License-Identifier: MIT

import os
import random
import unittest
import urllib

from ugcs import AccessTokenProvider, Bucket

class TestUGCS(unittest.TestCase):
    def setUp(self):
        service_account = os.environ["UGCS_TEST_SERVICE_ACCOUNT_FILE"]
        assert(service_account)
        bucket_name = os.environ["UGCS_TEST_BUCKET_NAME"]
        assert(bucket_name)

        self.test_dir = "ugcs-test-" + "".join(random.choices("0123456789", k=10))

        atp = AccessTokenProvider.from_service_account_json(service_account)
        self.bucket = Bucket(bucket_name, atp)

    def tearDown(self):
        l = self.bucket.list(self.test_dir)
        for obj in l.get("items", []):
            self.bucket.delete(obj["name"])

    def test_list_objects(self):
        f1_data = "aaa".encode('utf-8')
        self.bucket.put(self.test_dir + "/f1", f1_data, "text/plain")
        f2_data = "<html>αβγ</html>".encode('utf-8')
        self.bucket.put(self.test_dir + "/f2", f2_data, "text/html")

        l = self.bucket.list(prefix=self.test_dir)
        obj_f1 = [item for item in l["items"] if item["name"] == self.test_dir + "/f1"][0]
        obj_f2 = [item for item in l["items"] if item["name"] == self.test_dir + "/f2"][0]

        self.assertEqual(len(l["items"]), 2)
        self.assertEqual(obj_f1["contentType"], "text/plain")
        self.assertEqual(int(obj_f1["size"]), len(f1_data))
        self.assertEqual(obj_f2["contentType"], "text/html")
        self.assertEqual(int(obj_f2["size"]), len(f2_data))

    def test_get_object(self):
        with open("test.png", "rb") as f1:
            f1_data = f1.read()
        self.bucket.put(self.test_dir + "/f1", f1_data, "image/png")

        data = self.bucket.get(self.test_dir + "/f1")

        self.assertEqual(data, f1_data)
        
    def test_get_object_metadata(self):
        with open("test.png", "rb") as f1:
            f1_data = f1.read()
        self.bucket.put(self.test_dir + "/f1", f1_data, "image/png")

        metadata = self.bucket.get_metadata(self.test_dir + "/f1")

        self.assertEqual(metadata["name"], self.test_dir + "/f1")
        self.assertEqual(metadata["contentType"], "image/png")
        self.assertEqual(int(metadata["size"]), len(f1_data))

    def test_overwrite_object(self):
        f1_data = "aaa".encode('utf-8')
        self.bucket.put(self.test_dir + "/f1", f1_data, "text/plain")

        f1_data_new = "bbbb".encode('utf-8')
        self.bucket.put(self.test_dir + "/f1", f1_data_new, "text/plain")

        data = self.bucket.get(self.test_dir + "/f1")
        self.assertEqual(data, f1_data_new)

    def test_delete_object(self):
        f1_data = "aaa".encode('utf-8')
        self.bucket.put(self.test_dir + "/f1", f1_data, "text/plain")
        f2_data = "bbb".encode('utf-8')
        self.bucket.put(self.test_dir + "/f2", f2_data, "text/plain")

        self.bucket.delete(self.test_dir + "/f1")
        l = self.bucket.list(prefix=self.test_dir)

        self.assertEqual(len(l["items"]), 1)
        self.assertEqual(l["items"][0]["name"], self.test_dir + "/f2")

    def test_operations_on_invalid_object_throw(self):
        with self.assertRaises(urllib.error.HTTPError):
            self.bucket.get(self.test_dir + "/f1")

        with self.assertRaises(urllib.error.HTTPError):
            self.bucket.get_metadata(self.test_dir + "/f1")

        with self.assertRaises(urllib.error.HTTPError):
            self.bucket.delete(self.test_dir + "/f1")


if __name__ == '__main__':
    unittest.main()
