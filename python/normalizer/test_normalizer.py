import unittest
from normalizer import normalize


class TestNormalize(unittest.TestCase):
    raw_string1 = """{"employee_name": "Adam Deringer", "company_name":"PayScale, Inc.", "started_at": "2010-05-21T17:00:00.000Z"}"""
    raw_string2 = """{"company_name":"PayScale, Inc.", "employees": [{"employee_name": "Adam Deringer", "started_at": "2010-05-21T17:00:00.000Z"}]}"""
    raw_string3 = """[{"company_name":"PayScale, Inc.","employee_name":"Adam Deringer","started_at":"2019-05-21T17:00:00.000Z"}]a"""

    def test_normalize1(self):
        self.assertEqual(
            normalize(self.raw_string1),
            """[{"company_name": "PayScale, Inc.", "employee_name": "Adam Deringer", "started_at": "2010-05-21T17:00:00.000Z", "started_at_valid": true}]"""
        )

    def test_normalize2(self):
        self.assertEqual(
            normalize(self.raw_string2),
            """[{"company_name": "PayScale, Inc.", "employee_name": "Adam Deringer", "started_at": "2010-05-21T17:00:00.000Z", "started_at_valid": true}]"""
        )


    def test_normalize3(self):
        self.assertEqual(
            normalize(self.raw_string3),
            """[{"company_name": "PayScale, Inc.", "employee_name": "Adam Deringer", "started_at": "2019-05-21T17:00:00.000Z", "started_at_valid": false}]"""
        )


if __name__ == '__main__':
    unittest.main()
