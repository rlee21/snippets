import unittest
from normalizer import normalize


class TestNormalize(unittest.TestCase):

    def normalize1(self):
        self.assertEqual(
            normalize.(rawString1),
            """[{"company_name": "PayScale, Inc.", "employee_name": "Adam Deringer", "started_at": "2010-05-21T17:00:00.000Z", "started_at_valid": true}]"""
        )


    def normalize2(self):
        self.assertEqual("""[{"company_name": "PayScale, Inc.", "employee_name": "Adam Deringer", "started_at": "2010-05-21T17:00:00.000Z", "started_at_valid": true}]""")


    def normalize3(self):
        self.assertEqual("""[{"company_name": "PayScale, Inc.", "employee_name": "Adam Deringer", "started_at": "2019-05-21T17:00:00.000Z", "started_at_valid": false}]""")


if __name__ == '__main__':
    unittest.main()
