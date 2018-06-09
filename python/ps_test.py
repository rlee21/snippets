#1st line contain integer denoting number of elements in array
#each line i of n subsequent line where 0<=i<n contains integer describing n
#next line contains integer k, the element that needs to be searched
import json
import re
from datetime import datetime


def RawStringToNormalizedJson(rawString):
    """ Parse json from raw string and normalize """
    output = []
    try:
        for i in re.finditer(r'\{.*\}', rawString):
            data = json.loads(i.group())
        keys = set(data.keys())
        if 'employee_name' in keys:
            record = {
                "company_name": data['company_name'],
                "employee_name": data['employee_name'],
                "started_at": data['started_at']
            }
            output.append(record)
            return json.dumps(output)
        elif 'employees' in keys:
            record = {
                "company_name": data['company_name'],
                "employee_name": data['employees'][0]['employee_name'],
                "started_at": data['employees'][0]['started_at']
            }
            output.append(record)
            return json.dumps(output)
        else:
            return 'Employee information not present in input'
    except Exception as e:
        return 'Error malformed input: {} \n '.format(e)


if __name__ == '__main__':
    # rawString = """[{"company_name":"PayScale, Inc.","employee_name":"Adam Deringer","started_at":"2010-05-21T17:00:00.000Z"}]a"""
    # rawString = """{"employee_name": "Adam Deringer", "company_name":"PayScale, Inc.", "started_at": "2010-05-21T17:00:00.000Z"}"""
    rawString = """{"company_name":"PayScale, Inc.", "employees": [{"employee_name": "Adam Deringer", "started_at": "2010-05-21T17:00:00.000Z"}]}"""
    # rawString = """{"company_name":"PayScale, Inc.", "employees": [{"employee_name": "Adam Deringer", "started_at": "2010-05-21T17:00:00.000Z"}]}"""
    print(RawStringToNormalizedJson(rawString))
