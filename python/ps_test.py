# import json
# from pprint import pprint


# input1 = """{"employee_name": "Adam Deringer", "company_name":"PayScale, Inc.", "started_at": "2010-05-21T17:00:00.000Z"}"""
# input2 = """{"company_name":"PayScale, Inc.", "employees": [{"employee_name": "Adam Deringer", "started_at": "2010-05-21T17:00:00.000Z"}]}"""

# output_sample = [{"company_name":"PayScale, Inc.","employee_name":"Adam Deringer","started_at":"2010-05-21T17:00:00.000Z"}]

# data = json.loads(input1)
# output = []
# record = {
#     "company_name": data['company_name'],
#     "employee_name": data['employee_name'],
#     "started_at": data['started_at']
# }
# # print(record)
# output.append(record)
# print(output)

# data = json.loads(input2)
# # print(data['employees'][0]['employee_name'])
# output = []
# record = {
#     "company_name": data['company_name'],
#     "employee_name": data['employees'][0]['employee_name'],
#     "started_at": data['employees'][0]['started_at']
# }
# # print(record)
# output.append(record)
# print(output)

# data = json.loads(input2)
# print(list(data.keys()))
# print(data['employee_name'])
# if data['employee_name']:
#     print('y')
# else:
#     print('n')


import json
from datetime import datetime

# rawString = """[{"company_name":"PayScale, Inc.","employee_name":"Adam Deringer","started_at":"2010-05-21T17:00:00.000Z"}]a"""
rawString = {"employee_name": "Adam Deringer", "company_name":"PayScale, Inc.", "started_at": "2010-05-21T17:00:00.000Z"}
# rawString = """{"company_name":"PayScale, Inc.", "employees": [{"employee_name": "Adam Deringer", "started_at": "2010-05-21T17:00:00.000Z"}]}"""
# rawString = {"company_name":"PayScale, Inc.", "employees": [{"employee_name": "Adam Deringer", "started_at": "2010-05-21T17:00:00.000Z"}]}

def RawStringToNormalizedJson(rawString):
    output = []
    try:
        keys = set(rawString.keys())
        if 'employee_name' in keys:
            record = {
                "company_name": rawString['company_name'],
                "employee_name": rawString['employee_name'],
                "started_at": rawString['started_at']
            }
            output.append(record)
            return json.dumps(output)
        elif 'employees' in keys:
            record = {
                "company_name": rawString['company_name'],
                "employee_name": rawString['employees'][0]['employee_name'],
                "started_at": rawString['employees'][0]['started_at']
            }
            output.append(record)
            return json.dumps(output)
        else:
            return 'Employee information not present in input'
    except Exception as e:
        return 'Error malformed input: {} \n '.format(e)


print(RawStringToNormalizedJson(rawString))
