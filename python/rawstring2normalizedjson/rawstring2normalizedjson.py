import json
import logging
import re
import time
from datetime import datetime


def RawStringToNormalizedJson(rawString):
    """
    Parse two types of json from raw string and normalize into one json object.
    Also, check to see if started_at is in the past
    :type1: {"employee_name": "Adam Deringer", "company_name":"PayScale, Inc.", "started_at": "2010-05-21T17:00:00.000Z"}
    :type2: {"company_name":"PayScale, Inc.", "employees": [{"employee_name": "Adam Deringer", "started_at": "2010-05-21T17:00:00.000Z"}]}
    :return: [{"company_name":"PayScale, Inc.","employee_name":"Adam Deringer","started_at":"2010-05-21T17:00:00.000Z", "started_at_valid": true/false}]
    """
    output = []
    curr_datetime = datetime.now()
    try:
        for i in re.finditer(r'\{.*\}', rawString):
            data = json.loads(i.group())
        keys = set(data.keys())
        if 'employee_name' in keys:
            started_at = data['started_at']
            started_at_time = time.strptime(started_at[:19], '%Y-%m-%dT%H:%M:%S')
            started_at_datetime = datetime.fromtimestamp(time.mktime(started_at_time))
            started_at_valid = started_at_datetime < curr_datetime
            record = {
                "company_name": data['company_name'],
                "employee_name": data['employee_name'],
                "started_at": data['started_at'],
                "started_at_valid": started_at_valid
            }
            output.append(record)
            return json.dumps(output, sort_keys=True)
        elif 'employees' in keys:
            started_at = data['employees'][0]['started_at'] 
            started_at_time = time.strptime(started_at[:19], '%Y-%m-%dT%H:%M:%S')
            started_at_datetime = datetime.fromtimestamp(time.mktime(started_at_time))
            started_at_valid = started_at_datetime < curr_datetime
            record = {
                "company_name": data['company_name'],
                "employee_name": data['employees'][0]['employee_name'],
                "started_at": started_at,
                "started_at_valid": started_at_valid
            }
            output.append(record)
            return json.dumps(output, sort_keys=True)
        else:
            return logging.warning("String not processed!: json keys 'employee_name' and 'employees' were not present in raw string|{}".format(rawString))
    except Exception as e:
        logging.warning('String not processed!: raw string did not contain json object causing {}|{}'.format(e, rawString))
        pass


if __name__ == '__main__':
    log_format = '%(asctime)s|%(levelname)s|%(message)s'
    logging.basicConfig(
        level=logging.WARNING,
        format=log_format,
        filename='rawstring2normalizedjson.log'
    )
    rawString1 = """{"employee_name": "Adam Deringer", "company_name":"PayScale, Inc.", "started_at": "2010-05-21T17:00:00.000Z"}"""
    rawString2 = """{"company_name":"PayScale, Inc.", "employees": [{"employee_name": "Adam Deringer", "started_at": "2010-05-21T17:00:00.000Z"}]}"""
    rawString3 = """[{"company_name":"PayScale, Inc.","employee_name":"Adam Deringer","started_at":"2019-05-21T17:00:00.000Z"}]a"""
    rawString4 = 'foo'
    rawString5 = """{"name": "Adam Deringer", "company_name":"PayScale, Inc.", "started_at": "2010-05-21T17:00:00.000Z"}"""

    #print(RawStringToNormalizedJson(rawString1))
    #print(RawStringToNormalizedJson(rawString2))
    #print(RawStringToNormalizedJson(rawString3))

    assert RawStringToNormalizedJson(rawString1) == """[{"company_name": "PayScale, Inc.", "employee_name": "Adam Deringer", "started_at": "2010-05-21T17:00:00.000Z", "started_at_valid": true}]"""
    assert RawStringToNormalizedJson(rawString2) == """[{"company_name": "PayScale, Inc.", "employee_name": "Adam Deringer", "started_at": "2010-05-21T17:00:00.000Z", "started_at_valid": true}]"""
    assert RawStringToNormalizedJson(rawString3) == """[{"company_name": "PayScale, Inc.", "employee_name": "Adam Deringer", "started_at": "2019-05-21T17:00:00.000Z", "started_at_valid": false}]"""
    RawStringToNormalizedJson(rawString4)
    RawStringToNormalizedJson(rawString5)
