import json
from pprint import pprint


def transform(infile):
    output = dict()
    with open(infile) as datafile:
        for line in datafile:
            line = line.rstrip('\n')
            data = json.loads(line)
            for k, v in data.items():
                if k == "event":
                    output['timestamp'] = v["timestamp"]
                    output['app_id'] = v["app_id"]
                    if v["name"] == "AdClick":
                        output["event_type"] = "ad_click"
                    elif v["name"] == "AdRequest":
                        output["event_type"] = "ad_request"
                    elif v["name"] == "AdImpression":
                        output["event_type"] = "ad_impression"
                else:
                    output[k] = v
            return output

if __name__ == "__main__":
    infile = 'datafile.json'
    pprint(transform(infile))

