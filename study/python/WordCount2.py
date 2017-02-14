import json


with open("/Users/relee/Documents/NewRelicAnalysis/output_1.json","r") as infile:
    data = json.load(infile)
    # print(data)
    for result in data["results"]:
        for event in result["events"]:
            words = event["deviceType"], event["userAgentName"], event["userAgentOS"], event["appName"], event["appName"]
            # if words[3] == words[4]:
            #     print(True)
            # else:
            #     print(False)
            # print(words)
            word_count = {}
            for word in words:
                # print(word)
                if word in word_count:
                    word_count[word] += 1
                else:
                    word_count[word] = 1
# print(word_count)
result = sorted(word_count, key=word_count.get, reverse=True)[:3]
# max = max(word_count, key=word_count.get)
print(result[0:])
# print(max)