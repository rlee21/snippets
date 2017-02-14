import json

with open("PivotTableData.json","r") as file:
	data = json.load(file)
print data["Name"], data["Device Type"]
