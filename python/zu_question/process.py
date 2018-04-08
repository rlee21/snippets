import csv
from collections import defaultdict


with open('inventory.csv') as inventory_file, open('orders.csv') as orders_file:
    reader_inventory = csv.DictReader(inventory_file)
    reader_orders = csv.DictReader(orders_file)
    inventory = list(reader_inventory)
    orders = list(reader_orders)

fc = defaultdict(list)
for item in inventory:
  fc[item['product_id']].append(int(item['quantity']))
# {'1': [5, 10], '2': [5, 5], '3': [5, 5]}

for order in orders:
    #####print(order['product_id'], order['quantity'])
    if order['product_id'] in fc:
        if fc[order['product_id']][0] >= int(order['quantity']):
            fulfill = 'Y'
            # TODO: decrement inv qty
        else:
            fulfill = 'N'
            #print(fc[order['product_id']][0], int(order['quantity']))

print(fulfill)
