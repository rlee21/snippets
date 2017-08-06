# How can I fetch more than 1000 record from data store and put all in one single list to pass to Django?

### CONTEXT ###
# SELECT column FROM table
# LIMIT 10 OFFSET 10
# returns results 11-20 in MySQL anD PostgreSQL


# https://stackoverflow.com/questions/264154/how-to-fetch-more-than-1000
def _iterate_table(table, chunk_size=200):
    offset = 0
    while True:
        results = table.query().order(table.key).fetch(chunk_size + 1, offset=offset)
        if not results:
            break
        for result in results[:chunk_size]:
            yield result
        if len(results) < chunk_size + 1:
            break
        offset += chunk_size


