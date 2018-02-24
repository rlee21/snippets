import sys

# @KnownIssues:
# - Does not check for statement order, you could move full select statements
#   around and have the code appear to be logically matching. Fix: add cli args
#   to toggle ordering check or not

# @Improvements:
# - Add more error checking
# - Add argparsing
# - Add line number tracking to alert the user where to look. Maybe also tell
#   the user which statement doesn't match and display the code from both
#   statements.
#   Maybe use a Code class here storing the logic statement the code statement
#   and a link between the two. Something like:
#       dict[logic statement] = (code statement, line number)
# - Arg to check comments as well

def strip_comments(code):
    dash_num = 0
    output = ''
    for ch in code:
        if ch == '-':
            dash_num += 1
            if dash_num == 2:
                break
        else:
            if dash_num > 0:
                output += '-'
            dash_num = 0
            output += ch
    return output

def get_logical_set(path):
    output = set()
    statement = ''
    with open(path) as fh:
        for line in fh:
            line = strip_comments(line).lower().split()
            statement += ''.join(line)
            if statement == '':
                continue
            if statement[-1] == ';':
                output.add(statement)
                statement = ''
    return output

if __name__ == '__main__':
    file_a = sys.argv[1]
    file_b = sys.argv[2]
    if get_logical_set(file_a) == get_logical_set(file_b):
        print("SUCCESS: These two files logically match.")
    else:
        print("FAIL: These two files DO NOT logically match.")
