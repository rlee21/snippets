from collections import namedtuple

def compare_text_files(edw_file, skills_file):
    """ reads text files and converts each to a set and then checks for differences """
    with open(edw_file, 'r') as f:
        edw_data = f.readlines()
        edw_uids = {row for row in edw_data}

    with open(skills_file, 'r') as f:
        skills_data = f.readlines()
        skills_uids = {row for row in skills_data}

    edw_not_skills = edw_uids.difference(skills_uids)
    skills_not_edw = skills_uids.difference(edw_uids)
    Results = namedtuple('Results', ['edw', 'skills'])

    return Results(edw_not_skills, skills_not_edw)


if __name__ == '__main__':
       edw_file = 'edw.txt'
       skills_file = 'skills.txt'
       print(compare_text_files(edw_file, skills_file))
