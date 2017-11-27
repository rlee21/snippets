

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
    
    if len(edw_not_skills) > 0:
           print("Records in edw file but not in skills file: {}".format(edw_not_skills))
    else:
        print("All records in edw file are present in skills file")

    if len(skills_not_edw) > 0:
        print("Records in skills file  but not in edw file: {}".format(skills_not_edw))
    else:
        print("All records in skills file are present in edw file")


if __name__ == '__main__':
       edw_file = 'edw.txt'
       skills_file = 'skills.txt'
       compare_text_files(edw_file, skills_file)

