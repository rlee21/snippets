import csv


def csv_basic_io(infile, outfile):
    with open(infile, 'r') as fh:
        csv_reader = csv.reader(fh)
        #skip_header = next(csv_reader)
        with open(outfile, 'w') as fh:
            csv_writer = csv.writer(fh, delimiter='\t')
            for row in csv_reader:
                #print(row[0])
                csv_writer.writerow(row)


def csv_dict_io(infile, outfile_dict):
    with open(infile, 'r') as fh:
        csv_reader = csv.DictReader(fh)
        with open(outfile_dict, 'w') as fh:
            header = ['event_date',
                      'fullVisitorId',
                      'user_session_id',
                      'hit_number',
                      'time','hour',
                      'minute',
                      'hit_type',
                      'Category',
                      'Action',
                      'Label',
                      'user_id',
                      'content_group',
                      'destination_page_url',
                      'page_render_instance_guid',
                      'page_type',
                      'page_identity',
                      'urldomain',
                      'url',
                      'device_type',
                      'model',
                      'country',
                      'userID',
                      'latitude',
                      'longitude',
                      'city',
                      'keyword']
            csv_writer = csv.DictWriter(fh, fieldnames=header, delimiter='|')
            csv_writer.writeheader()
            for row in csv_reader:
                csv_writer.writerow(row)















if __name__ == '__main__':
    infile = 'csv_infile.csv'
    outfile = 'csv_outfile.csv'
    outfile_dict = 'csv_outfile_dict.csv'
    csv_basic_io(infile, outfile)
    csv_dict_io(infile, outfile_dict)
