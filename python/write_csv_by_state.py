import csv


def main(INPUTFILE):
    """ Read main targeted reviews file and write each state to it's own csv file """
    with open(INPUTFILE, 'r') as infile, \
         open('./datafiles/ak_targeted_reviews.csv', 'w', encoding='utf-8', errors='ignore') as ak_file, \
         open('./datafiles/id_targeted_reviews.csv', 'w', encoding='utf-8', errors='ignore') as id_file, \
         open('./datafiles/ne_targeted_reviews.csv', 'w', encoding='utf-8', errors='ignore') as ne_file, \
         open('./datafiles/nm_targeted_reviews.csv', 'w', encoding='utf-8', errors='ignore') as nm_file, \
         open('./datafiles/ny_targeted_reviews.csv', 'w', encoding='utf-8', errors='ignore') as ny_file, \
         open('./datafiles/tx_targeted_reviews.csv', 'w', encoding='utf-8', errors='ignore') as tx_file, \
         open('./datafiles/wa_targeted_reviews.csv', 'w', encoding='utf-8', errors='ignore') as wa_file, \
         open('./datafiles/ct_targeted_reviews.csv', 'w', encoding='utf-8', errors='ignore') as ct_file, \
         open('./datafiles/ga_targeted_reviews.csv', 'w', encoding='utf-8', errors='ignore') as ga_file, \
         open('./datafiles/il_targeted_reviews.csv', 'w', encoding='utf-8', errors='ignore') as il_file, \
         open('./datafiles/ks_targeted_reviews.csv', 'w', encoding='utf-8', errors='ignore') as ks_file, \
         open('./datafiles/ma_targeted_reviews.csv', 'w', encoding='utf-8', errors='ignore') as ma_file, \
         open('./datafiles/nc_targeted_reviews.csv', 'w', encoding='utf-8', errors='ignore') as nc_file:

        fieldnames = ['professional_id','review_id','city_id','city_name','state_id','state_code','review_date','overall_rating','specialty_id_tagged','specialty_name_tagged','keyword_searched','keyword_found','body','body_revised']

        writer_ak = csv.DictWriter(ak_file, delimiter=',', fieldnames=fieldnames)
        writer_id = csv.DictWriter(id_file, delimiter=',', fieldnames=fieldnames)
        writer_ne = csv.DictWriter(ne_file, delimiter=',', fieldnames=fieldnames)
        writer_nm = csv.DictWriter(nm_file, delimiter=',', fieldnames=fieldnames)
        writer_ny = csv.DictWriter(ny_file, delimiter=',', fieldnames=fieldnames)
        writer_tx = csv.DictWriter(tx_file, delimiter=',', fieldnames=fieldnames)
        writer_wa = csv.DictWriter(wa_file, delimiter=',', fieldnames=fieldnames)
        writer_ct = csv.DictWriter(ct_file, delimiter=',', fieldnames=fieldnames)
        writer_ga = csv.DictWriter(ga_file, delimiter=',', fieldnames=fieldnames)
        writer_il = csv.DictWriter(il_file, delimiter=',', fieldnames=fieldnames)
        writer_ks = csv.DictWriter(ks_file, delimiter=',', fieldnames=fieldnames)
        writer_ma = csv.DictWriter(ma_file, delimiter=',', fieldnames=fieldnames)
        writer_nc = csv.DictWriter(nc_file, delimiter=',', fieldnames=fieldnames)

        writer_ak.writeheader()
        writer_id.writeheader()
        writer_ne.writeheader()
        writer_nm.writeheader()
        writer_ny.writeheader()
        writer_tx.writeheader()
        writer_wa.writeheader()
        writer_ct.writeheader()
        writer_ga.writeheader()
        writer_il.writeheader()
        writer_ks.writeheader()
        writer_ma.writeheader()
        writer_nc.writeheader()

        reader = csv.reader(infile, delimiter=',')
        next(reader)

        for row in reader:
            professional_id, review_id, city_id, city_name, state_id, state_code, review_date, overall_rating, specialty_id_tagged, specialty_name_tagged, keyword_searched, keyword_found, body, body_revised = row
            data = {'professional_id': professional_id,
                    'review_id': review_id,
                    'city_id': city_id,
                    'city_name': city_name,
                    'state_id': state_id,
                    'state_code': state_code,
                    'review_date': review_date,
                    'overall_rating': overall_rating,
                    'specialty_id_tagged': specialty_id_tagged,
                    'specialty_name_tagged': specialty_name_tagged,
                    'keyword_searched': keyword_searched,
                    'keyword_found': keyword_found,
                    'body': body,
                    'body_revised': body_revised}
            if state_code == 'AK':
                writer_ak.writerow(data)
            elif state_code == 'ID':
                writer_id.writerow(data)
            elif state_code == 'NE':
                writer_ne.writerow(data)
            elif state_code == 'NM':
                writer_nm.writerow(data)
            elif state_code == 'NY':
                writer_ny.writerow(data)
            elif state_code == 'TX':
                writer_tx.writerow(data)
            elif state_code == 'WA':
                writer_wa.writerow(data)
            elif state_code == 'CT':
                writer_ct.writerow(data)
            elif state_code == 'GA':
                writer_ga.writerow(data)
            elif state_code == 'IL':
                writer_il.writerow(data)
            elif state_code == 'KS':
                writer_ks.writerow(data)
            elif state_code == 'MA':
                writer_ma.writerow(data)
            elif state_code == 'NC':
                writer_nc.writerow(data)


if __name__ == '__main__':
    INPUTFILE = './results/results_20180417_160558.csv'
    main(INPUTFILE)
