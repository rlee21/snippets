namespace :adhoc do

  desc 'Remove duplicate endorsements'
  task remove_duplicate_endorsements: :environment do
    start_time = Time.now

    duplicate_rows = ProfessionalEndorsement
                      .group('endorsee_id, endorser_id, specialty_id, relationship_id, endorsement_text')
                      .having('count(*) > 1')

    ids_to_keep = duplicate_rows.pluck('MIN(id)')

    ids_to_delete = ProfessionalEndorsement
                      .joins("INNER JOIN (#{duplicate_rows.to_sql}) AS dupes ON
                              dupes.endorsee_id = professional_endorsement.endorsee_id AND
                              dupes.endorser_id = professional_endorsement.endorser_id AND
                              dupes.specialty_id = professional_endorsement.specialty_id AND
                              dupes.relationship_id = professional_endorsement.relationship_id AND
                              dupes.endorsement_text = professional_endorsement.endorsement_text")
                      .where('professional_endorsement.id NOT IN (?)', ids_to_keep)
                      .pluck('professional_endorsement.id')

    BATCH_SIZE = 1000
    ProfessionalEndorsement.where(id: ids_to_delete).find_in_batches(batch_size: BATCH_SIZE).with_index do |batch, idx|
      puts "deleting #{BATCH_SIZE} rows for batch group: #{idx}"
      ProfessionalEndorsement.where(id: batch.map(&:id)).destroy_all
    end

    check_for_duplicates = ProfessionalEndorsement
                             .group('endorsee_id, endorser_id, specialty_id, relationship_id, endorsement_text')
                             .having('count(*) > 1')
                             .pluck(:id)

    puts "#{ids_to_delete.size} records were removed and now there are #{check_for_duplicates.size} duplicate endorsements"

    end_time = Time.now
    duration = (end_time - start_time) / 60

    puts "task completed in #{duration.round(2)} minutes"
  end
end

