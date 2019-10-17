namespace :adhoc do

  desc 'force password reset for active users that have not recently reset their password'
  task force_password_reset_active_users: :environment do
    users_password_reset = User.where("password_hash NOT LIKE '$scrypt$%' AND password_hash != '$invalid' AND reset_password = 0 AND updated_at > ?", 90.days.ago.at_beginning_of_day())
    before_count = users_password_reset.size
    users_password_reset.update_all(reset_password: 1)
    users_password_reset_updated = User.where("password_hash NOT LIKE '$scrypt$%' AND password_hash != '$invalid' AND reset_password = 1 AND updated_at > ?", 90.days.ago.at_beginning_of_day())
    after_count = users_password_reset_updated.size
    puts "number of records that should have been updated: #{before_count}"
    puts "number of records that were updated:             #{after_count}"
  end

  desc 'force password reset for remaining users that have not recently reset their password'
  task force_password_reset_remaining_users: :environment do
    years = (2007..2019).to_a
    years.each do |year|
      User.select("id").
           where("password_hash NOT LIKE '$scrypt$%' AND
                  password_hash != '$invalid' AND
                  reset_password = 0 AND
                  extract(year from created_at) = ?", year).
           find_in_batches(batch_size: 10000).with_index do |batch, idx|
             puts "updating users that were created in #{year} with batch index #{idx}"
             User.where(id: batch.map(&:id)).update_all(reset_password: 1)
           end
    end
  end
end
