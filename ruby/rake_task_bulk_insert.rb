namespace :adhoc do

  desc 'force password reset by adding users to password_reset_countdown'
  task force_password_reset: :environment do
    start_time = Time.now
    password_reset_countdown_users = PasswordResetCountdown.pluck(:user_id).to_set
    BATCH_SIZE = 50000
    offset = 0
    while true
      batch = User
                .joins(:email_address)
                .where("user.password_hash != '$invalid' AND
                        user.id NOT IN (5816113, 5816115) AND
                        user.id < 5816117")
                .limit(BATCH_SIZE)
                .offset(offset)
                .pluck("user.id, email_address.email_address")
                .reject { |user| password_reset_countdown_users.include?(user[0]) }
                .map { |user| { user_id: user[0], email: user[1] } }
      break if batch.empty?
      puts "offset: #{offset}"
      PasswordResetCountdown.import(batch, validate: false)
      offset += BATCH_SIZE
    end
    end_time = Time.now
    duration = (end_time - start_time) / 60
    puts "task completed in #{duration.round(2)} minutes"
  end
end

