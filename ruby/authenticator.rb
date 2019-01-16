users = [
  { username: "mashrur", password: "password1" },
  { username: "jack", password: "password2" },
  { username: "arya", password: "password3" },
  { username: "jonshow", password: "password4" },
  { username: "heisenberg", password: "password5" }
]

puts "Welcome to the authenticator"
25.times { print "-" }
puts
puts "This program will take input from the user and compare the password"
puts "If password is correct, you will get back the user object"


def auth_user(username, password, users)
  users.each do |user|
    if username.chomp == user[:username] and password.chomp == user[:password]
      return user
    end
  end
  return {}
end

attempts = 1
while attempts < 4
  print "username: "
  username = gets.chomp
  print "password: "
  password = gets.chomp
  # input = gets
  user = auth_user(username, password, users)
  if !user.empty?
    puts user
  else
    puts "Credentials were not correct"
  end
  puts "Press n to quit or any other key to continue:"
  input = gets.chomp.downcase
  break if input == 'n'
  attempts +=1
end
puts "You have exceeded the number of attempts" if attempts == 4

