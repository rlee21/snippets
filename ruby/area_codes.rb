area_codes = {
  "newyork" => "212",
  "newbrunswick" => "732",
  "edison" => "908",
  "plainsboro" => "609",
  "sanfrancisco" => "309",
  "miami" => "305",
  "paloalto" => "650",
  "evanston" => "847",
  "orlando" => "407",
  "lancaster" => "717"
}

# Get city names from the hash
def get_city_names(area_codes)
  area_codes.each do |city, code|
   puts "#{city}"
   # puts "The area code for #{city} is #{code}."
  end
end

# Get area code based on given hash and key
def get_area_code(area_codes, city)
  area_code = area_codes[city]
  puts "The area code for #{city} is #{area_code}"
end


# Execution flow
get_city_names(area_codes)
loop do
  puts "Do you want to lookup an area code based on a city name? (Y/N)"
  answer = gets.chomp.downcase
  break if answer != "y"
  print "Enter city name: "
  city = gets.chomp.downcase
  get_area_code(area_codes, city)
end

