#!/usr/bin/env ruby

require 'csv'
require 'json'
require 'net/http'


class AttorneyDetails
  INFILE = "input.csv"
  OUTFILE = "output.csv"
  HEADERS = ["id", "full_name", "avvo_pro"]
  ENDPOINT = "http://solicitor.conductor.prod.avvo.com:8890/api/2/lawyers?id="

  def get_ids
    CSV.read(INFILE, :headers => true)
  end

  def call_solicitor(id)
    url = ENDPOINT + id
    uri = URI(url)
    begin
      response = Net::HTTP.get(uri)
      payload = JSON.parse(response, :symbolize_names=>true)
      full_name = payload[:lawyers][0][:firstname] + " " + payload[:lawyers][0][:lastname] 
      avvo_pro = payload[:lawyers][0][:avvo_pro]
    rescue StandardError
      full_name = "Not found"
      avvo_pro = "Not found"
    end
    [id, full_name, avvo_pro]
  end

  def write_results(details)
    CSV.open(OUTFILE, "w", :write_headers => true, :headers => HEADERS) do |csv|
      details.each do |row|
        csv << row
      end
    end
  end

  def run
    attorney_details = []
    # ids = self.get_ids
    ids = get_ids
    ids.each do |id|
      # data = self.call_solicitor(id["professional_id"])
      data = call_solicitor(id["professional_id"])
      attorney_details.push(data)
    end
    # self.write_results(attorney_details)
    write_results(attorney_details)
  end
end

AttorneyDetails.new.run

