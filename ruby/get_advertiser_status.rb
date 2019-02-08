#!/usr/bin/env ruby

require 'csv'
require 'json'
require 'net/http'

class AdvertiserStatus
  INFILE = "input.csv"
  OUTFILE = "output.csv"
  OUTFILE_HEADERS = ["professional_id", "advertiser"]
  ENDPOINT = "http://quasi.conductor.corp.avvo.com:8890/api/v1/active_advertisers.json?professional_id="

  def get_professionals
    CSV.read(INFILE, :headers => true)
  end

  def get_advertiser_status(professional_id)
    url = ENDPOINT + professional_id
    uri = URI(url)
    response = JSON.parse(Net::HTTP.get(uri), :symbolize_names => true)
    advertiser = response[:active_advertisers].any?
    [professional_id, advertiser]
  end

  def write_results(rows)
    CSV.open(OUTFILE, "w", :write_headers => true, :headers => OUTFILE_HEADERS) do |csv|
      rows.each do |row|
        csv << row
      end
    end
  end

  def run
    rows = []
    professionals = get_professionals
    professionals.each do |professional|
      row = get_advertiser_status(professional["professional_id"])
      rows << row
    end
    write_results(rows)
  end
end

advertiser_status = AdvertiserStatus.new
advertiser_status.run

