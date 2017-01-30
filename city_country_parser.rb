require 'csv'
require 'net/http'
require 'json'

events = CSV.read("events_hd_export_flat.csv", col_sep: ';', headers: true)

headers = events.headers
headers << "city" << "country"
uri = "https://maps.googleapis.com/maps/api/geocode/json?latlng="

CSV.open("events_with_city_and_country.csv", "a", col_sep: ';') do |csv|
  csv << headers
  counter = 0
  events.each do |event|
    print counter if counter % 100 == 0
    city, country = ["", ""]
    unless event['loc'].split("\'")[1].nil?
      lat, lng = event['loc'].split("\'")[1].split("\ ")
      temp_uri = URI(uri + lat + "," + lng)
      while true
        response = JSON.parse(Net::HTTP.get(temp_uri))
        response["status"] == "OVER_QUERY_LIMIT" ? sleep(1) : break
      end
      if response["status"] != "ZERO_RESULTS"
        ((response["results"].reject(&:empty?))[0]["address_components"]).reject(&:empty?).each do |component|
          city = component["long_name"] if component["types"].include?("locality")
          country = component["long_name"] if component["types"].include?("country")
        end
      end
    end
    event = event.fields
    city.empty? ? event.push(" ") : event.push(city)
    country.empty? ? event.push(" ") : event.push(country)
    csv << event
    counter += 1
  end
end
