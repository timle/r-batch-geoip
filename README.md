Batch Geolocate a list of ips in R

Given a local copy of the GeoLiteCity data, and country/region lookup tables, this 
script will join the data sources and then apply the ip range information against a list of
IP addresses. Matching all ip to known region information. 

A large motivation for writing this script is to showcase the power of the wonderful foverlaps 
join from data.table package.
[ a note on ranged joins]

On my machine, this script will geolocate 1 million ips in 14 seconds. 

Relies on  GeoLiteCity data available here:
http://geolite.maxmind.com/download/geoip/database/GeoLiteCity_CSV/GeoLiteCity-latest.zip
With meta information here:
  Region/country lookup tables: http://www.maxmind.com/download/geoip/misc/region_codes.csv
  Country codes: http://dev.maxmind.com/static/csv/codes/iso3166.csv
  
  
