# Given a local copy of the GeoLiteCity data, and country/region lookup tables, this 
# script will join the data sources and then apply the ip range information against a list of
# IP addresses. Matching all ip to known region information. 
# This is primarily made possible by the wonderful foverlaps from data.table package
# 
# Files required:
#   iptcbl_to_geo_csv: This is path to your list of ips. single column. no column names. 
#     ips in binary format, i.e. 255.134.2.333
#     generator package has nice random ip generator, if you don't have one million 
#       ip addrress handy, but would like to test the script
#   iptbl_ip_blocks_2_geoid_csv: Path to local copy of GeoLiteCity-Blocks.csv
#   iptbl_geoid_2_locations_csv: Path to local copy of GeoLiteCity-Location.csv
#   iptbl_codes_region_csv: Path to local copy of region_codes.csv
#   iptbl_codes_country_csv: Path to local copy of iso3166.csv (country codes)
#
#   iptcbl_w_geodat is output table, all ips successuflly joined with regonal data
#
#

list.of.packages <- c("data.table")
# check if list.of.packages is installed, install if not found
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
# package requires
sapply(list.of.packages,require, character.only = TRUE)


#### list of ips to geolocate
# csv. single column. no column names. ips in binary format, i.e. 255.134.2.333
iptcbl_to_geo_csv = '~/mill.csv'
iptcbl_to_geo = fread(iptcbl_to_geo_csv, header = F, sep = ',')
colnames(iptcbl_to_geo) <- c("remote_addr")



#### Geoip reference files
## GeoLiteCity data
# Updated, online, on the first Tuesday of each month.
# Latest file available here:
#   http://geolite.maxmind.com/download/geoip/database/GeoLiteCity_CSV/GeoLiteCity-latest.zip
#   Specifcy locations of GeoLiteCity-Blocks.csv and GeoLiteCity-Location.csv below.
iptbl_ip_blocks_2_geoid_csv = '~/Documents/Maxmind/GeoLiteCity-Blocks.csv'
iptbl_geoid_2_locations_csv = '~/Documents/Maxmind/GeoLiteCity-Location.csv'

## Region/country lookup tables
# 'http://www.maxmind.com/download/geoip/misc/region_codes.csv'
iptbl_codes_region_csv = '~/Documents/Maxmind/region_codes.csv'
# 'http://dev.maxmind.com/static/csv/codes/iso3166.csv'
iptbl_codes_country_csv = '~/Documents/Maxmind/iso3166.csv'

## Other lookup tables (for reference):
#   https://dev.maxmind.com/geoip/legacy/codes/country_continent/
#   https://dev.maxmind.com/geoip/legacy/codes/

## Check that all reference files exist
csv_list = c(iptbl_ip_blocks_2_geoid_csv, 
             iptbl_geoid_2_locations_csv, 
             iptbl_codes_region_csv, 
             iptbl_codes_country_csv);
files_do_exist = sapply(csv_list, file.exists, USE.NAMES = FALSE)
if (any(!files_do_exist)){
  message("Following file(s) are not found")
  print(csv_list[!files_do_exist])
  stop("Geoip reference files missing, cannot complete uc loc data append")}

## read in geoip data
iptbl_ip_blocks_2_geoid = fread(iptbl_ip_blocks_2_geoid_csv, header = T, sep = ',')
iptbl_geoid_2_locations = fread(iptbl_geoid_2_locations_csv, header = T, sep = ',', stringsAsFactors=TRUE)
iptbl_codes_country = fread(iptbl_codes_country_csv, header = F, sep = ',')
colnames(iptbl_codes_country) <- c("country_code","country_name")
iptbl_codes_region = fread(iptbl_codes_region_csv, header = F, sep = ',' , colClasses = 'factor')
colnames(iptbl_codes_region) <- c("country_code","region_code","region_name")

#### Prepare for joins
## normalizing column types between tables, facilitates joining later
# iptbl_geoid_2_locations, regions has leading zeros, but iptbl_codes_region does not
# need to have consistent convention across columns
strip_leading_zeros <- function(val_in) return(gsub("(?<![0-9])0+", "", val_in, perl = TRUE))
iptbl_geoid_2_locations <- iptbl_geoid_2_locations[, region:=as.factor(strip_leading_zeros(region))]
# run it on iptbl_codes_region as well, futureproof in case
iptbl_codes_region <- iptbl_codes_region[, region_code:=as.factor(strip_leading_zeros(region_code))]

## updating column types to match across tables
iptbl_geoid_2_locations <- iptbl_geoid_2_locations[, locId:=as.numeric(locId)]
iptbl_ip_blocks_2_geoid <- iptbl_ip_blocks_2_geoid[, locId:=as.numeric(locId)]
iptbl_ip_blocks_2_geoid <- iptbl_ip_blocks_2_geoid[, startIpNum:=as.numeric(startIpNum)]
iptbl_ip_blocks_2_geoid <- iptbl_ip_blocks_2_geoid[, endIpNum:=as.numeric(endIpNum)]

## update NA = NAMBIA with NAM = Nambia, 'NA' causing problems later on
# iptbl_codes_country
ind <- is.na(iptbl_codes_country$country_code)
iptbl_codes_country$country_code[ind] <- "NAM"
# iptbl_geoid_2_locations
iptbl_geoid_2_locations <- iptbl_geoid_2_locations[, country:=as.character(country)]
ind <- is.na(iptbl_geoid_2_locations$country)
iptbl_geoid_2_locations$country[ind] <- "NAM"
iptbl_geoid_2_locations <- iptbl_geoid_2_locations[, country:=as.factor(country)]
# iptbl_codes_region
ind <- is.na(iptbl_codes_region$country_code)
iptbl_codes_region$country_code[ind] <- "NAM"

### joins
## add region info to IP blocks
# join iptbl_ip_blocks_2_geoid and iptbl_geoid_2_locations
  # iptbl_ip_blocks_2_geoid
    # startIpNum   endIpNum  locId
    # 1:   16777216   16777471     17
    # 2:   16777472   16778239     49
    # 3:   16778240   16779263     17
    # 4:   16779264   16781311  47667
  # iptbl_geoid_2_locations
    # locId country region      city postalCode latitude longitude metroCode areaCode
    # 1:      1      O1                               0.0000    0.0000        NA       NA
    # 2:      2      AP                              35.0000  105.0000        NA       NA
    # 3:      3      EU                              47.0000    8.0000        NA       NA
all_ip = merge(iptbl_ip_blocks_2_geoid, iptbl_geoid_2_locations, all=FALSE, by="locId")
#rename since coloumn name conflict with next joins
colnames(all_ip)[colnames(all_ip) == "country"] <- "country_code"
colnames(all_ip)[colnames(all_ip) == "region"] <- "region_code"

## update country codes with country names
  # all_ip
    #           locId startIpNum   endIpNum country_code region_code  city postalCode latitude longitude metroCode areaCode
    # 1:            2  265525504  265525759           AP                              35.0000  105.0000        NA       NA
    # 2:            2  265529600  265529855           AP                              35.0000  105.0000        NA       NA
    # 2106711: 739223 3058403224 3058403227           IN     12     Naren             32.7833   74.9333        NA       NA
    # 2106712: 740241 1173428096 1173428127           US     MN Roseville      55113  45.0061  -93.1566       613      651
  # iptbl_codes_country
    #       country_code   country_name
    # 1:           A1      Anonymous Proxy
    # 2:           A2      Satellite Provider
all_ip_country = merge(all_ip, iptbl_codes_country, all.x=TRUE, by="country_code") #all.x=TRUE - left outer joing

## update region/country code combinations with region name
  # iptbl_codes_region
    #    country_code  region_code   region_name
    # 1:           AD           2      Canillo
    # 2:           AD           3      Encamp
    # 3:           AD           4      La Massana
all_ip_country_region = merge(all_ip_country, iptbl_codes_region, all.x=TRUE, by=c("country_code","region_code"))
  # > all_ip_country_region
    #    country_code region_code  locId startIpNum   endIpNum     city postalCode latitude longitude metroCode areaCode    country_name        region_name
    # 1:           A1                242   87560192   87560447                       0.0000    0.0000        NA       NA Anonymous Proxy                 NA
    # 2:           A1                242   87950848   87950992                       0.0000    0.0000        NA       NA Anonymous Proxy                 NA
    # 2106711:     ZW           6 281952 3318992282 3318992282   Gwanda            -20.9333   29.0000        NA       NA        Zimbabwe Matabeleland North
    # 2106712:     ZW           6 281952 3318993358 3318993358   Gwanda            -20.9333   29.0000        NA       NA        Zimbabwe Matabeleland North

## cleanup after joins
# remove redundant fields
# all_ip_country_region[,country_code:=NULL]
  all_ip_country_region[,region_code:=NULL]
  all_ip_country_region[,locId:=NULL]
# convert str type, prevent errors later
  all_ip_country_region = all_ip_country_region[,city:=iconv(enc2utf8(as.character(city)),sub="byte"),]
  all_ip_country_region = all_ip_country_region[,country_name:=iconv(enc2utf8(as.character(country_name)),sub="byte"),]
  all_ip_country_region = all_ip_country_region[,region_name:=iconv(enc2utf8(as.character(region_name)),sub="byte"),]

# all_ip_country_region is our main table
#   holds ip ranges alongside all our location data
#   this will be range joined with the input file


### prep imported ip list
## convert ips to int_ips
#   required for doing matching in the all_ip_country_region table
#   the all_ip_country_region table table uses base10 integer IP address, 
#   not binary format in the form of [255.134.2.333]
#split on '.', temp columns to hold new data
ip_split = tstrsplit(iptcbl_to_geo$remote_addr, ".", fixed=TRUE, type.convert=TRUE)
# ip_as_int column for converted ips
iptcbl_to_geo$ip_as_int = (ip_split[[1]] * (256^3)) + (ip_split[[2]]  * (256^2)) + 
                          (ip_split[[3]]  * (256^1)) + (ip_split[[4]])



#ip2long <- function(ip) {
#  # convert IP string to long int. 
#  parts <- unlist(strsplit(ip, '.', fixed=TRUE))
#  # set up a function to bit-shift, then "OR" the octets
#  octets <- function(x,y) bitOr(bitShiftL(x, 8), y)
#  # Reduce applys a funcution cumulatively left to right
#  Reduce(octets, as.integer(parts))
#}
#iptcbl_to_geo$ip_as_int = sapply(iptcbl_to_geo$remote_addr, ip2long, USE.NAMES=FALSE)

# Remove rows that are invalid ip locations
invalid_ip_locs = is.na(iptcbl_to_geo$ip_as_int);
iptcbl_to_geo = iptcbl_to_geo[!invalid_ip_locs,]


### final join
# using the amazing foverlaps from data.table package
iptcbl_to_geo[, dummy := ip_as_int] #dummy required, set up current ip as rage of itself
setkey(all_ip_country_region, startIpNum, endIpNum) #set keys for join
setkey(iptcbl_to_geo, ip_as_int, dummy) #set keys for join 
iptcbl_w_geodat = foverlaps(all_ip_country_region, iptcbl_to_geo, nomatch=0L)[, dummy := NULL]

# final clean
iptcbl_w_geodat[,ip_as_int:=NULL]
iptcbl_w_geodat[,endIpNum:=NULL]


over = Sys.time()


  
