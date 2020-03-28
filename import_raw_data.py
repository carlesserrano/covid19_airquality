import urllib.request, urllib.parse, urllib.error
import json
import ssl
import sqlite3
import math

def API_query(service, parms):
    # API base URL
    service_url = 'https://api.openaq.org/v1/'

    # Ignore SSL certificate errors
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    # API URL for retrieving data
    search_url = service_url + service + '?'

    # Build up of the URL to retrieve
    url = search_url + urllib.parse.urlencode(parms)
    print('Retrieving URL:',url)
    uh = urllib.request.urlopen(url, context=ctx)
    data = uh.read().decode()
    print('Retrieved', len(data), 'characters')

    try:
        js = json.loads(data)
    except:
        js = None

    return js

def query_parameters(service):
    
    # Initial query to API to retrieve max number of entries
    parms = dict()
    parms['limit'] = 0

    # Call function to retrieve data from API
    js = API_query(service,parms)

    # Retrieve max number of entries
    found = js['meta']['found']
    
    # Compute number of pages required for the query considering that API results limit is 10000 per page
    API_limit = 10000
    pages = math.ceil (found / API_limit)
    #print ('Pages:', pages)

    # Return max number of entries and number of pages required for the query
    return found, pages

def retrieve_data(service):
    # Compute parameters required for the API query
    found, pages = query_parameters(service)
    print('Entries found:', found)
    print('Number of pages:',pages)

    loop = 1
    API_limit = 10000
    retrieved = 0

    while loop <= pages:
        # Set parameters for the query
        parms = dict()
        
        if pages == 1:
            limit = found
            parms['limit'] = limit
        elif found > API_limit:
            limit = API_limit
            parms['limit'] = limit
        
        parms['page'] = loop

        # Call function to retrieve data from API
        js = API_query(service,parms)
        results = len(js['results'])
        print('Results:',results)
        loop = loop + 1

        # Error control
        errors = 0

        if service == 'countries':

            #Results from the query are inserted into database
            for item in range(results):
                try:
                    v_code = js['results'][item]['code']
                    v_count = js['results'][item]['count']
                    v_locations = js['results'][item]['locations']
                    v_cities = js['results'][item]['cities']
                    v_name = js['results'][item]['name']
                    try:
                        cur.execute('''INSERT INTO Countries (code, count, locations, cities, name)
                        VALUES ( ?, ?, ?, ?, ? )''', (v_code, v_count, v_locations, v_cities, v_name))
                        print('Data inserted into Database:',v_code,v_count,v_locations,v_cities,v_name)
                    except: continue
                except: 
                    print ('Error importing data for country code:', v_code)
                    errors = errors + 1
                    continue

            retrieved = retrieved + results
            
        elif service == 'cities':

            #Results from the query are inserted into database
            for item in range(results):
                try:
                    v_name = js['results'][item]['name']
                    v_country = js['results'][item]['country']
                    v_count = js['results'][item]['count']
                    v_locations = js['results'][item]['locations']
                    
                    try:
                        cur.execute('''INSERT INTO Cities (name, country, count, locations)
                        VALUES ( ?, ?, ?, ? )''', (v_name, v_country, v_count, v_locations))
                        print('Data inserted into Database:',v_name, v_country, v_count, v_locations)
                    except: continue
                except: 
                    print ('Error importing data for city:', v_name)
                    errors = errors + 1
                    continue

            retrieved = retrieved + results

        elif service == 'locations':
            
            #Results from the query are inserted into database
            for item in range(results):
                try:
                    v_id = js['results'][item]['id']
                    v_location = js['results'][item]['location']
                    v_country = js['results'][item]['country']
                    v_city = js['results'][item]['city']
                    v_count = js['results'][item]['count']
                    v_sourcename = js['results'][item]['sourceName']
                    v_firstupdated = js['results'][item]['firstUpdated']
                    v_lastupdated = js['results'][item]['lastUpdated']
                    
                    try:
                        cur.execute('''INSERT INTO Locations (id, location, country, city, count, sourcename, firstupdated, lastupdated)
                        VALUES ( ?, ?, ?, ?, ?, ?, ?, ? )''', (v_id, v_location, v_country, v_city, v_count, v_sourcename, v_firstupdated, v_lastupdated))
                        print('Data inserted into Database:',v_id, v_location, v_country, v_city, v_count, v_sourcename, v_firstupdated, v_lastupdated)
                    except: continue
                except: 
                    print ('Error importing data for location:', v_id)
                    errors = errors + 1
                    continue

            retrieved = retrieved + results

        # Commit changes in database
        conn.commit()
        
    return retrieved,errors

# Prepare DB for importing data from the API
conn = sqlite3.connect('raw_data.sqlite')
cur = conn.cursor()
cur.execute('''CREATE TABLE IF NOT EXISTS Countries
    (code TEXT NOT NULL PRIMARY KEY UNIQUE, count INTEGER, locations INTEGER,
     cities INTEGER, name TEXT)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Cities
    (name TEXT NOT NULL PRIMARY KEY UNIQUE, country TEXT, count INTEGER, locations INTEGER)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Locations
    (id TEXT NOT NULL PRIMARY KEY UNIQUE, location TEXT, country TEXT, city TEXT, count INTEGER,
     sourcename TEXT, firstupdated TEXT, lastupdated TEXT)''')
conn.commit()

print('')
print('************************** Retrieving countries*******************************')
retrieved, errors = retrieve_data("countries")
print('Countries retrieved:',retrieved)
print('************************** Countries retrieved *******************************')
print('')

print('')
print('************************** Retrieving cities**********************************')
retrieved, errors = retrieve_data("cities")
print('Cities retrieved:',retrieved)
print('************************** Cities retrieved **********************************')
print('')

print('')
print('************************** Retrieving locations*******************************')
retrieved, errors = retrieve_data("locations")
print('Locations retrieved:',retrieved)
print('************************** Locations retrieved *******************************')
print('')

# Close database connection
cur.close()
