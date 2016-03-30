
"""Tools for Geospatial Information System (GIS) processing

To get a :class:'GIS' use something

    Fill in more doc info
"""



import geopy
import datetime
import re


class GIS(object):
    """CUBI's geospatial framework"""
    

    def __init__(self):
        """
            As class is built out, look to migrate common varibales in initialization.
            This should be a template for geocoding, mapping, and analyzing.
        """
    

    def geocode_address(self, address, geocoder):
        """Geocode a given address with a geocode service.         
        :Paramters:
            - 'address': A single string represented address. Paramter is scrubbed on client, in addition
               to any geocode service processing.
            - 'geocoder': A string represented geocoding service provider. See geopy  documentation at http://geopy.readthedocs.org/en/latest/
               for list of geocode services.

        Method returns dictionary of address items from geocode services. Raw
        output will differ between service API's
        """
        
        # Create geocoder with string object and find address from service
        geolocator = geopy.geocoders.get_geocoder_for_service(geocoder)
        address = re.sub(r'\$|#|"|:', ' ', address)
        location = geolocator().geocode(address)    
        
        # Flatten raw return data for normalization. This must be manually mapped otherwise filed will return None
        try:
            if geolocator.__name__ == 'ArcGIS':
                mapped_items = {
                    'longitude':location.raw['feature']['geometry']['x']
                    ,'latitude': location.raw['feature']['geometry']['y']
                    ,'geocode_address_type': location.raw['feature']['attributes']['Addr_Type']
                    ,'geocode_score': location.raw['feature']['attributes']['Score']
                    ,'max_latitude': location.raw['extent']['ymax']
                    ,'min_latitude': location.raw['extent']['ymin']
                    ,'max_longitude': location.raw['extent']['xmax']
                    ,'min_longitude': location.raw['extent']['xmin']
                    ,'return_address': location.raw['name']
                }
            elif geolocator.__name__ == 'Nominatim':
                mapped_items = {
                    'longitude':location.raw['lon']
                    ,'latitude': location.raw['lat']
                    ,'geocode_address_type': location.raw['type']
                    ,'geocode_score': location.raw['importance']
                    ,'max_latitude': location.raw['boundingbox'][1]
                    ,'min_latitude': location.raw['boundingbox'][0]
                    ,'max_longitude': location.raw['boundingbox'][3]
                    ,'min_longitude': location.raw['boundingbox'][2]
                    ,'return_address': location.raw['display_name']
                }
            else:
                mapped_items = None
        except:
            mapped_items = None
            raise
        
        # Build dictionary of values. Structure may need to be reconsidered for noSQL optimization
        addresses = {
            'address': address,
            'geolocation': [
                {
                    'geolocator': '{0}.{1}'.format(geolocator().__module__, geolocator.__name__),
                    'geocode_datetime': datetime.date.today(),
                    'flat_return': mapped_items,
                    'raw_return' : location.raw,
                },
            ]
        }

        return addresses
