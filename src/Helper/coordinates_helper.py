from math import pi, sin, cos, sqrt, atan2

def degreesToRadians(degrees):
    return degrees * pi / 180

def distanceInKmBetweenEarthCoordinates(lat1, lon1, lat2, lon2):
    """
    https://stackoverflow.com/questions/365826/calculate-distance-between-2-gps-coordinates
    """
    earthRadiusKm = 6371

    dLat = degreesToRadians(lat2-lat1)
    dLon = degreesToRadians(lon2-lon1)

    lat1 = degreesToRadians(lat1)
    lat2 = degreesToRadians(lat2)

    a = sin(dLat/2) * sin(dLat/2) + sin(dLon/2) * sin(dLon/2) * cos(lat1) * cos(lat2)
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return earthRadiusKm * c
