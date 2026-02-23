{#
    GPS Coordinates Anonymization Macro
    ====================================
    
    Reduces the precision of GPS coordinates by rounding to N decimals.
    This creates an approximate area of ~1 km radius when using 2 decimals.
    
    Precision by number of decimals:
    - 0: ~111 km
    - 1: ~11 km
    - 2: ~1.1 km  (recommended for GDPR)
    - 3: ~110 m
    - 4: ~11 m
    
    Args:
        lat_column (str): Name of the latitude column
        lon_column (str): Name of the longitude column
        precision (int): Number of decimals to keep (default: 2)
    
    Returns:
        str: SQL expression for the rounded coordinates
    
    Usage:
        select
            service_id,
            {{ privacy__mask_coordinates('latitude', 'longitude') }}
        from source
    
    Author: JML
    Date: February 2026
#}


{% macro privacy__mask_coordinates(column_name, precision=2) %}
    
    round(cast({{ column_name }} as numeric), {{ precision }}) as {{ column_name }}_anon

{% endmacro %}


{#
    GPS Coordinates Validation Macro
    =================================
    
    Checks whether GPS coordinates fall within valid ranges.
    Latitude: -90 to +90
    Longitude: -180 to +180
    
    Args:
        lat_column (str): Name of the latitude column
        lon_column (str): Name of the longitude column
    
    Returns:
        bool: SQL expression returning true if coordinates are valid
#}


{% macro are_valid_coordinates(lat_column, lon_column) %}
    
    {{ lat_column }} is not null
    and {{ lon_column }} is not null
    and {{ lat_column }} between -90 and 90
    and {{ lon_column }} between -180 and 180

{% endmacro %}


{#
    Distance Calculation Macro (Haversine)
    =======================================
    
    Computes the distance in kilometers between two GPS points
    using the Haversine formula.
    
    Args:
        lat1, lon1: Coordinates of point 1
        lat2, lon2: Coordinates of point 2
    
    Returns:
        float: Distance in kilometers
    
    Usage:
        select
            {{ haversine_distance('lat1', 'lon1', 'lat2', 'lon2') }} as distance_km
        from source
#}


{% macro haversine_distance(lat1, lon1, lat2, lon2) %}
    
    6371 * 2 * asin(
        sqrt(
            pow(sin(radians(({{ lat2 }} - {{ lat1 }})) / 2), 2) +
            cos(radians({{ lat1 }})) * cos(radians({{ lat2 }})) *
            pow(sin(radians(({{ lon2 }} - {{ lon1 }})) / 2), 2)
        )
    )

{% endmacro %}


{#
    Approximate Geohash Creation Macro
    ===================================
    
    Creates an aggregated geographic identifier based on rounding.
    Useful for grouping by area without exposing exact coordinates.
    
    Args:
        lat_column (str): Name of the latitude column
        lon_column (str): Name of the longitude column
        precision (int): Geohash precision (default: 2)
    
    Returns:
        str: SQL expression for the geohash
    
    Usage:
        select
            {{ create_geohash('latitude', 'longitude') }} as geohash
        from source
#}


{% macro create_geohash(lat_column, lon_column, precision=2) %}
    
    concat(
        'geo_',
        cast(round(cast({{ lat_column }} as numeric), {{ precision }}) as varchar),
        '_',
        cast(round(cast({{ lon_column }} as numeric), {{ precision }}) as varchar)
    )

{% endmacro %}


{#
    Full Coordinates Masking Macro
    ================================
    
    Completely removes GPS coordinates and returns NULL.
    To be used for highly sensitive data.
    
    Args:
        lat_column (str): Name of the latitude column
        lon_column (str): Name of the longitude column
    
    Returns:
        str: SQL expression returning NULL
#}


{% macro privacy__mask_full_coordinates(lat_column, lon_column) %}
    
    null as {{ lat_column }}_masked,
    null as {{ lon_column }}_masked

{% endmacro %}
