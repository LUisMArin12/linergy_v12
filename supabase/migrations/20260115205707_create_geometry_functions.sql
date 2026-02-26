/*
  # Create Geometry Helper Functions
  
  1. Functions
    - `get_point_coords`: Extract lat/lon from a Point geometry
    - `interpolate_point`: Interpolate between two points based on km
    - `interpolate_line_point`: Get point on a LineString at fraction
*/

-- Function to get coordinates from a Point geometry
CREATE OR REPLACE FUNCTION get_point_coords(p_geom geometry)
RETURNS TABLE (lat float, lon float) AS $$
BEGIN
  RETURN QUERY
  SELECT 
    ST_Y(p_geom)::float as lat,
    ST_X(p_geom)::float as lon;
END;
$$ LANGUAGE plpgsql STABLE;

-- Function to interpolate between two points based on km
CREATE OR REPLACE FUNCTION interpolate_point(
  p_geom1 geometry,
  p_geom2 geometry,
  p_km1 float,
  p_km2 float,
  p_km_target float
)
RETURNS TABLE (lat float, lon float) AS $$
DECLARE
  v_fraction float;
  v_lat1 float;
  v_lon1 float;
  v_lat2 float;
  v_lon2 float;
  v_result_lat float;
  v_result_lon float;
BEGIN
  v_fraction := (p_km_target - p_km1) / (p_km2 - p_km1);
  
  v_lat1 := ST_Y(p_geom1);
  v_lon1 := ST_X(p_geom1);
  v_lat2 := ST_Y(p_geom2);
  v_lon2 := ST_X(p_geom2);
  
  v_result_lat := v_lat1 + (v_lat2 - v_lat1) * v_fraction;
  v_result_lon := v_lon1 + (v_lon2 - v_lon1) * v_fraction;
  
  RETURN QUERY SELECT v_result_lat, v_result_lon;
END;
$$ LANGUAGE plpgsql STABLE;

-- Function to interpolate a point on a LineString at a given fraction
CREATE OR REPLACE FUNCTION interpolate_line_point(
  p_line_geom geometry,
  p_fraction float
)
RETURNS TABLE (lat float, lon float) AS $$
DECLARE
  v_point geometry;
BEGIN
  v_point := ST_LineInterpolatePoint(p_line_geom, p_fraction);
  
  RETURN QUERY
  SELECT 
    ST_Y(v_point)::float as lat,
    ST_X(v_point)::float as lon;
END;
$$ LANGUAGE plpgsql STABLE;
