-- Create a function to convert JSON coordinates to the desired format
-- DROP FUNCTION opx.convert_coordinates(json_data JSONB)

CREATE OR REPLACE FUNCTION opx.func_convert_coordinates(json_data JSONB)
RETURNS TEXT AS $$
DECLARE
    coordinate_pairs TEXT;
BEGIN
    -- Extract coordinates from JSON
    SELECT COALESCE('[[[' || string_agg('[' || (coord->>'longitude') || ',' || (coord->>'latitude') || ']', ',') || ']]]', '[]')
    INTO coordinate_pairs
    FROM JSONB_ARRAY_ELEMENTS(json_data->'coordinates') AS coord;

    RETURN coordinate_pairs;
END;
$$ LANGUAGE plpgsql;


/*

-- Usage example:
SELECT opx.func_convert_coordinates('
{
    "coordinates": [
        {
            "latitude": 37.92505229509907,
            "longitude": -122.3193465971359
        },
        {
            "latitude": 37.92505229509907,
            "longitude": -122.3193465971359
        },
        {
            "latitude": 37.92505229509907,
            "longitude": -122.3193465971359
        },
        {
            "latitude": 37.92505546875073,
            "longitude": -122.31942706429804
        },
        {
            "latitude": 37.92495179605891,
            "longitude": -122.3194284054174
        },
        {
            "latitude": 37.92494809346007,
            "longitude": -122.3193465971359
        }
    ]
}'::JSONB) AS converted_coordinates;

*/