import psycopg2
import json

# Your existing code for tables_and_outputs, generate_geojson, and other configurations
#Local Creds : Host=localhost;dbname=drx_fpl_test;user=postgres;password=root;
#Test Site Creds : Host=192.168.168.199;dbname=drx_fpl_test;user=drx_fpl;password=i$OF#Lc1b82!;

# Define an array of table names and output file names
tables_and_outputs = [

    {
        'table': 'vw_conductors_for_export', 
        'output': r'C:\Users\Lenovo\Downloads\ugcables_tblugconductors.geojson' , 
        'filter':'', 
        'columns':'asset_identifier, ug_conductor_id, ug_conductor_number, circuit_id, ug_length_miles, ug_length_kilometers', 
        'order_by':'asset_identifier ASC'
    }
    # Add more tables and output file names as needed
]

def generate_geojson(table_name, output_file, filter=None, columns=None , order_by = None):
#def generate_geojson(table_name, output_file):
    conn = None
    batch_size = 100000  # Adjust the batch size as needed
    offset = 0
    try:
        # Connect to the PostgreSQL server
        print(f'Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(
            host='192.168.168.244',
            dbname='aero_demo',  
            user='drx_demo_admin',
            password='SzM19p{b8#2S',
            port=5433
        )

        # Creating a cursor with name cur.
        cur = conn.cursor()
        print(f'Connected to the PostgreSQL database')

        # Get the total count of rows for the given table
        #cur.execute(f'SELECT COUNT(*) FROM ev.tblcircuits')
        # cur.execute(f'SELECT COUNT(*) FROM "ugcables"."{table_name}" {filter}')
        cur.execute(f'SELECT COUNT(*) FROM "ugcables"."{table_name}" {filter}')
        total_rows = cur.fetchone()[0]

        # Create an empty list to store GeoJSON features
        geojson_features = []

        while offset < total_rows:
            # Construct the query to fetch the rows for the given batch
            cur.execute(f'''
                SELECT {columns}, ST_AsGeoJSON(t.*)::json AS geojson
                FROM "ugcables"."{table_name}" t {filter}
                ORDER BY {order_by} OFFSET {offset} LIMIT {batch_size}
            ''')


            # Fetch the rows (as a list of tuples)
            rows = cur.fetchall()

            # Process the rows and add them to the list of GeoJSON features
            for row in rows:
                geojson_features.append(row[-1])  # The last element is the GeoJSON

            # Move to the next batch
            offset += batch_size

            # Calculate the percentage completed
            percentage = min((offset / total_rows) * 100, 100)
            print(f'Progress: {percentage:.2f}%', end='\r')
        # Close the cursor
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

    # Write the final FeatureCollection to the output file
    with open(output_file, 'w') as f:
        f.write(json.dumps({
        'type': 'FeatureCollection',
        'features': geojson_features
    }, indent=2))

print('GeoJSON generation inprogress.')
# Loop through the array and generate GeoJSON for each table with its custom filter (if provided)
for entry in tables_and_outputs:
    table_name = entry['table']
    output_file = entry['output']
    filters = entry['filter'] if 'filter' in entry else None
    columns = entry['columns'] if 'columns' in entry else '*'
    order_by = entry['order_by'] if 'order_by' in entry else 'fid'
    generate_geojson(table_name, output_file, filters, columns,order_by)
    # generate_geojson(table_name, output_file)

# Process finished indicator
print('GeoJSON generation process finished.')