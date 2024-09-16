from dotenv import load_dotenv
import os
import psycopg2


load_dotenv()


try:
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),  # Ensure the environment variable names match your .env file
        user=os.getenv("DB_USER"),    # Use correct environment variable for DB user
        password=os.getenv("DB_PASSWORD"),  # Correct environment variable for DB password
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    print("Database connection successful")
except psycopg2.OperationalError as e:
    print(f"Error: {e}")
    exit(1)  # Exit if connection fails

# Function to get the band associated with a concert
def get_band_for_concert(concert_id):
    query = """
        SELECT bands.* FROM bands
        JOIN concerts ON bands.id = concerts.band_id
        WHERE concerts.id = %s;
    """
    cur.execute(query, (concert_id,))
    return cur.fetchone()

# Function to get the venue associated with a concert
def get_venue_for_concert(concert_id):
    query = """
        SELECT venues.* FROM venues
        JOIN concerts ON venues.id = concerts.venue_id
        WHERE concerts.id = %s;
    """
    cur.execute(query, (concert_id,))
    return cur.fetchone()

# Function to get all concerts for a specific venue
def get_concerts_for_venue(venue_id):
    query = """
        SELECT * FROM concerts
        WHERE venue_id = %s;
    """
    cur.execute(query, (venue_id,))
    return cur.fetchall()

# Function to get all bands that performed at a specific venue
def get_bands_for_venue(venue_id):
    query = """
        SELECT DISTINCT bands.* FROM bands
        JOIN concerts ON bands.id = concerts.band_id
        WHERE concerts.venue_id = %s;
    """
    cur.execute(query, (venue_id,))
    return cur.fetchall()

# Function to get all concerts performed by a specific band
def get_concerts_for_band(band_id):
    query = """
        SELECT * FROM concerts
        WHERE band_id = %s;
    """
    cur.execute(query, (band_id,))
    return cur.fetchall()

# Function to get all venues where a band has performed
def get_venues_for_band(band_id):
    query = """
        SELECT DISTINCT venues.* FROM venues
        JOIN concerts ON venues.id = concerts.venue_id
        WHERE concerts.band_id = %s;
    """
    cur.execute(query, (band_id,))
    return cur.fetchall()

# Function to check if a concert is a hometown show
def is_hometown_show(concert_id):
    query = """
        SELECT CASE WHEN bands.hometown = venues.city THEN TRUE ELSE FALSE END AS hometown_show
        FROM concerts
        JOIN bands ON concerts.band_id = bands.id
        JOIN venues ON concerts.venue_id = venues.id
        WHERE concerts.id = %s;
    """
    cur.execute(query, (concert_id,))
    return cur.fetchone()[0]

# Function to get a concert introduction string
def get_concert_introduction(concert_id):
    query = """
        SELECT 'Hello ' || venues.city || '!!!!! We are ' || bands.name || ' and we''re from ' || bands.hometown AS introduction
        FROM concerts
        JOIN bands ON concerts.band_id = bands.id
        JOIN venues ON concerts.venue_id = venues.id
        WHERE concerts.id = %s;
    """
    cur.execute(query, (concert_id,))
    return cur.fetchone()[0]

# Function for a band to play in a specific venue
def play_in_venue(band_id, venue_id, concert_date):
    query = """
        INSERT INTO concerts (band_id, venue_id, concert_date)
        VALUES (%s, %s, %s);
    """
    cur.execute(query, (band_id, venue_id, concert_date))
    conn.commit()

# Function to get all introductions for a band
def get_all_introductions_for_band(band_id):
    query = """
        SELECT 'Hello ' || venues.city || '!!!!! We are ' || bands.name || ' and we''re from ' || bands.hometown AS introduction
        FROM concerts
        JOIN bands ON concerts.band_id = bands.id
        JOIN venues ON concerts.venue_id = venues.id
        WHERE concerts.band_id = %s;
    """
    cur.execute(query, (band_id,))
    return [row[0] for row in cur.fetchall()]

# Function to get the band with the most performances
def get_band_with_most_performances():
    query = """
        SELECT bands.*, COUNT(concerts.id) AS performance_count
        FROM bands
        JOIN concerts ON bands.id = concerts.band_id
        GROUP BY bands.id
        ORDER BY performance_count DESC
        LIMIT 1;
    """
    cur.execute(query)
    return cur.fetchone()

# Function to get a concert at a specific venue on a given date
def get_concert_at_venue_on_date(venue_id, concert_date):
    query = """
        SELECT * FROM concerts
        WHERE venue_id = %s AND concert_date = %s
        LIMIT 1;
    """
    cur.execute(query, (venue_id, concert_date))
    return cur.fetchone()

# Function to get the most frequent band at a venue
def get_most_frequent_band_at_venue(venue_id):
    query = """
        SELECT bands.*, COUNT(concerts.id) AS performance_count
        FROM bands
        JOIN concerts ON bands.id = concerts.band_id
        WHERE concerts.venue_id = %s
        GROUP BY bands.id
        ORDER BY performance_count DESC
        LIMIT 1;
    """
    cur.execute(query, (venue_id,))
    return cur.fetchone()

# Testing the functions
concert_id = 1
band = get_band_for_concert(concert_id)
venue = get_venue_for_concert(concert_id)
introduction = get_concert_introduction(concert_id)

if band:
    print("Band:", band)
else:
    print(f"No band found for concert ID: {concert_id}")

if venue:
    print("Venue:", venue)
else:
    print(f"No venue found for concert ID: {concert_id}")

print("Introduction:", introduction)

# Close cursor and connection
cur.close()
conn.close()
