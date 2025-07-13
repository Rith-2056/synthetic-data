import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text # Import 'text' for executing raw SQL

# Initialize Faker
fake = Faker('en_US')

# --- Data Generation Functions ---

def generate_airlines(num_airlines=5):
    airlines = []
    for i in range(1, num_airlines + 1):
        airlines.append({
            'AirlineID': i,
            'AirlineName': fake.company() + " Airlines",
            'IATA_Code': fake.bothify(text='??').upper(),
            'ICAO_Code': fake.bothify(text='???').upper(),
            'ContactEmail': fake.email(),
            'PhoneNumber': fake.phone_number()
        })
    return pd.DataFrame(airlines)

def generate_airports(num_airports=20):
    airports = []
    for i in range(1, num_airports + 1):
        city = fake.city()
        country = fake.country()
        airports.append({
            'AirportID': i,
            'AirportCode': fake.lexify(text='???', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ').upper(), # Random 3-letter code
            'AirportName': f"{city} International Airport",
            'City': city,
            'Country': country,
            'Timezone': fake.timezone()
        })
    return pd.DataFrame(airports)

def generate_aircrafts(num_aircrafts=50, airlines_df=None):
    aircrafts = []
    if airlines_df is None or airlines_df.empty:
        print("Warning: No airlines provided for aircraft generation. Generating without airline association.")
        airline_ids = [None] * num_aircrafts
    else:
        airline_ids = random.choices(airlines_df['AirlineID'].tolist(), k=num_aircrafts)

    aircraft_types = ["Boeing 737", "Airbus A320", "Boeing 787", "Airbus A350", "Embraer 190"]
    manufacturers = ["Boeing", "Airbus", "Embraer"]

    for i in range(1, num_aircrafts + 1):
        aircraft_type = random.choice(aircraft_types)
        manufacturer = random.choice(manufacturers)
        capacity = random.randint(100, 400) # Example capacity range
        year = random.randint(2000, 2024)

        aircrafts.append({
            'AircraftID': i,
            'AircraftType': aircraft_type,
            'Capacity': capacity,
            'AirlineID': airline_ids[i-1],
            'Manufacturer': manufacturer,
            'YearOfManufacture': year
        })
    return pd.DataFrame(aircrafts)

def generate_flights(num_flights=100, airlines_df=None, airports_df=None, aircrafts_df=None):
    flights = []
    if airlines_df is None or airports_df is None or aircrafts_df is None or airlines_df.empty or airports_df.empty or aircrafts_df.empty:
        raise ValueError("Airlines, Airports, and Aircrafts DataFrames are required to generate flights.")

    airline_ids = airlines_df['AirlineID'].tolist()
    airport_ids = airports_df['AirportID'].tolist()
    aircraft_ids = aircrafts_df['AircraftID'].tolist()

    for i in range(1, num_flights + 1):
        airline_id = random.choice(airline_ids)
        departure_airport_id = random.choice(airport_ids)
        arrival_airport_id = random.choice(airport_ids)
        while departure_airport_id == arrival_airport_id: # Ensure different airports
            arrival_airport_id = random.choice(airport_ids)

        aircraft_id = random.choice(aircraft_ids)
        
        scheduled_departure = fake.date_time_between(start_date='-1y', end_date='+1y')
        flight_duration_hours = random.randint(1, 10)
        scheduled_arrival = scheduled_departure + timedelta(hours=flight_duration_hours, minutes=random.randint(0, 59))

        # Simulate actual times with some delay or early arrival
        delay_minutes = random.randint(-60, 180) # -60 (early) to 180 (3-hour delay)
        actual_departure = scheduled_departure + timedelta(minutes=delay_minutes)
        actual_arrival = scheduled_arrival + timedelta(minutes=delay_minutes + random.randint(-30, 30)) # Additional variability for arrival

        status_options = ["Scheduled", "Departed", "Arrived", "Cancelled", "Delayed"]
        # Determine status based on if scheduled departure is in the past
        if scheduled_departure < datetime.now():
            flight_status = random.choice(["Departed", "Arrived", "Cancelled", "Delayed"])
            if flight_status == "Arrived" and actual_arrival is None: # Ensure consistency if arrived
                actual_arrival = scheduled_arrival + timedelta(minutes=random.randint(-30, 30))
        else:
            flight_status = "Scheduled"

        price = round(random.uniform(50.00, 1000.00), 2)

        flights.append({
            'FlightID': i,
            'FlightNumber': fake.bothify(text=airlines_df[airlines_df['AirlineID'] == airline_id]['IATA_Code'].iloc[0] + '####'),
            'AirlineID': airline_id,
            'DepartureAirportID': departure_airport_id,
            'ArrivalAirportID': arrival_airport_id,
            'AircraftID': aircraft_id,
            'ScheduledDepartureTime': scheduled_departure,
            'ActualDepartureTime': actual_departure if flight_status in ["Departed", "Arrived", "Delayed"] else None,
            'ScheduledArrivalTime': scheduled_arrival,
            'ActualArrivalTime': actual_arrival if flight_status == "Arrived" else None,
            'FlightStatus': flight_status,
            'Gate': fake.bothify(text='G##'),
            'Terminal': fake.random_element(elements=('A', 'B', 'C')),
            'Price': price
        })
    return pd.DataFrame(flights)

def generate_passengers(num_passengers=500):
    passengers = []
    for i in range(1, num_passengers + 1):
        gender = random.choice(['male', 'female'])
        if gender == 'male':
            first_name = fake.first_name_male()
        else:
            first_name = fake.first_name_female()

        passengers.append({
            'PassengerID': i,
            'FirstName': first_name,
            'LastName': fake.last_name(),
            'Email': fake.email(),
            'PhoneNumber': fake.phone_number(),
            'PassportNumber': fake.bothify(text='??########').upper(),
            'DateOfBirth': fake.date_of_birth(minimum_age=1, maximum_age=90),
            'Nationality': fake.country_code(representation="alpha-3")
        })
    return pd.DataFrame(passengers)

def generate_bookings(num_bookings=1000, passengers_df=None, flights_df=None, aircrafts_df=None):
    bookings = []
    if passengers_df is None or flights_df is None or aircrafts_df is None or passengers_df.empty or flights_df.empty or aircrafts_df.empty:
        raise ValueError("Passengers, Flights, and Aircrafts DataFrames are required to generate bookings.")

    passenger_ids = passengers_df['PassengerID'].tolist()
    flight_ids = flights_df['FlightID'].tolist()
    seat_letters = ['A', 'B', 'C', 'D', 'E', 'F']
    classes = ['Economy', 'Business', 'First']
    payment_statuses = ['Paid', 'Pending', 'Refunded']
    booking_statuses = ['Confirmed', 'Cancelled', 'Checked-in']

    for i in range(1, num_bookings + 1):
        passenger_id = random.choice(passenger_ids)
        flight_id = random.choice(flight_ids)
        
        # Ensure seat number is somewhat realistic for capacity
        # Need to correctly link flight_id to aircraft_id to get capacity
        try:
            aircraft_id_for_flight = flights_df[flights_df['FlightID'] == flight_id]['AircraftID'].iloc[0]
            flight_capacity = aircrafts_df[aircrafts_df['AircraftID'] == aircraft_id_for_flight]['Capacity'].iloc[0]
            seat_number = f"{random.randint(1, max(1, flight_capacity // len(seat_letters)))}{random.choice(seat_letters)}"
        except IndexError:
            # Fallback if the link is broken for some reason, though it shouldn't be with proper generation
            seat_number = f"{random.randint(1, 30)}{random.choice(seat_letters)}"


        booking_date = fake.date_time_between(start_date='-1y', end_date='now')

        bookings.append({
            'BookingID': i,
            'PassengerID': passenger_id,
            'FlightID': flight_id,
            'BookingDate': booking_date,
            'SeatNumber': seat_number,
            'Class': random.choice(classes),
            'PaymentStatus': random.choice(payment_statuses),
            'BookingStatus': random.choice(booking_statuses),
            'BaggageAllowance': random.randint(15, 50),
            'CheckinStatus': fake.boolean(chance_of_getting_true=70) # 70% chance of being checked in
        })
    return pd.DataFrame(bookings)

# --- Generate Data ---
print("Generating synthetic data...")
airlines_df = generate_airlines()
airports_df = generate_airports()
aircrafts_df = generate_aircrafts(airlines_df=airlines_df)
flights_df = generate_flights(airlines_df=airlines_df, airports_df=airports_df, aircrafts_df=aircrafts_df)
passengers_df = generate_passengers()
# Pass aircrafts_df to bookings_df to correctly get capacity for seat numbers
bookings_df = generate_bookings(passengers_df=passengers_df, flights_df=flights_df, aircrafts_df=aircrafts_df)
print("Synthetic data generation complete.")

# Display head of generated dataframes (for verification)
print("\n--- Sample Generated Data ---")
print("Airlines Data:")
print(airlines_df.head())
print("\nAirports Data:")
print(airports_df.head())
print("\nAircrafts Data:")
print(aircrafts_df.head())
print("\nFlights Data:")
print(flights_df.head())
print("\nPassengers Data:")
print(passengers_df.head())
print("\nBookings Data:")
print(bookings_df.head())
print("----------------------------")


# --- Database Connection Details ---
DB_USER = "airline-user"
DB_PASSWORD = "fastcat83" # <<< IMPORTANT: REPLACE WITH YOUR CLOUD SQL ROOT PASSWORD
DB_NAME = "airline-data-db" # <<< IMPORTANT: ENSURE THIS DATABASE EXISTS IN YOUR CLOUD SQL INSTANCE
DB_HOST = "127.0.0.1" # Cloud SQL Auth Proxy listens on localhost
DB_PORT = 3306    # Default port for MySQL, and proxy listens on this port

# Create the SQLAlchemy engine
db_connection_str = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = create_engine(db_connection_str)

# --- SQL DDL (Data Definition Language) for Table Creation ---
# Order matters due to foreign key constraints!
# Create tables referenced by foreign keys first.

create_airlines_table_sql = text("""
CREATE TABLE IF NOT EXISTS Airlines (
    AirlineID INT PRIMARY KEY,
    AirlineName VARCHAR(100) NOT NULL,
    IATA_Code VARCHAR(2) UNIQUE,
    ICAO_Code VARCHAR(3) UNIQUE,
    ContactEmail VARCHAR(100),
    PhoneNumber VARCHAR(20)
);
""")

create_airports_table_sql = text("""
CREATE TABLE IF NOT EXISTS Airports (
    AirportID INT PRIMARY KEY,
    AirportCode VARCHAR(3) UNIQUE NOT NULL,
    AirportName VARCHAR(100) NOT NULL,
    City VARCHAR(100),
    Country VARCHAR(100),
    Timezone VARCHAR(50)
);
""")

create_aircrafts_table_sql = text("""
CREATE TABLE IF NOT EXISTS Aircrafts (
    AircraftID INT PRIMARY KEY,
    AircraftType VARCHAR(50) NOT NULL,
    Capacity INT,
    AirlineID INT,
    Manufacturer VARCHAR(50),
    YearOfManufacture INT,
    FOREIGN KEY (AirlineID) REFERENCES Airlines(AirlineID)
);
""")

create_flights_table_sql = text("""
CREATE TABLE IF NOT EXISTS Flights (
    FlightID INT PRIMARY KEY,
    FlightNumber VARCHAR(10) NOT NULL,
    AirlineID INT,
    DepartureAirportID INT,
    ArrivalAirportID INT,
    AircraftID INT,
    ScheduledDepartureTime DATETIME NOT NULL,
    ActualDepartureTime DATETIME,
    ScheduledArrivalTime DATETIME NOT NULL,
    ActualArrivalTime DATETIME,
    FlightStatus VARCHAR(20),
    Gate VARCHAR(10),
    Terminal VARCHAR(10),
    Price DECIMAL(10, 2),
    FOREIGN KEY (AirlineID) REFERENCES Airlines(AirlineID),
    FOREIGN KEY (DepartureAirportID) REFERENCES Airports(AirportID),
    FOREIGN KEY (ArrivalAirportID) REFERENCES Airports(AirportID),
    FOREIGN KEY (AircraftID) REFERENCES Aircrafts(AircraftID)
);
""")

create_passengers_table_sql = text("""
CREATE TABLE IF NOT EXISTS Passengers (
    PassengerID INT PRIMARY KEY,
    FirstName VARCHAR(50) NOT NULL,
    LastName VARCHAR(50) NOT NULL,
    Email VARCHAR(100) UNIQUE,
    PhoneNumber VARCHAR(20),
    PassportNumber VARCHAR(20) UNIQUE,
    DateOfBirth DATE,
    Nationality VARCHAR(50)
);
""")

create_bookings_table_sql = text("""
CREATE TABLE IF NOT EXISTS Bookings (
    BookingID INT PRIMARY KEY,
    PassengerID INT NOT NULL,
    FlightID INT NOT NULL,
    BookingDate DATETIME NOT NULL,
    SeatNumber VARCHAR(5),
    Class VARCHAR(20),
    PaymentStatus VARCHAR(20),
    BookingStatus VARCHAR(20),
    BaggageAllowance INT,
    CheckinStatus BOOLEAN,
    FOREIGN KEY (PassengerID) REFERENCES Passengers(PassengerID),
    FOREIGN KEY (FlightID) REFERENCES Flights(FlightID)
);
""")

# List of DDL statements in order of dependency
ddl_statements = [
    create_airlines_table_sql,
    create_airports_table_sql,
    create_aircrafts_table_sql,
    create_flights_table_sql,
    create_passengers_table_sql,
    create_bookings_table_sql
]

# List of DataFrames and their corresponding table names for insertion
dataframes_to_insert = [
    (airlines_df, 'Airlines'),
    (airports_df, 'Airports'),
    (aircrafts_df, 'Aircrafts'),
    (passengers_df, 'Passengers'), # Passengers before Flights and Bookings
    (flights_df, 'Flights'),
    (bookings_df, 'Bookings')
]

# --- Execute DDL and Insert Data ---
try:
    with engine.connect() as connection:
        # Create tables - execute DDL statements
        print("\nCreating tables (if they don't exist)...")
        for ddl in ddl_statements:
            connection.execute(ddl)
        connection.commit() # Commit DDL changes
        print("Tables created or already exist.")

        # Insert data into tables
        print("\nInserting data into tables...")
        for df, table_name in dataframes_to_insert:
            print(f"Inserting data into {table_name}...")
            # Use 'replace' for testing if you want to clear tables each run
            # Use 'append' for adding new unique data. Be careful with primary key conflicts.
            df.to_sql(table_name, con=engine, if_exists='append', index=False)
        
        print("\nAll data successfully inserted into Cloud SQL.")

except Exception as e:
    print(f"Error during database operations: {e}")
finally:
    if 'engine' in locals():
        engine.dispose() # Close the connection pool
    print("Database connection closed.")