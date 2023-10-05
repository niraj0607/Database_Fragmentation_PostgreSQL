import datetime
import psycopg2
import random


DATABASE_NAME = 'assignment1'
SALES_REGION_TABLE = 'sales_region'
LONDON_TABLE = 'london'
SYDNEY_TABLE = 'sydney'
BOSTON_TABLE = 'boston'
SALES_TABLE = 'sales'
SALES_2020_TABLE = 'sales_2020'
SALES_2021_TABLE = 'sales_2021'
SALES_2022_TABLE = 'sales_2022'
REGIONS = ["Boston", "Sydney", "London"]
PRODUCT_NAMES = ["Product_A", "Product_B", "Product_C", "Product_D", "Product_E"]

def create_database(dbname):
    """Connect to the PostgreSQL by calling connect_postgres() function
       Create a database named {DATABASE_NAME}
       Close the connection"""
    connection = connect_postgres(dbname)
    if connection is not None:
        try:
            cursor = connection.cursor()
            connection.autocommit = True
            cursor.execute(f"CREATE DATABASE {dbname};")
            print(f"Database '{dbname}' created successfully.")
        except psycopg2.Error as e:
            print(f"Error creating database '{dbname}':", e)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed.")
    


def connect_postgres(dbname):
    """Connect to the PostgreSQL using psycopg2 with default database
       Return the connection"""
    try:
        connection = psycopg2.connect(
            user="postgres",
            password="niraj123",
            host="localhost",
            port="5432",
            database=dbname
        )
        return connection
    except psycopg2.OperationalError as e:
        print(f"Connecting to Default Database to create new database '{dbname}'")
        try:
            # If the specified database doesn't exist, connect to the default database
            connection = psycopg2.connect(
                user="postgres",
                password="niraj123",
                host="localhost",
                port="5432",
                database="postgres"  # Use your default database here
            )
            return connection
        except psycopg2.Error as e:
            print("Error connecting to the default database:", e)
            return None


def list_partitioning(conn):
    """Function to create partitions of {SALES_REGION_TABLE} based on list of REGIONS.
       Create {SALES_REGION_TABLE} table and its list partition tables {LONDON_TABLE}, {SYDNEY_TABLE}, {BOSTON_TABLE}
       Commit the changes to the database"""
    try:
        cursor = conn.cursor()
        
        # Create the main table SALES_REGION_TABLE
        cursor.execute(f"CREATE TABLE {SALES_REGION_TABLE} (id SERIAL, region VARCHAR(50), amount INTEGER) PARTITION BY LIST (region);")
        
        # Create list partitions for each region
        for region in REGIONS:
            cursor.execute(f"CREATE TABLE {SALES_REGION_TABLE}_{region.lower()} PARTITION OF {SALES_REGION_TABLE} FOR VALUES IN ('{region}');")
        
        conn.commit()
        print("List partitioning created successfully.")
    except psycopg2.Error as e:
        conn.rollback()
        print("Error creating list partitions:", e)


def insert_list_data(conn):
    """ Generate 50 rows data for {SALES_REGION_TABLE}
        Execute INSERT statement to add data to the {SALES_REGION_TABLE} table.
        Commit the changes to the database"""
    try:
        cursor = conn.cursor()
        for _ in range(50):
                region = random.choice(REGIONS)
                amount = random.randint(100,1000)
                cursor.execute(f"INSERT INTO {SALES_REGION_TABLE} (region, amount) VALUES ('{region}', '{amount}');")
        
        conn.commit()
        print("Data inserted into SALES_REGION_TABLE successfully.")
    except psycopg2.Error as e:
        conn.rollback()
        print("Error inserting data:", e)



def select_list_data(conn):
    """Select data from {SALES_REGION_TABLE}, {BOSTON_TABLE}, {LONDON_TABLE}, {SYDNEY_TABLE} seperately.
       Print each tables' data.
       Commit the changes to the database
    """
    try:
        cursor = conn.cursor()
        print(f"\nData from {SALES_REGION_TABLE}:\n")
        cursor.execute(f"\nSELECT * FROM {SALES_REGION_TABLE};\n")
        data = cursor.fetchall()
        for row in data:
                print(row)
        for region in REGIONS:
            print(f"\nData from {SALES_REGION_TABLE}_{region.lower()}:\n")
            cursor.execute(f"SELECT * FROM {SALES_REGION_TABLE}_{region.lower()};")
            data = cursor.fetchall()
            for row in data:
                print(row)
        
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print("Error selecting data:", e)

def range_partitioning(conn):
    """Function to create partitions of {SALES_TABLE} based on range of sale_date.
       Create {SALES_REGION_TABLE} table and its range partition tables {SALES_2020_TABLE}, {SALES_2021_TABLE}, {SALES_2022_TABLE}
       Commit the changes to the database
    """
    try:
        cursor = conn.cursor()
        
        # Create the main table SALES_TABLE
        cursor.execute(f"CREATE TABLE {SALES_TABLE} (id SERIAL , sale_date DATE, amount INTEGER, product VARCHAR(50)) PARTITION BY RANGE (sale_date);")
        
        # Create range partitions for each year
        for year in range(2020, 2023):
            partition_name = f"{SALES_TABLE}_{year}"
            cursor.execute(f"CREATE TABLE {partition_name} PARTITION OF {SALES_TABLE} FOR VALUES FROM ('{year}-01-01') TO ('{year}-12-31');")
        
        conn.commit()
        print("Range partitioning created successfully.")
    except psycopg2.Error as e:
        conn.rollback()
        print("Error creating range partitions:", e)

def insert_range_data(conn):
    """ Generate 50 rows data for {SALES_REGION_TABLE}
        Execute INSERT statement to add data to the {SALES_REGION_TABLE} table.
        Commit the changes to the database"""
    dates = generate_random_dates(50)
    try:
        cursor = conn.cursor()
        for _ in range(50):
            product = PRODUCT_NAMES[_ % len(PRODUCT_NAMES)]
            date = dates[_]
            amount = random.randint(1,100)
            cursor.execute(f"INSERT INTO {SALES_TABLE} (sale_date, amount, product) VALUES ('{date}', '{amount}','{product}');")
        
        conn.commit()
        print("Data inserted into SALES_TABLE successfully.")
    except psycopg2.Error as e:
        conn.rollback()
        print("Error inserting data:", e)



def select_range_data(conn):
    """Select data from {SALES_TABLE}, {SALES_2020_TABLE}, {SALES_2021_TABLE}, {SALES_2022_TABLE} seperately.
           Print each tables' data.
           Commit the changes to the database
        """
    try:
        cursor = conn.cursor()
        print(f"\nData from {SALES_TABLE}:\n")
        cursor.execute(f"SELECT * FROM {SALES_TABLE};")
        data = cursor.fetchall()
        for row in data:
                print(row)
        for year in range(2020,2023):
            print(f"\nData from {SALES_TABLE}_{year}:\n")
            cursor.execute(f"SELECT * FROM {SALES_TABLE}_{year};")
            data = cursor.fetchall()
            for row in data:
                print(row)
        
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print("Error selecting data:", e)
    

def generate_random_dates(n):
    random_dates = []
    
    # Define a date range
    start_date = datetime.datetime(2020, 1, 1)
    end_date = datetime.datetime(2022, 12, 31)
    
    for _ in range(n):
        # Generate a random number of days between start_date and end_date
        random_days = random.randint(0, (end_date - start_date).days)
        
        # Calculate the random date by adding the random days to start_date
        random_date = start_date + datetime.timedelta(days=random_days)
        
        # Format the date as "yyyy-mm-dd" and append it to the list
        formatted_date = random_date.strftime("%Y-%m-%d")
        random_dates.append(formatted_date)
    
    return random_dates

if __name__ == '__main__':

    print("Do you want to create new database? ")
    db = int(input("Press 1 for Yes or 0 for No: "))
    if db:
        create_database(DATABASE_NAME)

    with connect_postgres(dbname=DATABASE_NAME) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        print("Select the operation you want to perform: ")
        opr = 0
        while(opr != 7):
            opr = int(input("1. List Partitioning \n2. Insert List Data\n3. Select List Data\n4. Range Partitioning\n5. Insert Range Data\n6. Select Range Data\n7. Exit\n"))
            if opr == 1:
                list_partitioning(conn)
            elif opr == 2:
                insert_list_data(conn)
            elif opr == 3:
                select_list_data(conn)
            elif opr == 4:
                range_partitioning(conn)
            elif opr == 5:
                insert_range_data(conn)
            elif opr == 6:
                select_range_data(conn)
            else:
                print("Invalid Operation")

        print('Done')




