import pyodbc

# Define SQL Server connection details (connecting to the master database)
server = "DESKTOP-R4QM20O\SQLEXPRESS"
database = "master"
new_database_name = "FundDB"

# Connection string using Windows Authentication
connection_master = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
connection_new_db = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={new_database_name};Trusted_Connection=yes;"


def create_database(new_database_name):
    try:
        # Connect to the master database
        conn = pyodbc.connect(connection_master, autocommit=True)
        cursor = conn.cursor()

        # Check if the database already exists
        check_db_query = (
            f"SELECT * FROM sys.databases WHERE name = '{new_database_name}'"
        )
        cursor.execute(check_db_query)
        db_exists = cursor.fetchone()

        # If the database does not exist, create it
        if not db_exists:
            create_db_query = f"CREATE DATABASE {new_database_name}"
            cursor.execute(create_db_query)
            conn.commit()
            print(f"Database '{new_database_name}' has been created successfully.")
        else:
            print(f"Database '{new_database_name}' already exists.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the connection
        cursor.close()
        conn.close()


def connect():
    """connect to SQL server database"""
    conn = None
    try:
        print("Connecting to SQL Server database....")
        conn = pyodbc.connect(connection_new_db, autocommit=True)
    except Exception as e:
        print(f"An error occurred: {e}")
    print("connection successful.")
    return conn


def create_table():
    try:
        # Connect to the new database
        conn = connect()
        cursor = conn.cursor()

        # Check if the table exists
        check_table_query = """
        IF NOT EXISTS (
            SELECT * FROM sysobjects
            WHERE
                name in ('funds', 'owners', 'top_asset_hodings', 'top_stock_holdings', 'top_industry_holdings')
        )
        BEGIN
            CREATE TABLE funds (
                id INT PRIMARY KEY,
                name NVARCHAR(255),
                short_name VARCHAR(255),
                code VARCHAR(50),
                price FLOAT,
                nav FLOAT,
                last_year_nav FLOAT,
                holding_volume FLOAT,
                first_issue_at DATETIME,
                approve_at DATETIME,
                description NVARCHAR(500),
                create_at DATETIME,
                update_at DATETIME,
                status VARCHAR(50),
                owner_id INT,
                fund_type_name NVARCHAR(255),
                fund_type_code VARCHAR(50),
                nav_to_previous FLOAT,
                nav_to_last_year float,
                nav_to_establish float, 
                nav_to1_months float, 
                nav_to3_months float, 
                nav_to6_months float, 
                nav_to12_months float, 
                nav_to24_months float, 
                nav_to36_months float, 
                nav_to60_months float,
                annualized_return36_months float,
                nav_to_beginning float
                
            );

            CREATE TABLE owners (
                id INT PRIMARY KEY,
                encode_url VARCHAR(255),
                code VARCHAR(20),
                name NVARCHAR(255),
                email VARCHAR(100),
                short_name NVARCHAR(50),
                address1 NVARCHAR(255),
                website NVARCHAR(100),
                phone VARCHAR(15)
            );

            CREATE TABLE top_stock_holdings (
                id INT PRIMARY KEY,
                fund_id INT,
                stock_code VARCHAR(20),
                price FLOAT, 
                change_from_previous FLOAT, 
                change_from_previous_percent FLOAT, 
                industry NVARCHAR(100),
                type VARCHAR(20),
                net_asset_percent FLOAT, 
                update_at DATETIME
            );

            CREATE TABLE top_asset_holdings (
                id INT PRIMARY KEY,
                fund_id INT,
                asset_type VARCHAR(20),
                asset_name NVARCHAR(50),
                asset_percent FLOAT, 
                create_at DATETIME,
                update_at DATETIME
            );

            CREATE TABLE top_industry_holdings (
                id VARCHAR(10) PRIMARY KEY,
                fund_id INT,
                industry NVARCHAR(50),
                asset_percent FLOAT
            );

            CREATE TABLE net_asset_values (
                id INT PRIMARY KEY,
                product_id INT,
                nav_date DATE,
                nav FLOAT,
                created_at DATETIME
            );

        END
        """
        cursor.execute(check_table_query)
        print("created table successfully")

        # Commit the transaction
        conn.commit()

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the connection
        cursor.close()
        conn.close()


def insert_dataframe_to_sql(table_name, dataframe):
    """
    Insert data from a DataFrame into a SQL Server table.

    Parameters:
    - table_name: str, the name of the target SQL table
    - dataframe: pd.DataFrame, the data to be inserted

    Assumes column names in the DataFrame match those in the SQL table.
    """
    try:
        # Connect to SQL Server
        conn = connect()
        cursor = conn.cursor()

        # Get column names from the DataFrame
        columns = ", ".join(dataframe.columns)
        placeholders = ", ".join(
            ["?" for _ in dataframe.columns]
        )  # Placeholder for parameterized queries

        # Prepare the SQL insert statement
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Convert DataFrame rows to a list of tuples
        data = [tuple(row) for row in dataframe.itertuples(index=False)]

        # Execute insert statement for each row
        cursor.executemany(insert_query, data)
        conn.commit()
        print(f"Data inserted into {table_name} successfully.")

        # Close connection
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    create_database(new_database_name)
    create_table()
