import pyodbc

# Define SQL Server connection details (connecting to the master database)
server = "DESKTOP-R4QM20O\SQLEXPRESS"
database = "master"
new_database_name = "FundDB"

# Connection string using Windows Authentication
connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
connection_new_db = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={new_database_name};Trusted_Connection=yes;"


def create_database(new_database_name):
    try:
        # Connect to the master database
        conn = pyodbc.connect(connection_string, autocommit=True)
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
                AND xtype='U'
        )
        BEGIN
            CREATE TABLE funds (
                id INT IDENTITY(1,1) PRIMARY KEY,
                name NVARCHAR(100),
                code VARCHAR(20),
                price DECIMAL(10,2)
            )

            CREATE TABLE owners (
                id INT IDENTITY(1,1) PRIMARY KEY,
                name NVARCHAR(100),
                code VARCHAR(20),
                email VARCHAR(100)
            )
        END
        """
        cursor.execute(check_table_query)
        print("created table successfully (if it did not already exist).")

        # Commit the transaction
        conn.commit()

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the connection
        cursor.close()
        conn.close()


if __name__ == "__main__":
    create_database(new_database_name)
    create_table()
