import sqlite3
from queue import Queue

# Create the SQLite database and user table
def create_database():
    connection = sqlite3.connect('users.db')
    cursor = connection.cursor()

    # Create the `user` table if it doesn't already exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone TEXT NOT NULL,
            userId TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            date TEXT NOT NULL
        )
    ''')

    # Commit and close the connection
    connection.commit()
    connection.close()
 
def create_credentials_table():
    connection = sqlite3.connect('users.db')
    cursor = connection.cursor()

    # Create the `credentials` table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS credentials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            userId TEXT UNIQUE NOT NULL,
            fyers_userid TEXT NULL,
            fyers_apiKey TEXT NULL,
            fyers_secretKey TEXT NULL,
            fyers_redirectUrl TEXT NULL,
            fyers_grantType TEXT NULL,
            fyers_responseType TEXT NULL,
            
            shoonya_user_id TEXT NULL,
            shoonya_password TEXT NULL,
            shoonya_api_key TEXT NULL,
            shoonya_twofa TEXT NULL,
            shoonya_vendor_code TEXT NULL,
            shoonya_imei TEXT NULL,
            
            angle_user_id TEXT NULL,
            angle_api_key TEXT NULL,
            angle_secretKey TEXT NULL,
            angle_totp TEXT NULL,
            angle_pin TEXT NULL,
            
            FOREIGN KEY (userId) REFERENCES user(userId) ON DELETE CASCADE
        )
    ''')

    # Commit and close the connection
    connection.commit()
    connection.close()
 


class SQLiteConnectionPool:
    def __init__(self, db_path, pool_size=5):
        self.db_path = db_path
        self.pool = Queue(maxsize=pool_size)

        # Pre-fill the pool with connections
        for _ in range(pool_size):
            self.pool.put(self._create_connection())

    def _create_connection(self):
        return sqlite3.connect(self.db_path)

    def get_connection(self):
        return self.pool.get()

    def return_connection(self, connection):
        self.pool.put(connection)

    def close_all_connections(self):
        while not self.pool.empty():
            connection = self.pool.get()
            connection.close()

def insert_user(pool, name, phone, userId, password, date):
    connection = pool.get_connection()
    cursor = connection.cursor()

    try:
        # Check if the userId already exists
        cursor.execute('SELECT * FROM user WHERE userId = ?', (userId,))
        existing_user = cursor.fetchone()

        if existing_user:
            # Update the existing user's information
            cursor.execute('''
                UPDATE user
                SET name = ?, phone = ?, password = ?, date = ?
                WHERE userId = ?
            ''', (name, phone, password, date, userId))
            connection.commit()
            return True, "User updated successfully!"
        else:
            # Insert the new user if the userId doesn't exist
            cursor.execute(''' 
                INSERT INTO user (name, phone, userId, password, date)
                VALUES (?, ?, ?, ?, ?)
            ''', (name, phone, userId, password, date))
            connection.commit()
            return True, "User saved successfully!"

    except sqlite3.IntegrityError as e:
        return False, f"Error inserting or updating user: {e}"
    finally:
        pool.return_connection(connection)
        
def delete_user(pool, userId):

    connection = pool.get_connection()
    cursor = connection.cursor()

    try:
        # Check if the user exists
        cursor.execute('SELECT * FROM user WHERE userId = ?', (userId,))
        existing_user = cursor.fetchone()

        if existing_user:
            # Delete the user
            cursor.execute('DELETE FROM user WHERE userId = ?', (userId,))
            connection.commit()
            return True, "User deleted successfully!"
        else:
            return False, "User not found!"

    except Exception as e:
        return False, f"Error deleting user: {e}"

    finally:
        pool.return_connection(connection)
        

def fetch_all_users(pool):
    connection = pool.get_connection()
    cursor = connection.cursor()

    try:
        cursor.execute('SELECT * FROM user')
        rows = cursor.fetchall()

        column_names = [description[0] for description in cursor.description]
        
        users = []
        for row in rows:
            user_dict = {column_names[i]: row[i] for i in range(len(row))}
            users.append(user_dict)        
        return users
    
    finally:
        pool.return_connection(connection)
        

def fetch_user_by_userId(pool, userId):
 
    connection = pool.get_connection()
    cursor = connection.cursor()
    user_data = None
    try:
        cursor.execute('SELECT * FROM user WHERE userId = ?', (userId,))
        user = cursor.fetchone()
        if user:
            column_names = [desc[0] for desc in cursor.description]
            user_data = dict(zip(column_names, user))
        else:
            print(f"No user found with userId: {userId}")
    finally:
        pool.return_connection(connection)
    return user_data



def insert_or_update_credentials(pool, broker, userId, **kwargs):
 
    connection = pool.get_connection()
    cursor = connection.cursor()
    result_message = ""
    broker_columns = {
        "fyers": { "fyers_apiKey":"apiKey" , "fyers_secretKey":"secretKey" , "fyers_redirectUrl":"redirectUrl" , "fyers_grantType":"grantType" , "fyers_responseType":"responseType"},
        "shoonya": {"shoonya_user_id":"user_id", "shoonya_password":"password", "shoonya_api_key":"api_key" , "shoonya_twofa":"twofa" , "shoonya_vendor_code":"vendor_code", "shoonya_imei":"imei"},
        "angleone": {"angle_user_id":"user_id", "angle_api_key":"api_key", "angle_secretKey":"secretKey", "angle_totp":"totp", "angle_pin":"pin"}
    }
    
    if broker not in broker_columns:
        return False , f"Invalid broker name: {broker}"
    print("kwargs")
    print(type(kwargs))
    print(kwargs)
    update_columns = broker_columns[broker]
    update_values = [kwargs[col] for col in update_columns.values()]
    print("update_values")
    print(update_values)
    
    if not userId:
        return False , "User ID is required for updating credentials."
    
    try:

        cursor.execute('SELECT 1 FROM credentials WHERE userId = ?', (userId,))
        exists = cursor.fetchone()
        
        if exists:
            set_clause = ", ".join([f"{col} = ?" for col in update_columns.keys()])
            query = f"UPDATE credentials SET {set_clause} WHERE userId = ?"
            print(query)
            print(*update_values, userId)
            cursor.execute(query, (*update_values, userId))
            result_message = f"Credentials for {userId} updated successfully!"
        else:
            # Insert new credentials
            columns_str = ", ".join(update_columns.keys())
            placeholders = ", ".join(["?" for _ in update_columns.keys()])
            query = f"INSERT INTO credentials (userId, {columns_str}) VALUES (?, {placeholders})"
            cursor.execute(query, (userId, *update_values))
            result_message = f"Credentials for {userId} inserted successfully!"
        
        # Commit changes
        connection.commit()
    except sqlite3.IntegrityError as e:
        result_message = f"Database integrity error: {e}"
    except Exception as e:
        result_message = f"Error inserting/updating credentials: {e}"
    finally:
        pool.return_connection(connection)
    
    return True , result_message


def fetch_credentials_by_userId(pool, userId, broker): 
    connection = pool.get_connection()
    cursor = connection.cursor()
    broker_columns = {
        "fyers": [ "fyers_apiKey", "fyers_secretKey", "fyers_redirectUrl", "fyers_grantType", "fyers_responseType"],
        "shoonya": ["shoonya_user_id", "shoonya_password", "shoonya_api_key", "shoonya_twofa", "shoonya_vendor_code", "shoonya_imei"],
        "angleone": ["angle_user_id", "angle_api_key", "angle_secretKey", "angle_totp", "angle_pin"]
    }
    try:
        if broker not in broker_columns:
            raise ValueError("Invalid broker specified. Choose from 'fyers', 'shoonya', or 'angle'.")
        columns = ", ".join(broker_columns[broker])
        query = f"SELECT {columns} FROM credentials WHERE userId = ?"
        cursor.execute(query, (userId,))
        credentials = cursor.fetchone()
        if credentials:
            credentials_dict = dict(zip(broker_columns[broker], credentials))
        else:
            credentials_dict = None
    finally:
        pool.return_connection(connection)
    return credentials_dict


def fetch_all_broker_credentials(pool, userId):
    connection = pool.get_connection()
    cursor = connection.cursor()

    try:
        cursor.execute('SELECT * FROM credentials WHERE userId = ?', (userId,))
        credentials = cursor.fetchone()

        if credentials:
            column_names = [desc[0] for desc in cursor.description]
            credentials_dict = dict(zip(column_names, credentials))
        else:
            credentials_dict = None
    finally:
        pool.return_connection(connection)

    return credentials_dict
