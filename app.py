from flask import Flask, render_template, request, jsonify
import clickhouse_connect
import pandas as pd
import io
import os # Added for basic path validation

app = Flask(__name__)

# Mock function for JWT validation (replace with actual validation)
# In a real app, you would verify the token signature and claims
def validate_jwt(token):
    print(f"Validating token: {token[:10]}...") # Avoid logging the full token
    # This is a placeholder - real validation needed!
    return token is not None and len(token) > 20 # Basic check


def get_clickhouse_client(config):
    """Establishes a ClickHouse connection using provided config."""
    try:
        # Input validation
        required_keys = ['host', 'port', 'database', 'user']
        if not all(key in config and config[key] for key in required_keys):
             raise ValueError("Missing required ClickHouse connection parameters (host, port, database, user).")

        if not config.get('password') and not config.get('jwt'):
             raise ValueError("Either password or JWT token must be provided for ClickHouse connection.")

        # Prepare connection parameters
        connect_args = {
            'host': config['host'],
            'port': int(config['port']), # Ensure port is int
            'database': config['database'],
            'user': config['user'],
            'secure': config.get('secure', False), # Default to False if not provided
            # Add settings useful for ingestion
            'settings': {
                'insert_deduplicate': 0, # Common settings, adjust as needed
                'insert_distributed_sync': 1
            }
        }

        jwt_token = config.get('jwt')
        password = config.get('password')

        if jwt_token:
            # Basic check if JWT seems valid (replace with real validation)
            if not validate_jwt(jwt_token):
                 raise ValueError("Invalid JWT token provided.")
            # How to pass JWT depends on the connection method (HTTP vs Native)
            # For HTTP/S (ports 8123/8443 typically):
            if connect_args['port'] in [8123, 8443]:
                 print("Using JWT via HTTP Headers")
                 connect_args['http_headers'] = {'Authorization': f'Bearer {jwt_token}'}
                 # Ensure password is not sent if JWT is used for HTTP
                 connect_args.pop('password', None)
            else:
                # For Native protocol, JWT might be passed differently or not directly supported
                # by clickhouse-connect in this way. Check library docs for specifics.
                # Often, native protocol uses user/password. If JWT is the *only* mechanism,
                # a proxy or specific ClickHouse setup might be needed.
                # For now, we'll assume if JWT is provided for non-HTTP ports, it might fail
                # or require password as fallback/primary if JWT isn't natively supported here.
                print(f"Warning: JWT provided for non-standard HTTP port ({connect_args['port']}). Authentication might rely on password if provided, or fail.")
                if password:
                    connect_args['password'] = password
                else:
                    # If only JWT is given for native, raise error as it's likely not supported directly
                    raise ValueError(f"JWT authentication might not be supported for native protocol on port {connect_args['port']} without specific server config. Try password or HTTP port.")
        elif password:
            connect_args['password'] = password
        else:
             # This case is already checked above, but as a safeguard
             raise ValueError("No password or JWT token provided.")

        print(f"Attempting connection to {connect_args['host']}:{connect_args['port']}...")
        # Remove sensitive info before potentially logging args
        log_args = connect_args.copy()
        log_args.pop('password', None)
        log_args.pop('jwt', None)
        if 'http_headers' in log_args: log_args['http_headers'] = {'Authorization': 'Bearer [REDACTED]'}
        print(f"Connection args (sensitive info redacted): {log_args}")

        client = clickhouse_connect.get_client(**connect_args)
        client.ping() # Verify connection
        print("ClickHouse connection successful.")
        return client
    except ValueError as ve:
         print(f"Configuration Error: {ve}")
         raise # Re-raise validation errors
    except clickhouse_connect.driver.exceptions.Error as ch_err:
        print(f"ClickHouse Connection Error: {ch_err}")
        # Provide more specific feedback if possible
        err_str = str(ch_err).lower()
        if 'authentication failed' in err_str or 'auth failed' in err_str:
            raise ConnectionError(f"ClickHouse authentication failed for user '{config.get('user', 'N/A')}'. Check credentials/JWT.") from ch_err
        elif 'connection refused' in err_str or 'timed out' in err_str:
             raise ConnectionError(f"Could not connect to ClickHouse at {config.get('host', 'N/A')}:{config.get('port', 'N/A')}. Check host/port and network connectivity.") from ch_err
        else:
            raise ConnectionError(f"ClickHouse error: {ch_err}") from ch_err
    except Exception as e:
        print(f"Unexpected error during ClickHouse connection: {e}")
        raise ConnectionError(f"An unexpected error occurred: {e}") from e

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get_tables', methods=['POST'])
def get_tables():
    config = request.json
    try:
        client = get_clickhouse_client(config)
        # Use system.tables to get tables from the specified database
        # We query `system.tables` and filter by the database from the config
        # Adding limit to avoid fetching too many tables in large environments
        query = f"SELECT name FROM system.tables WHERE database = %(database)s ORDER BY name LIMIT 1000"
        tables_result = client.query(query, parameters={'database': config['database']})
        tables = [row[0] for row in tables_result.result_rows]
        client.close()
        return jsonify({'tables': tables})
    except (ValueError, ConnectionError, clickhouse_connect.driver.exceptions.Error) as e:
        return jsonify({'error': str(e)}), 400 # Bad request for config/connection issues
    except Exception as e:
        print(f"Error in /get_tables: {e}")
        return jsonify({'error': 'An unexpected server error occurred while fetching tables.'}), 500

@app.route('/get_columns', methods=['POST'])
def get_columns():
    config = request.json
    source_type = config.get('source_type')

    try:
        if source_type == 'clickhouse':
            table_name = config.get('table')
            if not table_name:
                raise ValueError("Missing 'table' parameter for ClickHouse source.")

            client = get_clickhouse_client(config) # Reuses connection logic
            # Query system.columns for the specific table and database
            query = f"SELECT name FROM system.columns WHERE database = %(database)s AND table = %(table)s ORDER BY position"
            columns_result = client.query(query, parameters={'database': config['database'], 'table': table_name})
            columns = [row[0] for row in columns_result.result_rows]
            client.close()
            if not columns:
                 raise ValueError(f"Table '{table_name}' not found or has no columns in database '{config['database']}'.")
            return jsonify({'columns': columns})

        elif source_type == 'flatfile':
            file_path = config.get('file_path')
            delimiter = config.get('delimiter')
            has_header = config.get('has_header', True) # Default to True if not provided

            if not file_path or not delimiter:
                raise ValueError("Missing 'file_path' or 'delimiter' for Flat File source.")

            # SECURITY WARNING: Basic path validation. In production, sanitize and restrict paths severely.
            if not os.path.exists(file_path):
                 raise FileNotFoundError(f"File not found at path: {file_path}")
            if not os.path.isfile(file_path):
                raise ValueError(f"Path is not a file: {file_path}")

            if not has_header:
                # Cannot get columns if there's no header
                return jsonify({'columns': []}) # Return empty list, UI handles message

            try:
                # Read only the header row using pandas
                df_header = pd.read_csv(file_path, sep=delimiter, nrows=0) # nrows=0 reads just the header
                columns = df_header.columns.tolist()
                return jsonify({'columns': columns})
            except pd.errors.EmptyDataError:
                 raise ValueError(f"File '{os.path.basename(file_path)}' appears to be empty.") from None
            except Exception as pd_err: # Catch potential pandas parsing errors
                raise ValueError(f"Error reading header from file '{os.path.basename(file_path)}': {pd_err}") from pd_err

        else:
            raise ValueError(f"Invalid source_type specified: {source_type}")

    except (ValueError, ConnectionError, FileNotFoundError, clickhouse_connect.driver.exceptions.Error) as e:
        print(f"Error getting columns: {e}")
        return jsonify({'error': str(e)}), 400 # Config, connection, or file error
    except Exception as e:
        # Catch-all for unexpected server errors
        print(f"Unexpected error in /get_columns: {e}")
        # Avoid leaking detailed internal errors to the client in production
        return jsonify({'error': 'An unexpected server error occurred while fetching columns.'}), 500

@app.route('/ingest', methods=['POST'])
def ingest():
     # To be implemented
     return jsonify({'error': 'Not implemented yet'}), 501


if __name__ == '__main__':
    # Make accessible on local network if needed, use 0.0.0.0
    # Use a specific port if default 5000 is taken
    app.run(host='0.0.0.0', port=5000, debug=True) 