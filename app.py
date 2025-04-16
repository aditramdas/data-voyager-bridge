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

# --- Helper for FF -> CH Schema Generation ---

def pandas_to_clickhouse_type(dtype):
    """Maps pandas dtype to a suitable ClickHouse data type string."""
    # This is a basic mapping, may need significant enhancement for production
    # Consider Nullable(), LowCardinality(), specific Int/UInt sizes, FixedString, etc.
    # Also, ClickHouse types like DateTime64, Decimal need specific precision/scale.
    if pd.api.types.is_integer_dtype(dtype):
        # Defaulting to Int64, might need smaller types based on data range
        return 'Int64'
    elif pd.api.types.is_float_dtype(dtype):
        return 'Float64'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'UInt8' # ClickHouse often uses 0/1 for booleans
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        # Basic DateTime, might need DateTime64 with precision
        return 'DateTime'
    elif pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
        # Default to String for objects/strings
        return 'String'
    else:
        print(f"Warning: Unhandled pandas dtype: {dtype}. Defaulting to String.")
        return 'String' # Fallback

# --- Main Ingestion Endpoint ---

@app.route('/ingest', methods=['POST'])
def ingest():
    config = request.json
    flow_type = config.get('flow_type')
    client = None # Initialize client variable
    processed_count = 0

    print(f"Starting ingestion flow: {flow_type}")
    print(f"Received config (sensitive data redacted): {{k: v for k, v in config.items() if k not in ['password', 'jwt']}}")

    try:
        if flow_type == 'ch_to_ff':
            # --- ClickHouse to Flat File ---
            print("Executing ClickHouse -> Flat File flow")
            selected_columns = config.get('columns', [])
            source_table = config.get('source_table')
            target_file = config.get('target_file')
            target_delimiter = config.get('target_delimiter')
            include_header = config.get('include_header', True)

            # Basic validation
            if not selected_columns:
                raise ValueError("No columns selected for ingestion.")
            if not source_table:
                raise ValueError("Source ClickHouse table not specified.")
            if not target_file or not target_delimiter:
                 raise ValueError("Target file path or delimiter not specified.")

            # SECURITY WARNING: Validate target_file path in production!
            # Ensure it's within an allowed directory, sanitize name, etc.
            target_dir = os.path.dirname(target_file)
            if target_dir and not os.path.exists(target_dir):
                print(f"Target directory {target_dir} does not exist, attempting to create.")
                try:
                    os.makedirs(target_dir, exist_ok=True)
                except OSError as e:
                     raise ValueError(f"Failed to create target directory '{target_dir}': {e}")

            client = get_clickhouse_client(config)
            # Quote column names to handle special characters/keywords
            quoted_columns = [f'"{col}"' for col in selected_columns]
            select_query = f'SELECT {", ".join(quoted_columns)} FROM "{config["database"]}"."{source_table}"'

            print(f"Executing query: {select_query}")
            # Use query_df for simplicity with Pandas. For very large tables, consider client.query_arrow() or client.iterate()
            df = client.query_df(select_query)
            processed_count = len(df)
            print(f"Fetched {processed_count} records from ClickHouse.")

            print(f"Writing data to {target_file} with delimiter '{target_delimiter}'")
            df.to_csv(target_file, sep=target_delimiter, index=False, header=include_header, quoting=1) # quoting=1 (QUOTE_MINIMAL)

            print("ClickHouse -> Flat File ingestion successful.")
            return jsonify({'success': True, 'records_processed': processed_count})

        elif flow_type == 'ff_to_ch':
            # --- Flat File to ClickHouse ---
            print("Executing Flat File -> ClickHouse flow")
            source_file = config.get('source_file')
            source_delimiter = config.get('source_delimiter')
            source_has_header = config.get('source_has_header', True)
            selected_columns = config.get('columns', []) # Columns selected in UI (usually from header)
            target_table = config.get('target_table')
            target_create = config.get('target_create', False)

            # Basic validation
            if not source_file or not source_delimiter:
                raise ValueError("Source file path or delimiter not specified.")
            if not target_table:
                raise ValueError("Target ClickHouse table not specified.")
            if not os.path.exists(source_file):
                 raise FileNotFoundError(f"Source file not found: {source_file}")
            if not os.path.isfile(source_file):
                raise ValueError(f"Source path is not a file: {source_file}")

            print(f"Reading data from {source_file}")
            # Read the CSV
            try:
                # Use low_memory=False to prevent mixed type inference issues on large files
                df = pd.read_csv(
                    source_file,
                    sep=source_delimiter,
                    header=0 if source_has_header else None,
                    low_memory=False
                )

                if source_has_header:
                     # If header exists and columns were selected, use only selected columns
                     if selected_columns:
                         # Ensure selected columns actually exist in the DataFrame
                         missing_cols = [col for col in selected_columns if col not in df.columns]
                         if missing_cols:
                              raise ValueError(f"Selected columns not found in file header: {missing_cols}")
                         df = df[selected_columns]
                     # Else (if header exists but no columns selected), use all columns from header
                else:
                    # No header: DataFrame gets default integer column names (0, 1, 2...)
                    # Ingestion will likely fail unless target table schema matches exactly by position
                    # or manual column mapping is implemented (future enhancement).
                    print("Warning: Reading file without header. Column matching relies on position.")
                    if selected_columns:
                        print("Warning: Columns were selected in UI, but file has no header. Selection ignored.")

                processed_count = len(df)
                if processed_count == 0:
                     print("Source file is empty. Nothing to ingest.")
                     return jsonify({'success': True, 'records_processed': 0})

                print(f"Read {processed_count} records from file.")

            except pd.errors.EmptyDataError:
                 print("Source file is empty. Nothing to ingest.")
                 return jsonify({'success': True, 'records_processed': 0})
            except Exception as pd_err:
                raise ValueError(f"Error reading CSV file '{os.path.basename(source_file)}': {pd_err}") from pd_err


            # --- Connect to ClickHouse Target ---
            client = get_clickhouse_client(config)
            db_name = config['database']
            full_table_name = f'"{db_name}"."{target_table}"' # Quote names

            # --- Handle Table Creation ---
            table_exists = False
            try:
                # Check if table exists (more reliable than SHOW TABLES in some contexts)
                client.command(f'CHECK TABLE {full_table_name}')
                table_exists = True
                print(f"Target table {full_table_name} exists.")
            except clickhouse_connect.driver.exceptions.ClickHouseError as e:
                # Specific error code for unknown table varies, check common ones
                # https://clickhouse.com/docs/en/interfaces/cli/error-codes
                if 'UNKNOWN_TABLE' in str(e) or 'Table doesn\'t exist' in str(e) or 'code: 60' in str(e):
                    print(f"Target table {full_table_name} does not exist.")
                    table_exists = False
                else:
                    raise # Re-raise other ClickHouse errors during check

            if not table_exists:
                if target_create:
                    if not source_has_header:
                        raise ValueError("Cannot create table automatically: Source file has no header to infer schema.")
                    print(f"Attempting to create target table {full_table_name}...")
                    # Generate CREATE TABLE statement (basic)
                    cols_with_types = []
                    for col_name, dtype in df.dtypes.items():
                         # Ensure column names are safe for SQL (basic quoting)
                         safe_col_name = f'"{str(col_name)}"' # Quote all column names
                         ch_type = pandas_to_clickhouse_type(dtype)
                         cols_with_types.append(f'{safe_col_name} {ch_type}')

                    # Simple MergeTree engine, adjust as needed (e.g., ORDER BY key)
                    # Inferring a good ORDER BY key automatically is tricky. Using tuple() for no explicit key.
                    create_statement = f"CREATE TABLE {full_table_name} (\n    {',\n    '.join(cols_with_types)}\n) ENGINE = MergeTree() ORDER BY tuple()"

                    print(f"Executing CREATE TABLE:
{create_statement}")
                    try:
                        client.command(create_statement)
                        print(f"Table {full_table_name} created successfully.")
                    except Exception as create_err:
                        raise RuntimeError(f"Failed to auto-create table {full_table_name}: {create_err}") from create_err
                else:
                     raise ValueError(f"Target table {full_table_name} does not exist and 'Create Table' option was not checked.")
            elif target_create:
                 print("Target table exists, 'Create Table' option ignored.")

            # --- Insert Data --- #
            print(f"Inserting {processed_count} records into {full_table_name}...")
            # Ensure DataFrame column names match expected format if needed (usually handled by insert_df)
            # Consider batching inserts for very large dataframes (e.g., loop over df chunks)
            client.insert_df(full_table_name, df)
            print("Flat File -> ClickHouse ingestion successful.")
            return jsonify({'success': True, 'records_processed': processed_count})

        else:
            raise ValueError(f"Invalid flow_type specified: {flow_type}")

    except (ValueError, ConnectionError, FileNotFoundError, clickhouse_connect.driver.exceptions.Error, RuntimeError) as e:
        # Handle known operational errors
        error_message = str(e)
        print(f"Ingestion Error: {error_message}")
        # Return specific errors as 400 Bad Request
        return jsonify({'success': False, 'error': error_message, 'records_processed': processed_count}), 400
    except Exception as e:
        # Handle unexpected server errors
        error_message = f"An unexpected server error occurred during ingestion: {e}"
        print(f"Unexpected Ingestion Error: {e}", exc_info=True) # Log traceback for debugging
        # Return generic error as 500 Internal Server Error
        return jsonify({'success': False, 'error': "An unexpected server error occurred.", 'records_processed': processed_count}), 500
    finally:
        # Ensure client connection is closed
        if client:
            try:
                client.close()
                print("ClickHouse connection closed.")
            except Exception as close_err:
                 print(f"Error closing ClickHouse connection: {close_err}")


if __name__ == '__main__':
    # Make accessible on local network if needed, use 0.0.0.0
    # Use a specific port if default 5000 is taken
    app.run(host='0.0.0.0', port=5000, debug=True) 