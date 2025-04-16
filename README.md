# Data Voyager Bridge

A web application built with React (TypeScript, Vite) and a Flask (Python) backend to facilitate data ingestion between a ClickHouse database and local Flat Files (CSV).

![image](https://github.com/user-attachments/assets/17494d0e-c2b7-472d-b2a9-5de515f868bb)

## Features

- Bidirectional data flow: ClickHouse <-> Flat File.
- Web UI (React) for configuration and triggering ingestion.
- Connect to ClickHouse using Host/Port/Database/User/Password or JWT Token (via HTTP `Authorization: Bearer` header).
- Fetches table lists from ClickHouse.
- Fetches column names and types from ClickHouse tables.
- Detects column names and infers types from Flat File headers and data.
- Allows selection of specific columns for ingestion.
- Reports the total number of records processed upon success.
- Basic automatic table creation (Flat File -> ClickHouse) based on inferred schema (requires header row).
- Input validation for file paths (restricted to a `data_files` subdirectory).
- JWT Token validation (HS256 algorithm by default).

## Project Structure

- **`/` (Root):** Contains configuration files (`package.json`, `vite.config.ts`, `tsconfig.json`, etc.), the Flask backend (`app.py`, `requirements.txt`), and the `data_files` directory.
- **`src/`:** Contains the React frontend source code (components, pages, hooks, etc.).
- **`data_files/`:** Directory managed by the Flask backend for storing input and output flat files. **Must be manually created or writable by the application.**
  - `data_files/input/`: Suggested location for source CSV files.
  - `data_files/output/`: Suggested location for exported CSV files.

## Setup

1.  **Prerequisites:**

    - Python 3.8+ recommended.
    - `pip` (Python package installer).
    - Node.js and `npm` (or `yarn`).
    - A running ClickHouse instance.

2.  **Clone the Repository (if applicable):**

    ```bash
    git clone https://github.com/aditramdas/data-voyager-bridge
    cd data-voyager-bridge
    ```

3.  **Backend Setup (Python):**

    - Create and activate a Python virtual environment (recommended):
      ```bash
      python -m venv venv
      # Windows: .\venv\Scripts\activate
      # Linux/macOS: source venv/bin/activate
      ```
    - Install Python dependencies:
      ```bash
      pip install -r requirements.txt
      ```

4.  **Frontend Setup (Node.js):**

    - Install Node.js dependencies:
      ```bash
      npm install
      # or yarn install
      ```

5.  **`data_files` Directory:**

    - Manually create a directory named `data_files` in the project root.
    - Ensure the user running the Flask application (`app.py`) has **read and write permissions** for this directory and its subdirectories.
    - (Optional) Create `input` and `output` subdirectories inside `data_files` for organization.

## Configuration

1.  **Backend (`app.py` / Environment):**

    - **JWT Secret Key (Mandatory for JWT Authentication):** If you intend to use JWT validation (even if not connecting with JWT), the backend needs a secret key. Set the `JWT_SECRET_KEY` environment variable (recommended) or edit the value directly in `app.py`. **See previous README versions or `app.py` comments for details. Keep this secret!**
    - **Allowed File Path Base:** The `ALLOWED_FILE_PATH_BASE` variable in `app.py` defaults to the `data_files` directory. Ensure this matches the directory you created.

2.  **Frontend (`vite.config.ts`):**

    - **API Proxy:** The Vite config includes a proxy to redirect `/api` requests from the frontend dev server (default port 8080) to the Flask backend (default port 5000). Adjust the `target` in `vite.config.ts` if your Flask backend runs on a different port.
    - **Development Port:** The Vite dev server port is set to 8080 in `vite.config.ts`. Change `server.port` if needed.

## Running the Application (Development)

1.  **Start the Backend (Flask):**

    - Ensure your Python virtual environment is active.
    - Set the `JWT_SECRET_KEY` environment variable if applicable.
    - Open a terminal in the project root.
    - Run: `python app.py`
    - The backend typically runs on `http://localhost:5000`.

2.  **Start the Frontend (Vite/React):**

    - Open a **second** terminal in the project root.
    - Run: `npm run dev` (or `yarn dev`)
    - The frontend development server typically runs on `http://localhost:8080` (check terminal output).

3.  **Access the UI:**

    - Open your web browser and navigate to the frontend URL (e.g., `http://localhost:8080`).

## Testing

1.  **ClickHouse Test Setup (One-time):**

    - Use `clickhouse-client` to connect to your instance.
    - Create a test database: `CREATE DATABASE IF NOT EXISTS test_db;`
    - Create a test user: `CREATE USER IF NOT EXISTS test_user IDENTIFIED WITH plaintext_password BY 'test_password';`
    - Grant permissions: `GRANT SELECT, INSERT, CREATE TABLE, DESCRIBE ON test_db.* TO test_user;`
    - Create a sample source table:
      ```sql
      USE test_db;
      CREATE TABLE IF NOT EXISTS test_source_table (`event_id` String, `timestamp` DateTime, `value` Float64) ENGINE = MergeTree() ORDER BY timestamp;
      INSERT INTO test_source_table VALUES ('abc', now(), 123.45), ('def', now()-100, 67.8);
      ```

2.  **Prepare Input File:**

    - Create `data_files/input/test_input.csv` with content like:
      ```csv
      col_a,col_b,col_c
      "hello",10,true
      "world",20,false
      ```

3.  **Run Test Scenario A (CH -> FF):**

    - UI Flow: `ClickHouse -> Flat File`
    - Source Config: Host `localhost`, Port `8123` (or your CH HTTP port), DB `test_db`, User `test_user`, Password `test_password`.
    - Connect, select `test_source_table`, Load Columns.
    - Target Config: Path `output/test_export.csv`, Delimiter `,`, Header checked.
    - Start Ingestion.
    - _Verify:_ Success message with record count (2). Check `data_files/output/test_export.csv` content.

4.  **Run Test Scenario B (FF -> CH):**

    - UI Flow: `Flat File -> ClickHouse`
    - Source Config: Path `input/test_input.csv`, Delimiter `,`, Header checked.
    - Load Columns.
    - Target Config (Connection details entered in Source section): Host `localhost`, Port `8123`, DB `test_db`, User `test_user`, Password `test_password`.
    - Target Config (Table details): Target Table `imported_csv_data`, Create Table checked.
    - Start Ingestion.
    - _Verify:_ Success message with record count (2). Check ClickHouse for the `imported_csv_data` table and its contents (`SELECT * FROM test_db.imported_csv_data;`).

## Note on Production

- The Flask development server (`python app.py`) is not suitable for production.
- The Vite development server (`npm run dev`) is not suitable for production.
- For deployment:
  1. Build the React frontend: `npm run build`. This creates static files (usually in a `dist` directory).
  2. Configure a production-grade WSGI server (like Gunicorn or uWSGI) to run the Flask app (`app.py`).
  3. Configure a web server (like Nginx) to:
     - Serve the static frontend files from the `dist` directory.
     - Reverse proxy API requests (e.g., `/api/*`) to the WSGI server running the Flask app.
