import os
import sys
import argparse
import time
import pandas as pd
import io  # For BytesIO and StringIO
import zipfile  # For handling ZIP files

# Optional import for progress bars
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    print("Note: Install 'tqdm' for progress bars.")

# MongoDB imports (install with: pip install pymongo)
try:
    from pymongo import MongoClient
    HAS_MONGO = True
except ImportError:
    HAS_MONGO = False
    print("Error: 'pymongo' is required for MongoDB integration. Install with 'pip install pymongo'.")
    sys.exit(1)

# SFTP imports (install with: pip install paramiko)
try:
    import paramiko
    HAS_PARAMIKO = True
except ImportError:
    HAS_PARAMIKO = False
    print("Error: 'paramiko' is required for SFTP. Install with 'pip install paramiko'.")
    sys.exit(1)

# Configuration
# SFTP Configuration (use env vars in production for security)
SFTP_HOST = 'your_sftp_host.com'  # Replace with your SFTP server
SFTP_USER = 'your_sftp_username'  # Replace with SFTP username
SFTP_PASS = 'your_sftp_password'  # Replace with SFTP password (or use key_file for SSH key)
SFTP_PORT = 22  # Default SFTP port
SFTP_SOURCE_DIR = '/remote/path/to/source/'  # Remote directory on SFTP to read files from (ensure it exists)
SFTP_OUTPUT_DIR = '/remote/path/to/output/'  # Remote directory on SFTP to store updated files (ensure it exists; can be same as source)

# ZIP Configuration
ZIP_PASSWORD = 'your_zip_password'  # Replace with the password for ZIP files (use env vars in production for security)

# MongoDB Configuration (use env vars in production for security)
MONGO_URI = 'mongodb://localhost:27017/'  # e.g., 'mongodb://user:pass@host:port/'
DB_NAME = 'your_database'                 # Replace with your DB name
COLLECTION_NAME = 'users'                 # Replace with your collection name (assumes fields: userid, dob, email)

def get_mongo_client():
    """Connect to MongoDB."""
    try:
        client = MongoClient(MONGO_URI)
        # Test connection
        client.admin.command('ismaster')
        print(f"Connected to MongoDB at {MONGO_URI}")
        return client
    except Exception as e:
        print(f"MongoDB connection error: {e}")
        sys.exit(1)

def get_user_data_from_mongo(userids, client=None):
    """
    Query MongoDB for user data based on list of userids.
    Returns dict: {userid: {'dob': ..., 'email': ...}}
    Assumes 'userid' is a field in the collection (or use '_id' if it's the primary key).
    """
    if not userids:
        return {}

    if client is None:
        mongo_client = get_mongo_client()
        db = mongo_client[DB_NAME]
        close_client = True
    else:
        db = client[DB_NAME]
        close_client = False

    collection = db[COLLECTION_NAME]

    # Batch query for efficiency (find all matching userids)
    query_results = collection.find({'userid': {'$in': userids}}, {'dob': 1, 'email': 1, '_id': 0})
    
    user_data = {}
    for doc in query_results:
        userid = doc.get('userid', '').strip()
        user_data[userid] = {
            'dob': doc.get('dob', '').strip() if doc.get('dob') else '',
            'email': doc.get('email', '').strip() if doc.get('email') else ''
        }
    
    # Fill missing userids with defaults
    for userid in userids:
        userid_str = str(userid).strip()
        if userid_str not in user_data:
            user_data[userid_str] = {'dob': '', 'email': ''}
    
    if close_client:
        mongo_client.close()
    
    return user_data

def get_sftp_client(max_retries=3):
    """Connect to SFTP server with retries."""
    for attempt in range(max_retries):
        try:
            transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
            transport.connect(username=SFTP_USER, password=SFTP_PASS)  # For key: password=None, pkey=paramiko.RSAKey.from_private_key_file('key_file')
            sftp = paramiko.SFTPClient.from_transport(transport)
            print(f"Connected to SFTP server: {SFTP_HOST}:{SFTP_PORT}")
            return sftp, transport
        except Exception as e:
            print(f"SFTP connection attempt {attempt+1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                print("Max retries exceeded. Exiting.")
                sys.exit(1)
    return None, None

def list_files_from_sftp(sftp, remote_dir, groupid):
    """List ZIP files in SFTP directory that match groupid."""
    try:
        sftp.chdir(remote_dir)
        all_files = sftp.listdir()
        matching_files = [f for f in all_files if groupid.lower() in f.lower() and f.endswith('.zip')]
        print(f"Listed files in {remote_dir}: Found {len(matching_files)} matching ZIP files")
        return matching_files
    except Exception as e:
        print(f"Error listing files in {remote_dir}: {e}")
        return []

def read_file_from_sftp(sftp, filename, max_retries=3):
    """Read SFTP file content into memory (BytesIO) without local download."""
    for attempt in range(max_retries):
        try:
            with sftp.open(filename, 'rb') as remote_file:
                content = remote_file.read()
            content_buffer = io.BytesIO(content)
            print(f"Read from SFTP: {filename} (size: {len(content)} bytes)")
            return content_buffer
        except Exception as e:
            print(f"Read attempt {attempt+1} for {filename} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"Failed to read {filename} after {max_retries} attempts.")
                return None

def extract_csv_from_zip(zip_buffer, password, filename):
    """Extract the CSV file from the password-protected ZIP in memory."""
    zip_buffer.seek(0)
    try:
        with zipfile.ZipFile(zip_buffer) as zf:
            csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
            if not csv_files:
                print(f"No CSV file found in ZIP: {filename}")
                return None, None
            if len(csv_files) > 1:
                print(f"Multiple CSVs found in {filename}; using the first: {csv_files[0]}")
            csv_name = csv_files[0]
            try:
                with zf.open(csv_name, pwd=password.encode('utf-8')) as csv_file:
                    csv_content = csv_file.read()
                csv_buffer = io.BytesIO(csv_content)
                print(f"Extracted CSV '{csv_name}' from ZIP: {filename}")
                return csv_buffer, csv_name
            except RuntimeError as e:
                if "Bad password" in str(e) or "password" in str(e).lower():
                    print(f"Incorrect password for ZIP file: {filename}")
                else:
                    print(f"Error opening CSV '{csv_name}' in {filename}: {e}")
                return None, None
    except Exception as e:
        print(f"Error processing ZIP file {filename}: {e}")
        return None, None

def upload_to_sftp(sftp, filename, content_buffer, output_dir):
    """Upload content buffer to SFTP output directory with new filename."""
    new_filename = f"updated_{filename}"
    try:
        # Change to output directory
        sftp.chdir(output_dir)
        with sftp.open(new_filename, 'wb') as remote_file:
            remote_file.write(content_buffer.getvalue())
        print(f"Uploaded to SFTP: {new_filename} in {output_dir}")
        return True
    except Exception as e:
        print(f"SFTP upload failed for {new_filename} in {output_dir}: {e}")
        return False

def process_csv_from_memory(content_buffer, filename, groupid, mongo_client=None):
    """
    Process CSV from memory buffer: Load with Pandas, append DOB/email from MongoDB.
    Returns updated CSV as BytesIO buffer.
    """
    only_dob = '_dob_only_' in filename.lower()  # Assumption: filename pattern for DOB-only files

    try:
        # Reset buffer position
        content_buffer.seek(0)
        df = pd.read_csv(content_buffer)
        if 'userid' not in df.columns or len(df) == 0:
            print(f"Invalid CSV structure in {filename}. Skipping.")
            return None

        # Get unique userids for batch query
        unique_userids = df['userid'].astype(str).str.strip().unique().tolist()
        print(f"Querying MongoDB for {len(unique_userids)} unique userids from {filename}...")

        # Query MongoDB (pass client for reuse)
        user_data = get_user_data_from_mongo(unique_userids, mongo_client)

        # Map DOB and email (vectorized for speed)
        df['dob'] = df['userid'].astype(str).str.strip().map(lambda x: user_data.get(x, {}).get('dob', ''))
        if not only_dob:
            df['email'] = df['userid'].astype(str).str.strip().map(lambda x: user_data.get(x, {}).get('email', ''))
        else:
            df['email'] = ''  # Or drop if not needed

        # Reorder columns (assume userid and username are present; adjust if needed)
        cols = [col for col in df.columns if col not in ['dob', 'email']] + ['dob']
        if not only_dob:
            cols.append('email')
        df = df[cols]

        # Convert back to CSV in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        updated_content = io.BytesIO(csv_buffer.getvalue().encode('utf-8'))
        updated_content.seek(0)
        print(f"Processed {filename} in memory (DOB-only: {only_dob}, rows: {len(df)})")
        return updated_content
    except Exception as e:
        print(f"Error processing {filename}: {e}")
        return None

def ensure_sftp_dir(sftp, remote_dir):
    """Ensure the remote directory exists (create if not)."""
    try:
        sftp.chdir(remote_dir)
    except IOError:
        # Directory doesn't exist, create it
        try:
            sftp.mkdir(remote_dir)
            print(f"Created SFTP directory: {remote_dir}")
        except Exception as e:
            print(f"Failed to create {remote_dir}: {e}")
            # Try to create parent dirs if needed (simple recursive mkdir)
            parts = remote_dir.strip('/').split('/')
            current_path = ''
            for part in parts:
                current_path += '/' + part
                try:
                    sftp.chdir(current_path)
                except IOError:
                    try:
                        sftp.mkdir(current_path)
                    except Exception:
                        pass  # Ignore if already exists
            print(f"Ensured SFTP directory: {remote_dir}")

def main(groupid, parallel_downloads=False, verbose=False):
    """Main function: Read ZIP from SFTP in memory, extract/process CSV, upload updated CSV to SFTP output dir."""
    # Note: Parallelism disabled for simplicity (shared SFTP connection issues); set to sequential
    if parallel_downloads:
        print("Warning: Parallel mode disabled for in-memory SFTP reads (sequential only).")

    mongo_client = get_mongo_client()
    sftp, transport = get_sftp_client()

    if sftp is None:
        print("Failed to connect to SFTP. Exiting.")
        return

    try:
        # Ensure directories exist
        ensure_sftp_dir(sftp, SFTP_SOURCE_DIR)
        ensure_sftp_dir(sftp, SFTP_OUTPUT_DIR)

        # List matching files in source directory
        matching_files = list_files_from_sftp(sftp, SFTP_SOURCE_DIR, groupid)

        if not matching_files:
            print(f"No ZIP files found containing groupid: {groupid} in {SFTP_SOURCE_DIR}")
            return

        print(f"Found {len(matching_files)} matching files: {matching_files[:5]}{'...' if len(matching_files) > 5 else ''}")

        uploaded_count = 0
        # Process files sequentially (read-extract-process-upload)
        for filename in (tqdm(matching_files) if HAS_TQDM else matching_files):
            # Read ZIP from SFTP to memory
            zip_buffer = read_file_from_sftp(sftp, filename)
            if zip_buffer is None:
                continue

            # Extract CSV from ZIP
            csv_buffer, csv_name = extract_csv_from_zip(zip_buffer, ZIP_PASSWORD, filename)
            if csv_buffer is None:
                continue

            # Process CSV in memory (pass original ZIP filename for pattern checks like DOB-only)
            updated_content = process_csv_from_memory(csv_buffer, filename, groupid, mongo_client)
            if updated_content is None:
                continue

            # Prepare filename for upload (derive CSV name from ZIP basename)
            base_name = os.path.splitext(filename)[0]
            csv_filename = f"{base_name}.csv"

            # Upload updated CSV to SFTP output dir
            if upload_to_sftp(sftp, csv_filename, updated_content, SFTP_OUTPUT_DIR):
                uploaded_count += 1
                if verbose:
                    print(f"Uploaded: updated_{csv_filename} to SFTP {SFTP_OUTPUT_DIR}")

        print(f"Processing complete. {uploaded_count} files uploaded to SFTP {SFTP_OUTPUT_DIR}.")

    finally:
        mongo_client.close()
        if sftp:
            sftp.close()
        if transport:
            transport.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="In-memory SFTP ZIP processor with CSV extraction, MongoDB integration (Pandas required).")
    parser.add_argument("groupid", help="Group ID to filter filenames (e.g., '123')")
    parser.add_argument("--parallel", action="store_true", help="Enable parallel (disabled for in-memory mode)")
    parser.add_argument("--verbose", action="store_true", help="Print detailed upload paths")
    args = parser.parse_args()
    main(args.groupid, parallel_downloads=args.parallel, verbose=args.verbose)
