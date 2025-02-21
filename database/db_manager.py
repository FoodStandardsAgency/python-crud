# database/db_manager.py
import os
import json
import duckdb
from datetime import datetime

class DatabaseManager:
    def __init__(self, db_path='data/food_data.db'):
        """Initialize database connection and ensure directory exists."""
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Check if file exists but is invalid
        if os.path.exists(db_path) and os.path.getsize(db_path) > 0:
            try:
                # Test if it's a valid database
                test_conn = duckdb.connect(db_path, read_only=True)
                test_conn.close()
            except:
                # If not valid, remove it
                os.remove(db_path)
                print(f"Removed invalid database file: {db_path}")
        
        # Create fresh connection
        self.conn = duckdb.connect(db_path)
        self._initialize_db()
    def _initialize_db(self):
        """Initialize database schema if tables don't exist."""
        # Create food consumption table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS food_consumption (
                id INTEGER PRIMARY KEY,
                commodity TEXT NOT NULL,
                age_group TEXT NOT NULL,
                mean_consumption_chronic FLOAT,
                mean_consumption_acute FLOAT,
                percentile_97_chronic FLOAT,
                percentile_97_acute FLOAT,
                notes TEXT,
                version INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create deleted records table for "undo" functionality
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS deleted_records (
                original_id INTEGER,
                table_name TEXT,
                record_data JSON,
                deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    def get_all_records(self, table_name):
        """Retrieve all records from a specific table."""
        return self.conn.execute(f"SELECT * FROM {table_name}").fetchdf()
    
    def get_record_by_id(self, table_name, record_id):
        """Retrieve a specific record by ID."""
        return self.conn.execute(
            f"SELECT * FROM {table_name} WHERE id = ?", 
            [record_id]
        ).fetchone()
    
    def add_record(self, table_name, record_data):
        """Add a new record to the specified table."""
        if table_name == 'food_consumption':
            columns = ', '.join(record_data.keys())
            placeholders = ', '.join(['?'] * len(record_data))
            values = list(record_data.values())
            
            self.conn.execute(
                f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})",
                values
            )
            return self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    
    def update_record(self, table_name, record_id, record_data):
        """Update an existing record and increment version."""
        if table_name == 'food_consumption':
            # Get current version
            current_version = self.conn.execute(
                f"SELECT version FROM {table_name} WHERE id = ?", 
                [record_id]
            ).fetchone()[0]
            
            # Update fields and version
            set_clause = ', '.join([f"{key} = ?" for key in record_data.keys()])
            set_clause += ", version = ?, updated_at = CURRENT_TIMESTAMP"
            values = list(record_data.values())
            values.append(current_version + 1)
            values.append(record_id)
            
            self.conn.execute(
                f"UPDATE {table_name} SET {set_clause} WHERE id = ?",
                values
            )
    
    def delete_record(self, table_name, record_id):
        """Soft delete by moving record to deleted_records table."""
        # Get record data
        record = self.get_record_by_id(table_name, record_id)
        if record:
            # Convert record to JSON for storage
            record_json = json.dumps(dict(zip(record.keys(), record)))
            
            # Store in deleted_records
            self.conn.execute(
                "INSERT INTO deleted_records (original_id, table_name, record_data) VALUES (?, ?, ?)",
                [record_id, table_name, record_json]
            )
            
            # Delete from original table
            self.conn.execute(f"DELETE FROM {table_name} WHERE id = ?", [record_id])
            return True
        return False
    
    def undo_delete(self, original_id, table_name):
        """Restore a deleted record."""
        # Find the deleted record
        deleted_record = self.conn.execute(
            "SELECT record_data FROM deleted_records WHERE original_id = ? AND table_name = ? ORDER BY deleted_at DESC LIMIT 1",
            [original_id, table_name]
        ).fetchone()
        
        if deleted_record:
            record_data = json.loads(deleted_record[0])
            
            # Reinsert the record with its original ID
            columns = ', '.join(record_data.keys())
            placeholders = ', '.join(['?'] * len(record_data))
            values = list(record_data.values())
            
            self.conn.execute(
                f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})",
                values
            )
            
            # Remove from deleted_records
            self.conn.execute(
                "DELETE FROM deleted_records WHERE original_id = ? AND table_name = ? ORDER BY deleted_at DESC LIMIT 1",
                [original_id, table_name]
            )
            return True
        return False
        
    def import_csv_data(self, table_name, df):
        """Import data from a pandas DataFrame to the specified table."""
        # Validate column names against table schema
        if table_name == 'food_consumption':
            required_columns = ['commodity', 'age_group']
            for col in required_columns:
                if col not in df.columns:
                    raise ValueError(f"Required column '{col}' missing from CSV")
                
            # Insert data
            for _, row in df.iterrows():
                record_data = row.to_dict()
                # Filter out any NaN values
                record_data = {k: v for k, v in record_data.items() if k in ['commodity', 'age_group', 
                                                                            'mean_consumption_chronic', 
                                                                            'mean_consumption_acute',
                                                                            'percentile_97_chronic', 
                                                                            'percentile_97_acute', 
                                                                            'notes']}
                self.add_record(table_name, record_data)
            
            return True
        return False
    
    def get_versions(self, table_name, record_id):
        """Get version history information for a record."""
        # In a real system, you'd have a proper version history table
        # This is a simplified placeholder
        current = self.get_record_by_id(table_name, record_id)
        if current:
            return [{"version": current["version"], "updated_at": current["updated_at"]}]
        return []
    
    def close(self):
        """Close the database connection."""
        self.conn.close()