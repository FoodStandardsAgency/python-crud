# database/schema.py
import pandas as pd
import numpy as np
import os
import yaml
import toml

class SchemaValidator:
    def __init__(self, schema_file=None):
        """
        Initialize schema validator with schema from YAML or TOML file.
        
        Args:
            schema_file: Path to YAML or TOML file containing schema definitions.
                         If None, uses default schema file in database directory.
        """
        if schema_file is None:
            # Default to schemas.yaml in the database directory
            schema_file = os.path.join(os.path.dirname(__file__), 'schemas.yaml')
        
        self.schemas = self._load_schema(schema_file)
        
    def _load_schema(self, schema_file):
        """
        Load schema from YAML or TOML file.
        
        Args:
            schema_file: Path to schema file
            
        Returns:
            Dictionary of schema definitions
        """
        if not os.path.exists(schema_file):
            print(f"Warning: Schema file {schema_file} not found, using default schema")
            return self._get_default_schema()
        
        # Determine file type based on extension
        file_ext = os.path.splitext(schema_file)[1].lower()
        
        try:
            with open(schema_file, 'r') as f:
                if file_ext == '.yaml' or file_ext == '.yml':
                    raw_schema = yaml.safe_load(f)
                elif file_ext == '.toml':
                    raw_schema = toml.load(f)
                else:
                    print(f"Warning: Unsupported schema file type: {file_ext}, using default schema")
                    return self._get_default_schema()
            
            # Process the schema to convert string type names to actual types
            processed_schema = {}
            for table_name, table_schema in raw_schema.items():
                processed_table = {}
                for field, rules in table_schema.items():
                    processed_rules = rules.copy()
                    # Convert 'type' from string to actual type
                    if 'type' in rules:
                        type_str = rules['type'].lower()
                        if type_str == 'str' or type_str == 'string':
                            processed_rules['type'] = str
                        elif type_str == 'float':
                            processed_rules['type'] = float
                        elif type_str == 'int' or type_str == 'integer':
                            processed_rules['type'] = int
                        elif type_str == 'bool' or type_str == 'boolean':
                            processed_rules['type'] = bool
                        elif type_str == 'date':
                            processed_rules['type'] = 'date'  # Special handling during validation
                        elif type_str == 'datetime':
                            processed_rules['type'] = 'datetime'  # Special handling during validation
                    processed_table[field] = processed_rules
                processed_schema[table_name] = processed_table
            
            return processed_schema
        except Exception as e:
            print(f"Error loading schema file: {e}, using default schema")
            return self._get_default_schema()
    
    def _get_default_schema(self):
        """Return the default schema definition."""
        return {
            'food_consumption': {
                'commodity': {'type': str, 'required': True},
                'age_group': {'type': str, 'required': True},
                'mean_consumption_chronic': {'type': float, 'required': False, 'min': 0},
                'mean_consumption_acute': {'type': float, 'required': False, 'min': 0},
                'percentile_97_chronic': {'type': float, 'required': False, 'min': 0},
                'percentile_97_acute': {'type': float, 'required': False, 'min': 0},
                'notes': {'type': str, 'required': False}
            }
        }
    
    def validate_record(self, table_name, record):
        """Validate a single record against the schema."""
        if table_name not in self.schemas:
            raise ValueError(f"Unknown table: {table_name}")
        
        schema = self.schemas[table_name]
        errors = {}
        
        for field, rules in schema.items():
            # Check required fields
            if rules.get('required', False) and (field not in record or record[field] is None or record[field] == ''):
                errors[field] = f"{field} is required"
                continue
                
            # Skip validation if field is not present and not required
            if field not in record or record[field] is None or record[field] == '':
                continue
                
            # Type validation
            expected_type = rules.get('type')
            if expected_type:
                try:
                    # Try to convert to expected type
                    if expected_type == float:
                        record[field] = float(record[field])
                    elif expected_type == int:
                        record[field] = int(record[field])
                    elif expected_type == str:
                        record[field] = str(record[field])
                except (ValueError, TypeError):
                    errors[field] = f"{field} must be a valid {expected_type.__name__}"
                    continue
            
            # Min/max validation for numeric types
            if (expected_type in (int, float)) and isinstance(record[field], (int, float)):
                if 'min' in rules and record[field] < rules['min']:
                    errors[field] = f"{field} must be at least {rules['min']}"
                if 'max' in rules and record[field] > rules['max']:
                    errors[field] = f"{field} must be at most {rules['max']}"
        
        return errors
    
    def validate_dataframe(self, table_name, df):
        """Validate a pandas DataFrame against the schema."""
        if table_name not in self.schemas:
            raise ValueError(f"Unknown table: {table_name}")
        
        schema = self.schemas[table_name]
        
        # Create a copy to avoid modifying the original
        validated_df = df.copy()
        
        # Check required columns
        required_columns = [col for col, rules in schema.items() if rules.get('required', False)]
        missing_columns = [col for col in required_columns if col not in validated_df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
        
        # Initialize error tracking
        row_errors = [{}] * len(validated_df)
        has_errors = False
        
        # Validate each column
        for col, rules in schema.items():
            if col not in validated_df.columns:
                if rules.get('required', False):
                    for i in range(len(row_errors)):
                        row_errors[i][col] = f"{col} is required"
                    has_errors = True
                continue
            
            # Apply type conversion
            expected_type = rules.get('type')
            if expected_type == float:
                # Convert to numeric, coerce errors to NaN
                validated_df[col] = pd.to_numeric(validated_df[col], errors='coerce')
                
                # Mark NaN values as errors if the column is required
                if rules.get('required', False):
                    for i, val in enumerate(validated_df[col].isna()):
                        if val:
                            row_errors[i][col] = f"{col} must be a valid number"
                            has_errors = True
                
                # Apply min/max validation
                if 'min' in rules:
                    min_val = rules['min']
                    for i, val in enumerate(validated_df[col]):
                        if not pd.isna(val) and val < min_val:
                            row_errors[i][col] = f"{col} must be at least {min_val}"
                            has_errors = True
                            
                if 'max' in rules:
                    max_val = rules['max']
                    for i, val in enumerate(validated_df[col]):
                        if not pd.isna(val) and val > max_val:
                            row_errors[i][col] = f"{col} must be at most {max_val}"
                            has_errors = True
            
            elif expected_type == str:
                # Convert non-string values to strings
                validated_df[col] = validated_df[col].astype(str)
                
                # Check if empty and required
                if rules.get('required', False):
                    for i, val in enumerate(validated_df[col]):
                        if val == '' or val.isspace():
                            row_errors[i][col] = f"{col} cannot be empty"
                            has_errors = True
        
        return validated_df, row_errors, has_errors
    
    def clean_dataframe_for_import(self, table_name, df):
        """Clean and prepare a DataFrame for database import."""
        if table_name not in self.schemas:
            raise ValueError(f"Unknown table: {table_name}")
        
        schema = self.schemas[table_name]
        clean_df = df.copy()
        
        # Only keep columns that exist in the schema
        valid_columns = list(schema.keys())
        clean_df = clean_df[[col for col in clean_df.columns if col in valid_columns]]
        
        # Convert data types according to schema
        for col, rules in schema.items():
            if col in clean_df.columns:
                expected_type = rules.get('type')
                if expected_type == float:
                    clean_df[col] = pd.to_numeric(clean_df[col], errors='coerce')
                elif expected_type == str:
                    # Replace NaN with empty string before converting to string
                    clean_df[col] = clean_df[col].fillna('').astype(str)
        
        return clean_df