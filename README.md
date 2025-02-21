# Food Data Management System

A CRUD web application built with Python Shiny and DuckDB for managing food consumption data.

## Features

- **Data Management**: Create, read, update, and delete food consumption data
- **Data Validation**: Validate input data against a defined schema
- **Bulk Import**: Import data from CSV files
- **Export Options**: Download data as CSV or Excel
- **Version Tracking**: Track changes to records with version information
- **Undo Delete**: Restore accidentally deleted records

## Getting Started

### Prerequisites

- Python 3.7+
- Pip package manager

### Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/food-data-management.git
   cd food-data-management
   ```

2. Run the setup script to install dependencies and create the folder structure:
   ```
   python setup.py
   ```

### Running the Application

Start the application with:

```
python app.py
```

The application will be available at http://localhost:8000 in your web browser.

You can specify custom schema and database files:

```
python app.py --schema path/to/schema.yaml --db path/to/database.db
```

Both YAML and TOML schema formats are supported.

## Usage

### Data Entry

1. Enter food consumption data manually using the form
2. Import data in bulk by uploading a CSV file or pasting CSV data
3. Preview and validate data before importing

### Data Display

1. View all food consumption records
2. Filter data by commodity or age group
3. Export data to CSV or Excel format
4. Toggle edit mode to make changes
5. Restore accidentally deleted records

## Database Schema

The database schema is configurable through YAML or TOML files.

### Default Schema Structure

The main table structure is as follows:

- `id`: Unique identifier for the record
- `commodity`: Name of the food commodity (e.g., "Almonds")
- `age_group`: Population age group (e.g., "Adults")
- `mean_consumption_chronic`: Average chronic consumption (float)
- `mean_consumption_acute`: Average acute consumption (float)
- `percentile_97_chronic`: 97th percentile chronic consumption (float)
- `percentile_97_acute`: 97th percentile acute consumption (float)
- `notes`: Additional information (text)
- `version`: Record version number
- `created_at`: Timestamp when record was created
- `updated_at`: Timestamp when record was last updated

### Schema Configuration

You can modify the database schema by editing the schema files:

- YAML schema: `database/schemas.yaml`
- TOML schema: `database/schemas.toml`

Example schema format (YAML):

```yaml
table_name:
  field_name:
    type: str|int|float|bool|date|datetime
    required: true|false
    min: minimum_value           # for numeric types
    max: maximum_value           # for numeric types
    description: "Field description"
```

Example schema format (TOML):

```toml
[table_name.field_name]
type = "str|int|float|bool|date|datetime"
required = true|false
min = minimum_value              # for numeric types
max = maximum_value              # for numeric types
description = "Field description"
```

After modifying a schema, restart the application with the new schema file:

```
python app.py --schema database/schemas.yaml
```

## Moving to Production

### Cloud Deployment

To deploy this application to the cloud:

1. For Azure SQL:
   - Update the `db_manager.py` file to use pyodbc or another SQL Server connector
   - Modify connection strings to point to your Azure SQL database

2. For Databricks:
   - Update the database connection to use the Databricks SQL connector
   - Configure access credentials for your Databricks instance

3. For shinyapps.io:
   - Create a `requirements.txt` file with all dependencies
   - Follow the shinyapps.io deployment guide for Python applications

## Future Enhancements

- User authentication system
- More data visualization options
- Full version history tracking
- Multi-table support
- Cloud integration templates

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.