# components/utils.py
import pandas as pd
import io
import base64

def parse_paste_data(data_string):
    """Parse pasted tabular data into a pandas DataFrame."""
    # Try to infer the delimiter
    for delimiter in [',', '\t', ';', '|']:
        try:
            df = pd.read_csv(io.StringIO(data_string), delimiter=delimiter)
            # If we got more than one column, assume this is the right delimiter
            if len(df.columns) > 1:
                return df
        except:
            continue
    
    # If all else fails, try to read it as a CSV
    try:
        return pd.read_csv(io.StringIO(data_string))
    except:
        # Return an empty DataFrame if parsing fails
        return pd.DataFrame()

def parse_uploaded_file(file_path):
    """Parse an uploaded file into a pandas DataFrame."""
    try:
        # First try reading as CSV
        return pd.read_csv(file_path)
    except:
        # Then try reading as Excel
        try:
            return pd.read_excel(file_path)
        except:
            # Return an empty DataFrame if parsing fails
            return pd.DataFrame()

def decode_dragged_data(data_uri):
    """Decode data URI from dragged and dropped content."""
    if not data_uri or not data_uri.startswith('data:'):
        return None
    
    # Split the data URI into metadata and content
    header, encoded = data_uri.split(',', 1)
    
    # Decode content
    content = base64.b64decode(encoded).decode('utf-8')
    
    # Parse into DataFrame
    return parse_paste_data(content)

def format_validation_message(validation_errors):
    """Format validation errors into an HTML message."""
    if not validation_errors:
        return "<div class='alert alert-success'>Data is valid.</div>"
    
    error_count = sum(1 for errors in validation_errors if errors)
    if error_count == 0:
        return "<div class='alert alert-success'>Data is valid.</div>"
    
    message = f"<div class='alert alert-danger'><strong>{error_count} validation errors found:</strong><ul>"
    
    # Show up to 5 errors
    error_shown = 0
    for i, errors in enumerate(validation_errors):
        if errors and error_shown < 5:
            message += f"<li>Row {i+1}: "
            message += ", ".join([f"{field}: {error}" for field, error in errors.items()])
            message += "</li>"
            error_shown += 1
    
    if error_shown < error_count:
        message += f"<li>...and {error_count - error_shown} more errors</li>"
    
    message += "</ul></div>"
    return message