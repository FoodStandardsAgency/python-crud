# components/data_entry.py
from shiny import ui, module, reactive
import shinyswatch
import pandas as pd
from io import StringIO
import traceback

from database.db_manager import DatabaseManager
from database.schema import SchemaValidator

@module.ui
def data_entry_ui():
    """UI for data entry component."""
    return ui.nav_panel(
        "Data Entry",
        ui.layout_sidebar(
            ui.sidebar(
                ui.h4("Add New Record"),
                ui.input_text("commodity", "Commodity", placeholder="e.g., Almonds"),
                ui.input_text("age_group", "Age Group", placeholder="e.g., Adults"),
                ui.input_numeric("mean_consumption_chronic", "Mean Consumption (Chronic)", value=None),
                ui.input_numeric("mean_consumption_acute", "Mean Consumption (Acute)", value=None),
                ui.input_numeric("percentile_97_chronic", "97th Percentile (Chronic)", value=None),
                ui.input_numeric("percentile_97_acute", "97th Percentile (Acute)", value=None),
                ui.input_text_area("notes", "Notes", placeholder="Additional information", width="100%"),
                ui.div(
                    ui.input_action_button("add_record", "Add Record", class_="btn-primary"),
                    ui.input_action_button("clear_form", "Clear Form", class_="btn-secondary"),
                    class_="d-flex justify-content-between my-2"
                ),
                ui.hr(),
                ui.h4("Bulk Import"),
                ui.p("Upload a CSV file or paste CSV data:"),
                ui.input_file("csv_upload", "Upload CSV", accept=[".csv"], multiple=False),
                ui.input_text_area("csv_paste", "Or paste CSV data:", height="150px"),
                ui.input_action_button("preview_csv", "Preview Data", class_="btn-info"),
                ui.input_action_button("import_csv", "Import Data", class_="btn-success"),
                width=300
            ),
            ui.card(
                ui.card_header(ui.h3("Data Preview")),
                ui.div(
                    ui.output_ui("validation_messages"),
                    ui.output_data_frame("preview_table")
                ),
                full_screen=True
            )
        )
    )

@module.server
def data_entry_server(input, output, session):
    """Server logic for data entry component."""
    db = DatabaseManager()
    validator = SchemaValidator()
    
    preview_data = reactive.Value(pd.DataFrame())
    validation_errors = reactive.Value([])
    
    @reactive.Effect
    @reactive.event(input.add_record)
    def handle_add_record():
        """Handle adding a new record."""
        record = {
            'commodity': input.commodity(),
            'age_group': input.age_group(),
            'mean_consumption_chronic': input.mean_consumption_chronic(),
            'mean_consumption_acute': input.mean_consumption_acute(),
            'percentile_97_chronic': input.percentile_97_chronic(),
            'percentile_97_acute': input.percentile_97_acute(),
            'notes': input.notes()
        }
        
        # Validate record
        errors = validator.validate_record('food_consumption', record)
        if errors:
            error_msg = "<div class='alert alert-danger'>"
            error_msg += "<strong>Validation errors:</strong><ul>"
            for field, msg in errors.items():
                error_msg += f"<li>{msg}</li>"
            error_msg += "</ul></div>"
            output.validation_messages.set_content(error_msg)
            return
        
        # Insert into database
        try:
            db.add_record('food_consumption', record)
            success_msg = "<div class='alert alert-success'>Record added successfully!</div>"
            output.validation_messages.set_content(success_msg)
            clear_form()
        except Exception as e:
            error_msg = f"<div class='alert alert-danger'>Error adding record: {str(e)}</div>"
            output.validation_messages.set_content(error_msg)
    
    @reactive.Effect
    @reactive.event(input.clear_form)
    def clear_form():
        """Clear the form inputs."""
        ui.update_text(input, "commodity", value="")
        ui.update_text(input, "age_group", value="")
        ui.update_numeric(input, "mean_consumption_chronic", value=None)
        ui.update_numeric(input, "mean_consumption_acute", value=None)
        ui.update_numeric(input, "percentile_97_chronic", value=None)
        ui.update_numeric(input, "percentile_97_acute", value=None)
        ui.update_text_area(input, "notes", value="")
    
    @reactive.Effect
    @reactive.event(input.preview_csv)
    def preview_csv_data():
        """Preview CSV data from file upload or paste."""
        csv_data = None
        
        # Check if there's an uploaded file
        if input.csv_upload() and len(input.csv_upload()) > 0:
            file_info = input.csv_upload()[0]
            csv_data = file_info["datapath"]
            try:
                df = pd.read_csv(csv_data)
            except Exception as e:
                error_msg = f"<div class='alert alert-danger'>Error parsing CSV file: {str(e)}</div>"
                output.validation_messages.set_content(error_msg)
                preview_data.set(pd.DataFrame())
                return
        
        # Check if there's pasted CSV data
        elif input.csv_paste():
            csv_text = input.csv_paste()
            try:
                df = pd.read_csv(StringIO(csv_text))
            except Exception as e:
                error_msg = f"<div class='alert alert-danger'>Error parsing CSV text: {str(e)}</div>"
                output.validation_messages.set_content(error_msg)
                preview_data.set(pd.DataFrame())
                return
        else:
            output.validation_messages.set_content("<div class='alert alert-warning'>Please upload a CSV file or paste CSV data.</div>")
            return
        
        # Validate the dataframe against schema
        try:
            validated_df, row_errors, has_errors = validator.validate_dataframe('food_consumption', df)
            preview_data.set(validated_df)
            validation_errors.set(row_errors)
            
            if has_errors:
                error_msg = "<div class='alert alert-warning'>"
                error_msg += "<strong>Validation warnings:</strong> Some data may need correction. Hover over cells for details.</div>"
                output.validation_messages.set_content(error_msg)
            else:
                output.validation_messages.set_content("<div class='alert alert-success'>Data validated successfully. Ready to import.</div>")
        except Exception as e:
            error_msg = f"<div class='alert alert-danger'>Validation error: {str(e)}</div>"
            output.validation_messages.set_content(error_msg)
            traceback.print_exc()
    
    @reactive.Effect
    @reactive.event(input.import_csv)
    def import_csv_data():
        """Import validated CSV data into the database."""
        if preview_data() is None or preview_data().empty:
            output.validation_messages.set_content("<div class='alert alert-warning'>No data to import. Please preview data first.</div>")
            return
        
        # Check if there are validation errors
        if any(errors for errors in validation_errors() if errors):
            output.validation_messages.set_content("<div class='alert alert-danger'>Cannot import data with validation errors. Please fix the issues first.</div>")
            return
        
        try:
            # Clean data for import
            clean_df = validator.clean_dataframe_for_import('food_consumption', preview_data())
            
            # Import data
            success = db.import_csv_data('food_consumption', clean_df)
            
            if success:
                output.validation_messages.set_content(f"<div class='alert alert-success'>Successfully imported {len(clean_df)} records!</div>")
                # Clear the preview
                preview_data.set(pd.DataFrame())
                # Clear the file input and paste area
                ui.update_text_area(input, "csv_paste", value="")
            else:
                output.validation_messages.set_content("<div class='alert alert-danger'>Failed to import data.</div>")
        except Exception as e:
            error_msg = f"<div class='alert alert-danger'>Error importing data: {str(e)}</div>"
            output.validation_messages.set_content(error_msg)
            traceback.print_exc()
    
    @output
    @render.data_frame
    def preview_table():
        """Render the preview table."""
        df = preview_data()
        if df is None or df.empty:
            return None
        
        return render.DataGrid(
            df,
            width="100%",
            height="400px"
        )