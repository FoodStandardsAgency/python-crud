# components/data_display.py
from shiny import ui, module, reactive
import pandas as pd
from htmltools import HTML

from database.db_manager import DatabaseManager

@module.ui
def data_display_ui():
    """UI for data display component."""
    return ui.nav_panel(
        "Data Display",
        ui.layout_sidebar(
            ui.sidebar(
                ui.h4("Filter Data"),
                ui.input_text("filter_commodity", "Filter by Commodity", placeholder="Enter text to filter"),
                ui.input_text("filter_age_group", "Filter by Age Group", placeholder="Enter text to filter"),
                ui.input_action_button("refresh_data", "Refresh Data", class_="btn-primary"),
                ui.hr(),
                ui.h4("Export Options"),
                ui.download_button("download_csv", "Download CSV"),
                ui.download_button("download_excel", "Download Excel"),
                ui.hr(),
                ui.div(
                    ui.h4("Recently Deleted"),
                    ui.output_table("deleted_records_table"),
                    ui.input_action_button("show_deleted", "Show Deleted", class_="btn-secondary"),
                    ui.input_action_button("undo_delete", "Undo Delete", class_="btn-warning"),
                    id="deleted_records_panel",
                    style="display: none;"
                ),
                width=250
            ),
            ui.card(
                ui.card_header(
                    ui.div(
                        ui.h3("Food Consumption Data", class_="d-inline"),
                        ui.span(
                            ui.input_action_button("toggle_edit", "Toggle Edit Mode", class_="btn-warning"),
                            class_="float-end"
                        )
                    )
                ),
                ui.output_ui("status_message"),
                ui.output_data_frame("data_table"),
                full_screen=True
            )
        )
    )

@module.server
def data_display_server(input, output, session):
    """Server logic for data display component."""
    db = DatabaseManager()
    
    # Reactive values
    data = reactive.Value(pd.DataFrame())
    edit_mode = reactive.Value(False)
    selected_row = reactive.Value(None)
    deleted_records = reactive.Value([])
    
    def load_data():
        """Load data from database with filters applied."""
        try:
            # Get all records
            df = db.get_all_records('food_consumption')
            
            # Apply filters if provided
            if input.filter_commodity():
                df = df[df['commodity'].str.contains(input.filter_commodity(), case=False, na=False)]
            
            if input.filter_age_group():
                df = df[df['age_group'].str.contains(input.filter_age_group(), case=False, na=False)]
            
            data.set(df)
            output.status_message.set_content("")
            return df
        except Exception as e:
            error_msg = f"<div class='alert alert-danger'>Error loading data: {str(e)}</div>"
            output.status_message.set_content(error_msg)
            return pd.DataFrame()
    
    @reactive.Effect
    def initialize_data():
        """Initialize the data table on startup."""
        load_data()
    
    @reactive.Effect
    @reactive.event(input.refresh_data)
    def refresh_data():
        """Refresh data from database."""
        load_data()
        
    @reactive.Effect
    @reactive.event(input.edit_record)
    def handle_edit_record():
        """Open a modal dialog to edit a record."""
        record_id = input.edit_record()
        if record_id:
            record = db.get_record_by_id('food_consumption', record_id)
            if record:
                # Convert record to dict
                record_dict = dict(zip(record.keys(), record))
                
                # Create a modal dialog for editing
                ui.modal_show(
                    ui.modal(
                        ui.modal_header("Edit Record"),
                        ui.modal_body(
                            ui.input_text("edit_commodity", "Commodity", value=record_dict['commodity']),
                            ui.input_text("edit_age_group", "Age Group", value=record_dict['age_group']),
                            ui.input_numeric("edit_mean_chronic", "Mean Consumption (Chronic)", 
                                           value=record_dict['mean_consumption_chronic']),
                            ui.input_numeric("edit_mean_acute", "Mean Consumption (Acute)", 
                                           value=record_dict['mean_consumption_acute']),
                            ui.input_numeric("edit_p97_chronic", "97th Percentile (Chronic)", 
                                           value=record_dict['percentile_97_chronic']),
                            ui.input_numeric("edit_p97_acute", "97th Percentile (Acute)", 
                                           value=record_dict['percentile_97_acute']),
                            ui.input_text_area("edit_notes", "Notes", value=record_dict['notes']),
                            ui.input_hidden("edit_record_id", value=record_id)
                        ),
                        ui.modal_footer(
                            ui.input_action_button("save_edit", "Save Changes", class_="btn-primary"),
                            ui.input_action_button("cancel_edit", "Cancel", class_="btn-secondary")
                        ),
                        size="lg"
                    )
                )
            else:
                output.status_message.set_content("<div class='alert alert-danger'>Record not found.</div>")
                
    @reactive.Effect
    @reactive.event(input.save_edit)
    def save_edited_record():
        """Save changes to an edited record."""
        record_id = input.edit_record_id()
        if record_id:
            # Collect edited data
            updated_record = {
                'commodity': input.edit_commodity(),
                'age_group': input.edit_age_group(),
                'mean_consumption_chronic': input.edit_mean_chronic(),
                'mean_consumption_acute': input.edit_mean_acute(),
                'percentile_97_chronic': input.edit_p97_chronic(),
                'percentile_97_acute': input.edit_p97_acute(),
                'notes': input.edit_notes()
            }
            
            # Update in database
            db.update_record('food_consumption', record_id, updated_record)
            
            # Close modal and refresh data
            ui.modal_remove()
            load_data()
            output.status_message.set_content("<div class='alert alert-success'>Record updated successfully.</div>")
    
    @reactive.Effect
    @reactive.event(input.cancel_edit)
    def cancel_edit():
        """Cancel editing and close modal."""
        ui.modal_remove()
    
    @reactive.Effect
    @reactive.event(input.toggle_edit)
    def toggle_edit_mode():
        """Toggle between view and edit modes."""
        edit_mode.set(not edit_mode.get())
        if edit_mode.get():
            output.status_message.set_content("<div class='alert alert-info'>Edit mode enabled. Click on a row to edit or delete.</div>")
        else:
            output.status_message.set_content("")
            
    @reactive.Effect
    @reactive.event(input.delete_record)
    def handle_delete_record():
        """Delete a record and update the deleted records list."""
        record_id = input.delete_record()
        if record_id:
            # Get the record before deleting it
            record = db.get_record_by_id('food_consumption', record_id)
            
            # Delete the record
            success = db.delete_record('food_consumption', record_id)
            
            if success:
                # Add to deleted records list
                deleted = deleted_records.get()
                deleted.append({
                    'id': record_id,
                    'commodity': record['commodity'],
                    'age_group': record['age_group']
                })
                deleted_records.set(deleted)
                
                # Show the deleted records panel
                ui.run_js("$('#deleted_records_panel').show();", session=session)
                
                # Refresh the data
                load_data()
                
                output.status_message.set_content("<div class='alert alert-success'>Record deleted successfully. Use 'Undo Delete' to restore it.</div>")
            else:
                output.status_message.set_content("<div class='alert alert-danger'>Failed to delete record.</div>")
                
    @reactive.Effect
    @reactive.event(input.undo_delete)
    def handle_undo_delete():
        """Restore the most recently deleted record."""
        deleted = deleted_records.get()
        if deleted:
            # Get the most recent deleted record
            record = deleted[-1]
            
            # Restore the record
            success = db.undo_delete(record['id'], 'food_consumption')
            
            if success:
                # Remove from deleted records list
                deleted.pop()
                deleted_records.set(deleted)
                
                # Hide the panel if no more deleted records
                if not deleted:
                    ui.run_js("$('#deleted_records_panel').hide();", session=session)
                
                # Refresh the data
                load_data()
                
                output.status_message.set_content("<div class='alert alert-success'>Record restored successfully.</div>")
            else:
                output.status_message.set_content("<div class='alert alert-danger'>Failed to restore record.</div>")
        else:
            output.status_message.set_content("<div class='alert alert-warning'>No deleted records to restore.</div>")
    
    @output
    @render.table
    def deleted_records_table():
        """Display recently deleted records."""
        deleted = deleted_records.get()
        if deleted:
            # Create a DataFrame from the list of deleted records
            df = pd.DataFrame(deleted)
            # Only show the most recent 5 deleted records
            return df.tail(5)[['id', 'commodity', 'age_group']]
        return pd.DataFrame()
    
    @reactive.Effect
    @reactive.event(input.show_deleted)
    def toggle_deleted_records():
        """Show/hide the deleted records panel."""
        ui.run_js("$('#deleted_records_panel').toggle();", session=session)
    
    # Download handlers
    @session.download
    def download_csv():
        """Download the current dataset as CSV."""
        df = data.get()
        csv_data = df.to_csv(index=False)
        return {"filename": "food_consumption_data.csv", "content": csv_data}
    
    @session.download
    def download_excel():
        """Download the current dataset as Excel."""
        df = data.get()
        # Create a BytesIO object
        output = pd.io.excel.BytesIO()
        # Use ExcelWriter to write to the BytesIO object
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Food Consumption', index=False)
        # Get the BytesIO object's contents
        excel_data = output.getvalue()
        return {"filename": "food_consumption_data.xlsx", "content": excel_data}
    
    @output
    @render.data_frame
    def data_table():
        """Render the data table."""
        df = data.get()
        if df is None or df.empty:
            return None
        
        # If in edit mode, add action buttons
        if edit_mode.get():
            # Create a copy to avoid modifying the reactive value
            display_df = df.copy()
            
            # Add edit and delete buttons
            actions = []
            for idx in display_df.index:
                record_id = display_df.loc[idx, 'id']
                edit_btn = f"<button class='btn btn-sm btn-info edit-btn' data-id='{record_id}'>Edit</button>"
                delete_btn = f"<button class='btn btn-sm btn-danger delete-btn' data-id='{record_id}'>Delete</button>"
                actions.append(f"{edit_btn} {delete_btn}")
            
            display_df['Actions'] = actions
            
            # Make the grid interactive with JavaScript handlers
            ui.insert_ui(
                ui.tags.script("""
                $(document).on('click', '.edit-btn', function() {
                    var id = $(this).data('id');
                    Shiny.setInputValue('edit_record', id);
                });
                
                $(document).on('click', '.delete-btn', function() {
                    var id = $(this).data('id');
                    if(confirm('Are you sure you want to delete this record?')) {
                        Shiny.setInputValue('delete_record', id);
                    }
                });
                """),
                selector="head",
                immediate=True,
                session=session
            )
            
            return render.DataGrid(
                display_df,
                width="100%",
                height="500px",
                filters=True,
                sortable=True,
                row_selection_mode="single"
            )
        else:
            # Regular view mode
            return render.DataGrid(
                df,
                width="100%",
                height="500px",
                filters=True,
                sortable=True
            )