# app.py
import os
import argparse
from shiny import App, ui, reactive
import shinyswatch

from components.data_entry import data_entry_ui, data_entry_server
from components.data_display import data_display_ui, data_display_server
from database.db_manager import DatabaseManager
from database.schema import SchemaValidator

# Parse command line arguments
parser = argparse.ArgumentParser(description='Food Data Management App')
parser.add_argument('--schema', type=str, help='Path to schema file (YAML or TOML)')
parser.add_argument('--db', type=str, help='Path to database file')
args = parser.parse_args()

# Initialize database and schema
db_path = args.db if args.db else 'data/food_data.db'
schema_path = args.schema if args.schema else os.path.join('database', 'schemas.yaml')

db = DatabaseManager(db_path=db_path)
schema_validator = SchemaValidator(schema_file=schema_path)

# Define app UI
app_ui = ui.page_fluid(
    shinyswatch.theme.cosmo(),  # Use a clean, modern theme
    ui.head_content(
        ui.tags.title("Food Data Management"),
        ui.tags.style("""
            .table-container {
                max-height: 500px;
                overflow-y: auto;
            }
            .card {
                margin-bottom: 20px;
            }
            .btn-group-xs > .btn, .btn-xs {
                padding: .25rem .4rem;
                font-size: .875rem;
                line-height: .5;
                border-radius: .2rem;
            }
        """)
    ),
    ui.layout_sidebar(
        ui.sidebar(
            ui.h3("Food Data Management"),
            ui.p("A CRUD interface for managing food consumption data"),
            ui.hr(),
            ui.h4("Navigation"),
            ui.navset_pill_list(
                ui.nav("Data Entry", icon=ui.img(src="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.5.0/icons/pencil-square.svg", height="16px")),
                ui.nav("Data Display", icon=ui.img(src="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.5.0/icons/table.svg", height="16px")),
            ),
            ui.hr(),
            ui.p("Version: 1.0.0"),
            width=200,
            bg="#f8f9fa",
        ),
        ui.navset_tab(
            data_entry_ui(),
            data_display_ui(),
            id="main_tabs"
        )
    )
)

# Define app server
def server(input, output, session):
    # Initialize modules
    data_entry_server("data_entry", "data_display")
    data_display_server("data_display", "data_entry")

# Create Shiny app
app = App(app_ui, server)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)