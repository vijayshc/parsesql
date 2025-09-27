# Lineage Visualizer

A modern, interactive web-based tool for visualizing SQL column lineage data. This application transforms CSV lineage exports into beautiful, interactive graphs that help users understand data flow and dependencies between tables and columns.

## Overview

The Lineage Visualizer provides an intuitive interface for exploring complex SQL lineage relationships:

- **Interactive Graph Visualization**: Drag and rearrange table cards to explore relationships
- **Column-Level Detail**: Click on any column to highlight upstream sources and downstream consumers
- **Smart Search**: Find specific tables or columns instantly with live filtering
- **Expression Inspector**: View SQL expressions and file sources for each lineage connection
- **Theme Support**: Toggle between light and dark themes
- **Export Functionality**: Download high-resolution PNG snapshots of your lineage diagrams

## Architecture

### Frontend (Web Application)
- **app.js**: Core JavaScript application handling CSV parsing, graph rendering, and user interactions
- **index.html**: Main HTML structure with upload interface and layout containers
- **styles.css**: Comprehensive CSS with dark/light theme support and responsive design

### Backend (Python Utilities)
- **transform.py**: Data transformation utilities for converting lineage CSV to graph structures
- **__init__.py**: Module exports for easy integration

## Features

### Core Functionality
- **CSV Upload**: Support for standard lineage CSV format with required columns
- **Sample Data**: Built-in sample dataset for testing and demonstrations
- **Interactive Cards**: Draggable table cards with column listings
- **Connection Lines**: Curved SVG paths showing column relationships
- **Highlighting System**: Visual indicators for selected, upstream, and downstream columns

### User Interface
- **Status Bar**: Real-time statistics (column count, connection count, dataset info)
- **Inspector Panel**: Detailed information about selected columns and their relationships
- **Search Panel**: Live search with highlighting and filtering
- **Theme Toggle**: Seamless light/dark mode switching
- **Reset Functionality**: Clear view and return to default layout

### Data Handling
- **Robust CSV Parsing**: Handles missing columns and malformed data gracefully
- **Data Validation**: Warns about missing required fields and data quality issues
- **Normalization**: Consistent handling of table/column naming and case sensitivity
- **Error Reporting**: Clear messaging for parsing errors and data issues

## Installation & Setup

### Option 1: Local Web Server (Recommended)
```bash
# From project root directory
python -m http.server 8000
# Navigate to http://localhost:8000/lineage_visualizer/
```

### Option 2: Direct File Access
Open `index.html` directly in a modern web browser. Note: Some features may be limited without a web server.

## Usage

### Basic Workflow
1. **Load Data**: Upload a lineage CSV file or click "Load sample" to use test data
2. **Explore**: Click on column rows to highlight dependencies
3. **Search**: Use the search panel to find specific tables or columns
4. **Navigate**: Drag table cards to rearrange the layout
5. **Export**: Click "Download PNG" to save the current view

### CSV Format Requirements
The input CSV must contain these columns:
- `source_table`: Source table name
- `source_column`: Source column name
- `expression`: SQL expression (optional)
- `target_column`: Target column name
- `target_table`: Target table name
- `file`: Source file path (optional)

### Example CSV Data
```csv
source_table,source_column,expression,target_column,target_table,file
orders,customer_id,,customer_id,customer_orders,
customers,first_name,,first_name,customer_orders,
orders,total_amount,,total_amount,customer_orders,
```

## API Reference

### Python Module (`transform.py`)

#### `load_lineage_csv(path, encoding="utf-8")`
Load lineage data from CSV file.

**Parameters:**
- `path` (str | Path): Path to CSV file
- `encoding` (str): File encoding (default: "utf-8")

**Returns:** `LineageGraph` object with nodes, edges, and warnings

#### `transform_records(records)`
Transform raw CSV records into graph structure.

**Parameters:**
- `records` (Iterable[Mapping[str, str]]): CSV row data

**Returns:** `LineageGraph` object

### Data Classes

#### `Node`
Represents a column, table, or expression in the lineage graph.

**Fields:**
- `id`: Unique identifier
- `label`: Display label
- `table`: Table name
- `column`: Column name
- `file`: Source file
- `role`: Node type ("table", "result", "expression")
- `expression`: SQL expression
- `parent`: Parent node ID

#### `Edge`
Represents a lineage connection between nodes.

**Fields:**
- `id`: Unique identifier
- `source`: Source node ID
- `target`: Target node ID
- `expression`: SQL expression
- `file`: Source file

#### `LineageGraph`
Complete graph representation with validation warnings.

**Fields:**
- `nodes`: List of Node objects
- `edges`: List of Edge objects
- `warnings`: List of validation messages

## Development

### File Structure
```
lineage_visualizer/
├── app.js          # Main JavaScript application
├── index.html      # HTML structure and UI
├── styles.css      # CSS styling and themes
├── transform.py    # Python data transformation utilities
├── __init__.py     # Module exports
└── README.md       # This file
```

### Key Components

#### JavaScript Application (`app.js`)
- **State Management**: Centralized state object for nodes, edges, and UI state
- **CSV Parsing**: PapaParse integration for robust CSV handling
- **Graph Rendering**: Dynamic SVG generation for connection lines
- **Interaction Handling**: Event listeners for clicks, drags, and searches
- **Layout Engine**: Automatic positioning and collision detection for cards

#### Styling (`styles.css`)
- **CSS Custom Properties**: Theme-aware color system
- **Responsive Design**: Mobile-friendly layout adjustments
- **Animation**: Smooth transitions for interactions
- **Accessibility**: Focus states and ARIA attributes

### Browser Compatibility
- Modern browsers with ES6+ support
- Chrome 60+, Firefox 55+, Safari 12+, Edge 79+

## Integration Examples

### Python Integration
```python
from lineage_visualizer import load_lineage_csv, transform_records

# Load from file
graph = load_lineage_csv("lineage_data.csv")

# Transform raw records
records = [
    {"source_table": "users", "source_column": "id", "target_column": "user_id", "target_table": "orders"},
    # ... more records
]
graph = transform_records(records)

print(f"Graph has {len(graph.nodes)} nodes and {len(graph.edges)} edges")
```

### Web Integration
```html
<!-- Embed in existing page -->
<iframe src="lineage_visualizer/index.html" width="100%" height="600"></iframe>
```

## Troubleshooting

### Common Issues

**CSV Parsing Errors**
- Ensure CSV has required columns: `source_table`, `source_column`, `target_column`, `target_table`
- Check for proper CSV formatting (commas, quotes, encoding)
- Verify column headers match expected casing

**Display Issues**
- Use a local web server for full functionality
- Check browser console for JavaScript errors
- Ensure modern browser with SVG support

**Performance Issues**
- Large datasets may require longer processing times
- Consider filtering or sampling data for better performance
- Clear browser cache if experiencing display issues

### Debug Mode
Open browser developer tools and check the console for detailed error messages and warnings.

## Contributing

When making changes:
1. Maintain consistent code style and formatting
2. Test with sample data to ensure functionality
3. Update documentation for new features
4. Verify both light and dark theme compatibility

## License

This project is part of the SQL Lineage Parser suite and follows the same licensing terms.
