# Tribal Land Checker

**Tribal Land Checker** is a Python tool designed to identify whether properties in the United States are located on federally recognized Tribal lands. It uses address geocoding and official Tribal land boundary data from the Bureau of Indian Affairs (BIA) or Census TIGER/Line files. This tool was created in response to a real business need to identify and categorize real estate properties that are located on federally identified tribal land. The results are meant to be used to be inline with compliance during a bank aquisition transition. 

---

## Features

- Geocode U.S. addresses to latitude and longitude using **OpenStreetMap (Nominatim)** or **Census Geocoder**.
- Check if a location falls within federally recognized Tribal land boundaries.
- Process Excel files with addresses and output results with:

  - Latitude and Longitude columns
  - Tribal land indicator (`Yes` / `No` / `Could Not Geocode`)

- Supports shapefile or GeoJSON Tribal boundary files.
- Automatic address column detection for Excel files.

---

## Installation

1. Clone the repository or download the script.
2. Install required Python packages:

```bash
pip install pandas geopandas shapely requests openpyxl
```

> `openpyxl` is needed to read and write Excel files.

---

## Usage

### Command Line

```bash
python tribal_land_checker.py path/to/addresses.xlsx
```

The script will prompt for a custom Tribal boundaries file if you have one; otherwise, it will download boundaries automatically from Census TIGER/Line data.

### Interactive Input

- **Input Excel file**: The Excel file containing property addresses.
- **Custom Tribal boundaries file**: Optional shapefile or GeoJSON file.
- **Address column detection**: Automatically detects common address column names. If none are found, it prompts for manual selection.

---

## Output

- A new Excel file is generated with `_tribal_checked` appended to the original filename.
- Example added columns:

  - `Latitude` – latitude of the address
  - `Longitude` – longitude of the address
  - `On_Tribal_Land` – `"Yes"` if the property is on Tribal land, `"No"` if not, `"Could Not Geocode"` if geocoding failed

---

## Example

```python
from tribal_land_checker import TribalLandChecker

checker = TribalLandChecker(tribal_boundaries_path="tribal_boundaries.geojson")
df = checker.process_excel("addresses.xlsx")
print(df.head())
```

---

## Notes

- Geocoding via Nominatim is **rate-limited** (1 request per second). For large datasets, consider using **Google Maps API** or **Census Geocoder** for batch geocoding.
- If the boundaries file is not provided, the script will attempt to download boundaries automatically.
- The script expects **U.S. addresses** only.
- Logging is enabled to track progress and errors.

---

## Resources

- [Census TIGER/Line Files: AIANNH](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html)
- [Bureau of Indian Affairs GIS Data](https://biamaps.doi.gov/)

---

## License

MIT License (or specify your preferred license)
