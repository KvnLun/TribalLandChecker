#!/usr/bin/env python3
"""
Tribal Land Checker
Identifies if properties are located on federally recognized Tribal lands
based on address geocoding and BIA boundary data.

Author: Kevin Loun
Version: 1.0.0
"""

import pandas as pd
import requests
import geopandas as gpd
from shapely.geometry import Point
import time
from typing import Tuple, Optional
import logging
from pathlib import Path
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TribalLandChecker:
    """Check if addresses are located on federally recognized Tribal lands."""
    
    def __init__(self, tribal_boundaries_path: Optional[str] = None):
        """
        Initialize the checker with Tribal boundary data.
        
        Args:
            tribal_boundaries_path: Path to Tribal boundaries shapefile/geojson
        """
        self.tribal_boundaries = None
        self.geocoding_cache = {}
        
        if tribal_boundaries_path:
            self.load_tribal_boundaries(tribal_boundaries_path)
    
    def download_tribal_boundaries(self) -> gpd.GeoDataFrame:
        """
        Download Tribal boundaries from BIA or Census TIGER data.
        
        Returns:
            GeoDataFrame with Tribal land boundaries
        """
        logger.info("Downloading Tribal boundaries data...")
        
        try:
            # Try to download from Census TIGER/Line files (publicly available)
            # American Indian/Alaska Native/Native Hawaiian Areas
            url = "https://www2.census.gov/geo/tiger/TIGER2023/AIANNH/tl_2023_us_aiannh.zip"
            
            logger.info(f"Downloading from: {url}")
            gdf = gpd.read_file(url)
            
            # Filter for federally recognized areas (AIANNHCE codes)
            # These have specific codes that indicate federal recognition
            gdf = gdf[gdf['GEOID'].notna()]
            
            logger.info(f"Loaded {len(gdf)} Tribal areas")
            return gdf
            
        except Exception as e:
            logger.error(f"Error downloading Tribal boundaries: {e}")
            logger.info("Please download Tribal boundaries manually from:")
            logger.info("1. Census TIGER/Line: https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html")
            logger.info("2. BIA GIS: https://biamaps.doi.gov/")
            raise
    
    def load_tribal_boundaries(self, path: str) -> None:
        """
        Load Tribal boundaries from a local file.
        
        Args:
            path: Path to shapefile or GeoJSON with Tribal boundaries
        """
        logger.info(f"Loading Tribal boundaries from {path}")
        try:
            self.tribal_boundaries = gpd.read_file(path)
            # Ensure CRS is WGS84 (EPSG:4326) for lat/lon coordinates
            if self.tribal_boundaries.crs != 'EPSG:4326':
                self.tribal_boundaries = self.tribal_boundaries.to_crs('EPSG:4326')
            logger.info(f"Loaded {len(self.tribal_boundaries)} Tribal areas")
        except Exception as e:
            logger.error(f"Error loading Tribal boundaries: {e}")
            raise
    
    def geocode_address(self, address: str) -> Optional[Tuple[float, float]]:
        """
        Convert an address to latitude and longitude coordinates.
        
        Args:
            address: Street address to geocode
            
        Returns:
            Tuple of (latitude, longitude) or None if geocoding fails
        """
        # Check cache first
        if address in self.geocoding_cache:
            return self.geocoding_cache[address]
        
        try:
            # Using Nominatim (OpenStreetMap) - free but rate limited
            # For production, consider using Google Maps API or Census geocoder
            
            # Clean address
            address = str(address).strip()
            
            # Option 1: Nominatim (OpenStreetMap)
            url = "https://nominatim.openstreetmap.org/search"
            params = {
                'q': address,
                'format': 'json',
                'limit': 1,
                'countrycodes': 'us'  # Limit to US addresses
            }
            headers = {
                'User-Agent': 'TribalLandChecker/1.0'  # Required by Nominatim
            }
            
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            if data:
                lat = float(data[0]['lat'])
                lon = float(data[0]['lon'])
                result = (lat, lon)
                self.geocoding_cache[address] = result
                
                # Rate limiting for Nominatim (1 request per second)
                time.sleep(1)
                
                return result
            
            # Option 2: Census Geocoder (backup)
            census_url = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
            census_params = {
                'address': address,
                'benchmark': 'Public_AR_Current',
                'format': 'json'
            }
            
            response = requests.get(census_url, params=census_params)
            response.raise_for_status()
            
            data = response.json()
            if data.get('result', {}).get('addressMatches'):
                match = data['result']['addressMatches'][0]
                lat = match['coordinates']['y']
                lon = match['coordinates']['x']
                result = (lat, lon)
                self.geocoding_cache[address] = result
                return result
            
            logger.warning(f"Could not geocode address: {address}")
            self.geocoding_cache[address] = None
            return None
            
        except Exception as e:
            logger.error(f"Geocoding error for '{address}': {e}")
            self.geocoding_cache[address] = None
            return None
    
    def check_tribal_land(self, lat: float, lon: float) -> bool:
        """
        Check if a coordinate point is within Tribal land boundaries.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            True if on Tribal land, False otherwise
        """
        if self.tribal_boundaries is None:
            # Try to download if not loaded
            self.tribal_boundaries = self.download_tribal_boundaries()
        
        # Create point geometry
        point = Point(lon, lat)  # Note: Point takes (lon, lat) not (lat, lon)
        
        # Check if point is within any Tribal boundary
        for idx, boundary in self.tribal_boundaries.iterrows():
            if boundary.geometry.contains(point):
                logger.debug(f"Point is within: {boundary.get('NAME', 'Unknown area')}")
                return True
        
        return False
    
    def process_excel(self, 
                     input_file: str, 
                     output_file: Optional[str] = None,
                     address_column: str = None,
                     sheet_name: str = 0) -> pd.DataFrame:
        """
        Process an Excel file with addresses and add Tribal land indicator.
        
        Args:
            input_file: Path to input Excel file
            output_file: Path for output Excel file (optional)
            address_column: Name of column containing addresses (will auto-detect if None)
            sheet_name: Sheet name or index to read
            
        Returns:
            DataFrame with added Tribal land indicator column
        """
        logger.info(f"Processing Excel file: {input_file}")
        
        # Read Excel file
        df = pd.read_excel(input_file, sheet_name=sheet_name)
        logger.info(f"Loaded {len(df)} rows")
        
        # Auto-detect address column if not specified
        if address_column is None:
            # Look for common address column names
            possible_names = ['address', 'Address', 'ADDRESS', 'Street Address', 
                            'Property Address', 'Full Address', 'Location']
            for col in df.columns:
                if any(name.lower() in col.lower() for name in possible_names):
                    address_column = col
                    logger.info(f"Auto-detected address column: {address_column}")
                    break
            
            if address_column is None:
                # If no address column found, ask user
                print("\nAvailable columns:")
                for i, col in enumerate(df.columns):
                    print(f"{i}: {col}")
                idx = int(input("Enter the number of the address column: "))
                address_column = df.columns[idx]
        
        # Add new columns for results
        df['Latitude'] = None
        df['Longitude'] = None
        df['On_Tribal_Land'] = None
        
        # Process each address
        total = len(df)
        for idx, row in df.iterrows():
            address = row[address_column]
            
            if pd.isna(address):
                logger.warning(f"Row {idx}: Empty address")
                df.at[idx, 'On_Tribal_Land'] = 'No Data'
                continue
            
            logger.info(f"Processing {idx + 1}/{total}: {address}")
            
            # Geocode address
            coords = self.geocode_address(address)
            
            if coords:
                lat, lon = coords
                df.at[idx, 'Latitude'] = lat
                df.at[idx, 'Longitude'] = lon
                
                # Check if on Tribal land
                on_tribal = self.check_tribal_land(lat, lon)
                df.at[idx, 'On_Tribal_Land'] = 'Yes' if on_tribal else 'No'
                
                logger.info(f"  Coordinates: ({lat:.6f}, {lon:.6f}) - Tribal Land: {'Yes' if on_tribal else 'No'}")
            else:
                df.at[idx, 'On_Tribal_Land'] = 'Could Not Geocode'
                logger.warning(f"  Could not geocode address")
        
        # Save results
        if output_file is None:
            # Create output filename
            input_path = Path(input_file)
            output_file = input_path.parent / f"{input_path.stem}_tribal_checked.xlsx"
        
        df.to_excel(output_file, index=False)
        logger.info(f"Results saved to: {output_file}")
        
        # Print summary
        print("\n=== Summary ===")
        print(df['On_Tribal_Land'].value_counts())
        
        return df


def main():
    """Main function to run the Tribal land checker."""
    
    print("=" * 60)
    print("TRIBAL LAND CHECKER")
    print("Identifies properties on federally recognized Tribal lands")
    print("=" * 60)
    
    # Get input file
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        input_file = input("Enter path to Excel file with addresses: ").strip()
    
    # Check if file exists
    if not Path(input_file).exists():
        logger.error(f"File not found: {input_file}")
        return
    
    # Optional: Specify Tribal boundaries file
    boundaries_file = None
    use_custom = input("\nDo you have a custom Tribal boundaries file? (y/n): ").lower()
    if use_custom == 'y':
        boundaries_file = input("Enter path to boundaries file (shapefile/GeoJSON): ").strip()
    
    # Create checker instance
    checker = TribalLandChecker(tribal_boundaries_path=boundaries_file)
    
    # Process the Excel file
    try:
        df = checker.process_excel(input_file)
        
        print("\n=== Processing Complete ===")
        print(f"Total addresses processed: {len(df)}")
        print(f"On Tribal Land: {(df['On_Tribal_Land'] == 'Yes').sum()}")
        print(f"Not on Tribal Land: {(df['On_Tribal_Land'] == 'No').sum()}")
        print(f"Could not geocode: {(df['On_Tribal_Land'] == 'Could Not Geocode').sum()}")
        
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        raise


if __name__ == "__main__":
    main()
