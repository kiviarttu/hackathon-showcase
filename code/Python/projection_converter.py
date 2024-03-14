'''
# Script for EPSG conversion from UTM bands to ETRS-TM35FIN (EPSG:3067).
# To be ran after mosaicking/index calculation.

# Usage: python coordinate_converter.py full/path/to/folder
# Author: @kiviarttu

'''
# -*- coding: utf-8 -*-
# Necessary imports
import argparse 
import os
import rasterio
import sys
from rasterio.crs import CRS
from osgeo import gdal
import dask 
import glob

# Initialize argument parser
parser = argparse.ArgumentParser()

# Add arguments for parser

# Directory for tifs to reproject
parser.add_argument(
    "--dir",
    dest = "dir",
    help = "Full path to the directory where tifs to reproject are.",
    required = True, 
)

# Flag whether original files should be overwritten or if stored into another directory
parser.add_argument(
    "--overwrite",
    dest = "overwrite",
    action = "store_true",
    help = "Flag to indicate that original files should be overwritten."
)

# Output path in case overwriting is not preferred.
parser.add_argument(
    "--output",
    dest = "output",
    help = "Full path where the reprojected tifs should be written. Defaults to subdirectory 'Reprojected' of --dir.",
    default = None
)

parser.add_argument(
    "--epsg",
    dest = "epsg",
    help = "EPSG code to which the tifs should be reprojected.",
    required = True
)

# Define function reproject 
def reproject(tifpath, epsg, outpath):
    # Extract filename from tifpath
    filename = os.path.basename(tifpath)
    # Build outpath
    output = os.path.join(outpath, filename)
    # Build EPSG string
    dstSRS = "EPSG:{}".format(epsg)
    # Read the tif with gdal
    input_tif = gdal.Open(tifpath)
    # Reproject 
    gdal.Warp(output, input_tif, dstSRS=dstSRS)
    return None


# Read arguments
args = parser.parse_args()

# Build search pattern for glob
pattern = os.path.join("{}".format(args.dir), "*.tif")

# Glob all the tifs in given directory
tifs = glob.glob(pattern)
if not tifs:
    quit("No tifs were found in {}, please check your inputs.".format(args.dir))
    
print("All tif files in --dir listed!")

# Check if overwrite flag was given
if args.overwrite:
    # If true, use input directory as output path (and overwrite the current files)
    output = args.dir
else: 
    # If false, check if outputpath was given:
    if args.output:
        # Define output with argument
        output = args.output
        # Check that output path exists
        if not os.path.isdir(output):   
            # Create if it does not exist
            os.mkdir(output)
    else: 
        # Create output directory to input directory
        output = os.path.join(args.dir, "Reprojected")
        # Check that output path exists
        if not os.path.isdir(output):
            # Create if it does not exist
            os.mkdir(output)

print("Output path determined!")

# Create an empty list for dask
list_of_delayed_functions = []

# Loop over globbed tifs
for tif in tifs:
    # Build function call for each of the tifs
    functioncall = dask.delayed(reproject)(tif, args.epsg, output)
    # Append the list of delayed functions
    list_of_delayed_functions.append(functioncall)

print("Executing reprojections...")
# Process everything with dask
dask.compute(list_of_delayed_functions)

# Finalize the script
print("Processing completed!")

