'''
A Python script for calculating remote sensing indices that are available on 10 m resolution. 
The script will be utilized in MaaTu project for calculating indices not provided from SYKE or FMI or to do calculations from 
different timeframe.

The script is originally built to be used after running mosaic_tile_loop.sh for the relevant tiles.

Author: Arttu Kivimäki (some index formulas from EODIE code by Samantha Wittke and Petteri Lehti [https://gitlab.com/fgi_nls/public/EODIE/-/blob/main/src/eodie/index.py])
'''
# Necessary imports
from cmath import sqrt
import rasterio 
import numpy as np
import os 
import sys
import argparse
import glob
import boto3
import dask 

# Initialize argument parser
parser = argparse.ArgumentParser()

# Add arguments
parser.add_argument(
    "--indices",
    dest = "indices",
    help = "Which indices should be calculated",
    nargs = "*",
    required = True
)

parser.add_argument(
    "--directories",
    dest = "directories",
    help = "Directories where the sen2mosaic products are stored for index calculation",
    required = True,
    nargs="*"
)

parser.add_argument(
    "--output",
    dest = "output",
    help = "Full path to preferred output directory. Defaults to current working directory.",
    default = os.getcwd()
)

parser.add_argument(
    "--allas",
    dest = "allas",
    action = "store_true",
    default = False,
    help = "Flag to determine if end products should be uploaded to Allas and removed from Puhti. Defaults to False."
)

# Parse arguments
args = parser.parse_args() 

# If allas input parameter was given, determine s3 client
if args.allas:
    s3 = boto3.client("s3", endpoint_url='https://a3s.fi')

# Check if length of indices is above 1
if len(args.indices) > 1: 
    # Indices were given directly instead of a text file
    indices = args.indices 
# If length is one, check if a file was given
else: 
    if os.path.isfile(args.indices[0]):
    # Read the values from the text file into a list
        with open(args.indices[0], "r") as file:
            indices = file.read().lower().splitlines()
    else:
        indices = args.indices 

# Check if folders are given as a text file
if len(args.directories) > 1:
    # Directories were given directly instead of a text file
    folders = args.directories
else:    
    if os.path.isfile(args.directories[0]):
        print("Directories were given as a file!")
        # Read the values from the text file into a list
        with open(args.directories[0], "r") as file:
            folders = file.read().splitlines()             
    else:
        folders = args.directories 

# Define function get_band
def get_band(folder, number, resolution = 10):
    ''' Finds correct tif from folder, reads it with rasterio and converts the values to reflectance values.'''
    # Define pattern to look for
    pattern = "*_R{}m_*B*{}.tif".format(resolution, number)
    path = os.path.join(folder, pattern)
    # Use glob to search for the pattern and take the first (and presumably only) value of the output list
    tif = glob.glob(path)[0]
    # Read tif with rasterio
    with rasterio.open(tif) as band:
        band_array = np.array(band.read(1))
    # Convert digital numbers to reflectance values
    band_array = band_array.astype('f4')/10000
    
    # Return numpy array
    return band_array

# Define function get_profile
def get_profile(folder, resolution = 10):
    '''Reads an example profile of the red band (band number 4) to be used as a baseline for raster saving.'''
    # Define pattern to look for
    pattern = "*_R{}m_B04.tif".format(resolution)
    path = os.path.join(folder, pattern)
    # Use glob to search for the pattern
    profiletif = glob.glob(path)[0]
    # Read profiletif with rasterio 
    band = rasterio.open(profiletif)
    # Store profile into varibale
    profile = band.profile
    # Return profile
    return profile

# Defining the functions for calculating different indices. Most indices from EODIE source code are included. 



def calc_ndvi():
    '''Calculates the traditional NDVI with bands 4 and 8 [insert a reference]'''
    # Read bands 4 and 8
    resolution = 10
    red = get_band(folder, 4)
    nir = get_band(folder, 8)
    # Do the index calculation
    ndvi = np.divide((nir-red), (nir+red))
    # Return ndvi
    return ndvi, resolution


def calc_ndwi():
    '''Calculates the NDWI (McFeeters 1996) with bands 3 and 8 '''
    # Read bands 3 and 8
    resolution = 10
    nir = get_band(folder, 8)
    green = get_band(folder, 3)
    # Do the index calculation
    ndwi = np.divide((green-nir), (green+nir))
    # Return ndwi
    return ndwi, resolution



def calc_savi():
    '''Calculates the Soil-Adjusted Vegetation Index [Huete 1986] with bands 4 and 8 and with L value of 0.5'''
    # Read bands 4 and 8
    resolution = 10
    nir = get_band(folder, 8)
    red = get_band(folder, 4)    
    # Do the index calculation
    savi = np.divide(1.5*(nir-red),nir+red+0.5)
    # Return array
    return savi, resolution

def calc_ndmi():
    '''Calculates the normalized difference moisture index based on Gao (1996).'''
    resolution = 20
    # Read bands nir and swir1
    nir = get_band(folder, "8A", resolution)
    swir1 = get_band(folder, 11, resolution)
    
    # Do the index calculation
    ndmi = np.divide((nir-swir1), (nir+swir1))
    # Return array
    return ndmi, resolution

def calc_tctb():
    ''' Calculates the Tasseled-Cap brightness index based on ???'''
    resolution = 20
    # Read required bands
    blue = get_band(folder, 2, resolution)
    green = get_band(folder, 3, resolution)
    red = get_band(folder, 4, resolution)
    nir = get_band(folder, "8A", resolution)
    swir1 = get_band(folder, 11, resolution)
    swir2 = get_band(folder, 12, resolution)

    # Calculate the weighted sum of bands. These coefficients only work with Sentinel-2! 
    tctb = 0.3510 * blue + 0.3813 * green + 0.3437 * red + 0.7196 * nir + 0.2396 * swir1 + 0.1949 * swir2 

    return tctb, resolution

def calc_tctg():
    ''' Calculates the Tasseled-Cap greenness index'''
    resolution = 20
    # Read required bands
    blue = get_band(folder, 2, resolution)
    green = get_band(folder, 3, resolution)
    red = get_band(folder, 4, resolution)
    nir = get_band(folder, "8A", resolution)
    swir1 = get_band(folder, 11, resolution)
    swir2 = get_band(folder, 12, resolution)

    # Calculate the weighted sum of bands. These coefficients only work with Sentinel-2!
    tctg = -0.3599 * blue + (-0.3533) * green + (-0.4734) * red + 0.6633 * nir + 0.0087 * swir1 + (-0.2856) * swir2

    return tctg, resolution

def calc_tctw():
    ''' Calculates the Tasseled-Cap wetness index'''
    resolution = 20
    # Read required bands
    blue = get_band(folder, 2, resolution)
    green = get_band(folder, 3, resolution)
    red = get_band(folder, 4, resolution)
    nir = get_band(folder, "8A", resolution)
    swir1 = get_band(folder, 11, resolution)
    swir2 = get_band(folder, 12, resolution)    

    # Calculate the weighted sum of bands. These coefficients only work with Sentinel-2!
    tctw =  0.2578 * blue + 0.2305 * green + 0.0883 * red + 0.1071 * nir + (-0.7611) * swir1 + (-0.5308) * swir2

    # Return array
    return tctw, resolution

def calc_rvi():
    ''' Calculates the Ratio Vegetation Index based on '''
    resolution = 10
    # Read required bands
    red = get_band(folder, 4)
    nir = get_band(folder, 8)

    # Do the index calculation
    rvi = np.divide(nir, red)
    # Return array
    return rvi, resolution

def calc_kndvi():
    ''' Calculates the Kernel Normalized Difference Vegetation index based on'''
    resolution = 10
    # Read required bands
    red = get_band(folder, 4)
    nir = get_band(folder, 8)

    # Pixelwise sigma calculation (formula from EODIE source code)
    sigma = 0.5*(nir + red)
    knr = np.exp(-(nir-red)**2/(2*sigma**2))

    # Do the index calculation
    kndvi = np.divide(1-knr, 1+knr)
    # Return array
    return kndvi, resolution

def calc_mndwi():
    ''' Calculates the modified Normalized Difference Wetness Index'''
    resolution = 20
    # Read required bands
    green = get_band(folder, 3, resolution)
    swir1 = get_band(folder, 11, resolution)

    # Do the index calculation
    mndwi = np.divide(green-swir1, green+swir1)
    # Return array
    return mndwi, resolution

def calc_evi():
    ''' Calculates the enhanced vegetation index'''
    resolution = 10
    # Read required bands 
    nir = get_band(folder, 8)
    red = get_band(folder, 4)
    blue = get_band(folder, 2)

    # Determine variables included in formula
    L = 1
    C1 = 6
    C2 = 7.5
    G = 2.5

    # Do the index calculation (formula from EODIE source code)
    num = nir - red
    denom = nir + C1 * red - C2 * blue + L
    evi = G * np.divide(num, denom)
    # Return array
    return evi, resolution

def calc_evi2():
    ''' Calculates the enhanced vegetation index 2 based on '''
    resolution = 10
    # Read the required bands
    nir = get_band(folder, 8)
    red = get_band(folder, 4)
    
    # Determine variables included in formula
    L = 1
    C = 2.4
    G = 2.5

    # Do the index calculation (formula from EODIE source code)
    num = nir-red
    denom = np.multiply(C, red) + nir + L
    evi2 = G * np.divide(num, denom)
    # Return array
    return evi2, resolution

def calc_dvi():
    ''' Calculates the difference vegetation index '''
    resolution = 10
    # Read the required bands
    nir = get_band(folder, 8)
    red = get_band(folder, 4)

    # Do the index calculation
    dvi = nir - red
    # Return array
    return dvi, resolution

def calc_cvi():
    ''' Calculates the chlorophyll vegetation index '''
    resolution = 10
    # Read the required bands
    nir = get_band(folder, 8)
    red = get_band(folder, 4)
    green = get_band(folder, 3)
    
    # Do the index calculation (formula from EODIE source code)
    cvi = np.divide(np.multiply(nir, red), green**2)
    # Return array
    return cvi, resolution

def calc_ndsi():
    '''Calculates the Normalized Difference Snow Index'''
    resolution = 20
    # Read the required bands
    green = get_band(folder, 3, 20)
    swir1 = get_band(folder, 11, 20)

    # Do the index calculation
    ndsi = np.divide((green-swir1), (green+swir1))
    # Return array
    return ndsi, resolution

def calc_nbr():
    '''Calculates the Normalized Burn Ratio'''
    resolution = 20
    # Read the required bands
    nir = get_band(folder, "8A", 20)
    swir2 = get_band(folder, 12, 20)

    # Do the index calculation
    nbr = np.divide((nir-swir2), (nir+swir2))
    # Return array
    return nbr, resolution

def calc_sci():
    '''Calculates the Soil Colour Index'''
    resolution = 10
    # Read the required bands
    nir = get_band(folder, 8)
    red = get_band(folder, 4)
    green = get_band(folder, 3)
    blue = get_band(folder, 2)

    # Do the index calculation
    sci = 3 * nir + red - green - 3 * blue
    # Return array
    return sci, resolution

def calc_sm():
    '''Calculates soil moisture index'''
    resolution = 10
    # Read the required bands
    nir = get_band(folder, 8)
    blue = get_band(folder, 2)

    # Do the index calculation
    sm = np.divide(nir, blue)
    # Return array
    return sm, resolution 
    
# Define function get_filename
def get_filename(folder):
    '''Extracts the filename base by removing the file extension tif and the band name from the end of the inputs'''
    # List tifs in folder
    tifs = os.listdir(folder)
    # Choose the first tif and cut the 8 last characters from it (works directly with sen2mosaic output files)
    filename = tifs[0][0:len(tifs[0])-13]
    timeframe = tifs[0][6:23]
    # Return filename
    return filename, timeframe

# Define function save_raster
def save_raster(array, index, folder, resolution = 10, allas = False):
    '''Saves the raster with the given index name by editing the original profile of the red band (number 4)'''
    # Run get_filename to get the baseline
    filename, timeframe = get_filename(folder)
    # Build output name based on filename
    output_name = "{}_{}.tif".format(filename, index)
    # Build outputpath for given index    
    indexpath = os.path.join(args.output, index)
    # Check if outputpath exists for given index; if not, create
    if not os.path.isdir(indexpath):
        os.mkdir(indexpath)        
    # Add output path 
    outputpath = os.path.join(indexpath, output_name)
    # Update profile status
    with rasterio.Env():
        # Read profile of the red band
        profile = get_profile(folder, resolution)
        # Update profile
        profile.update(
            # Datatype float32
            dtype=rasterio.float32,
            count=1,
            # Set compression
            compress="lzw"
        )
    # Save the raster
    with rasterio.open(outputpath, "w", **profile) as dst:
        dst.write(array.astype(rasterio.float32),1)

    # If flag --allas is given, the file will be uploaded to Allas and removed from Puhti scratch.
    if allas:
        # Build path for allas upload
        allaspath = "{}/{}/{}".format(index, timeframe, output_name)
        # Use boto3 for uploading the file
        s3.upload_file(outputpath, "mosaicbucket", allaspath)
        # Remove file from Puhti
        os.remove(outputpath)

    return None

def extract_index(folder, index, allas=False):
    """Calculates and saves the requested index from each folder."""
    # Set numpy errors to be ignored within dask instances
    np.seterr(divide="ignore", invalid="ignore")        
    # Build function call based on index given
    function_call = "calc_" + index 
    # Execute function call and calculate required index
    output, resolution = globals()[function_call]()
    # Save raster (either to Puhti or to Allas)
    save_raster(output, index, folder, resolution, allas)
    # No returns
    return None

# Create list of delayed functions
list_of_delayed_functions = []

print("Preparing index computations...")
# Loop through folders
for folder in folders:
    # Loop through indices
    for index in indices:
        # Build dask.delayed command 
        result = dask.delayed(extract_index)(folder, index, args.allas)
        # Append the list of delayed functions
        list_of_delayed_functions.append(result)

# Execute delayed computations
print("Executing index computations...")
dask.compute(list_of_delayed_functions)
print("Processing completed!")

