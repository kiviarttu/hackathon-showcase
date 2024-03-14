
# Example R scripts

Three .R files can be found from this folder, two of which (climgrid) are a pair for the same workflow. 

## CLIMGRID
CLIMGRID is a series of gridded meteorological daily observations from Finland with several variables. The script downloads the annual netCDF files based on user request from a predefined s3 Amazon cloud storage and proceeds to extract daily values for each feature in a given vector dataset. This workflow does not utilise parallel processing.

## TRIMMER
trim_with_terra.R script was used in a digital peatland mapping project. Its purpose is to remove NoData values from the raster edges. It was used as a part in a country-wide Sentinel-2 mosaicking process. This script is parallelized in a simple way and was used in a HPC environment. 
