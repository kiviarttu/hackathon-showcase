#!/bin/bash
#SBATCH --job-name=Virtual_raster_pipeline
#SBATCH --account=project_2005231
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=5
#SBATCH --mem=35G
#SBATCH --time=5:00:00
#SBATCH --partition=small
#SBATCH --output=/scratch/project_2005231/Sentinel-2/Slurm_outputs/%x_%j.out

##################################################################
# A PIPELINE SCRIPT FOR MAATU PROJECT 
# 1. CALCULATING WANTED INDICES FROM INDIVIDUAL TILES
# 2. REPROJECTING INDEX TIFS TO EPSG:3067
# 3. TRIMMING NO DATA VALUES AND RESAMPLING TO 50m X 50m RESOLUTION 
# 4. MERGING THE INDIVIDUAL TILES INTO A LARGER MOSAIC THROUGH VIRTUAL RASTER
# 5. UPLOADING THE FINAL PRODUCT TO ALLAS
# 6. REMOVING THE FILES CREATED IN THE PROCESS
# Author: @kiviarttu - February 2022
# Edited: @kiviarttu - December 2022

# Usage: sbatch virtual_pipeline.sh full/path/to/folders.txt full/path/to/indices.txt timeframe(YYYYMMDD-YYYYMMDD)

# Store command line arguments into variables
dirs=$1
indices=$2
timeframe=$3

# Load geoconda
module load geoconda

echo "******************************"
echo "STARTING TO CALCULATE INDICES!"
echo "******************************"

# Run index_calculator.py
python index_calculator.py $dirs $indices

# Print a status update
echo "***************************"
echo "INDEX CALCULATION COMPLETE!"
echo "***************************"
# Read the indices from text file and move the files to index-based directories
while IFS= read -r tiledir; do  
    while IFS= read -r index; do
        # Create a new directory with the index
        mkdir -p /scratch/project_2005231/Sentinel-2/Mosaic_results_$timeframe/Pipeline/$index
        # Find the file matching keyword
        filename=$(ls $tiledir/*$index.tif)    
        # Cut the filename to exclude directory structure
        slashes=$(echo $filename | tr -dc '/' | wc -m)        
        cutname=$(echo $filename | cut -d '/' -f $slashes)
        # Move the file to the new location
        mv $filename /scratch/project_2005231/Sentinel-2/Mosaic_results_$timeframe/Pipeline/$index/$cutname
    done < $indices
done < $dirs

echo "*************************"
echo "FILE MANAGEMENT COMPLETE!"
echo "*************************"
# ALL OF THE FOLLOWING NEEDS TO BE DONE SEPARATELY FOR EACH INDEX FOLDER!

# Determine basepath in which the index folders are

basepath=/scratch/project_2005231/Sentinel-2/Mosaic_results_$timeframe/Pipeline

# Read through indices again to loop through index directories
for index in $(cat $indices); do
    echo "***************************"
    echo "STARTING TO PROCESS $index!"
    echo "***************************"
    # Run projection_converter.py to reproject the tifs to EPSG:3067
    python projection_converter.py $basepath/$index
 	
    echo "******************************************"
    echo "PROJECTION CONVERSION FOR $index COMPLETE!"
    echo "******************************************"
    
    # Unload geoconda and load r-env-singularity (they clash in Puhti)
    module unload geoconda
    module load r-env-singularity
    # Trim the nodata values from tile edges and resample the tiles to match 50 m mask
    srun singularity_wrapper exec Rscript --no-save Puhti_Resample.R $basepath/$index/Reprojected/
    # Unload r-env-singularity and load geoconda
    module unload r-env-singularity
    module load geoconda

    # Print a status update
    echo "********************************************"
    echo "TRIMMING AND RESAMPLING FOR $index COMPLETE!"
    echo "********************************************"


    # Remove the pre-used .tif files from parent directories
    rm $basepath/$index/*.tif
    
    # Create a virtual raster of the tiles
    gdalbuildvrt $basepath/$index/$index-$timeframe.vrt $basepath/$index/Reprojected/Resampled/*.tif
    #Translate the virtual raster into a tif
    gdal_translate -of GTiff $basepath/$index/$index-$timeframe.vrt $basepath/$index/$index-$timeframe.tif
    # Upload the final product to Allas
    #s3cmd put $basepath/$index/$index-$timeframe.tif s3://pilottikohde-pohjanmaa/selittavat_muuttujat/Sentinel2/Indeksimosaiikit/2021/$index/$index-$timeframe.tif
    # Move the final product out from processing directories
    mkdir /scratch/project_2005231/Sentinel-2/Pilotti/$index
    mv $basepath/$index/$index-$timeframe.tif /scratch/project_2005231/Sentinel-2/Pilotti/$index/$index-$timeframe.tif
done

# Remove the files created in the process
rm -rf /scratch/project_2005231/Sentinel-2/Mosaic_results_$timeframe/

echo "******************"
echo "ALL FILES REMOVED!"
echo "******************"

