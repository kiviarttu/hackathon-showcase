#!/bin/bash

inputrasters=$1
outputname=$2

# Load geoconda
module load geoconda

python move_files.py --files ${inputrasters} --model ${outputname}

# Create directory where to copy files
datadir="/scratch/project_2005231/Arttu/indexmaps/${outputname}/"
mkdir -p ${datadir}

# Define path to maamaski
basegridpath="/scratch/project_2005231/maskit/maamaski_50m_Fin_laaja.tif"

# Define text file into the names of files to extend will be added
extendfiles="/projappl/project_2005231/RS_code/indexmaps/files_to_extend.txt"
uniquefiles="/projappl/project_2005231/RS_code/indexmaps/unique_files.txt"

# Use gdalinfo to get extent of maamaski
basegridsize=$(gdalinfo -json ${basegridpath} | jq -r .size)

# Loop over textfile containing input rasters to check whether their cell grids match the base grid. 
# This is important as the array stacks cannot be done with rasters of different sized dimensions.

echo "Comparing input raster extents to base grid extent..."
ls -d ${datadir}/*.* > ${uniquefiles}
while IFS= read -r raster
do
    
    gridsize=$(gdalinfo -json ${raster} | jq -r .size)
    
    if ! [ "$gridsize" == "$basegridsize" ]; then
    
        # Add name of raster to the list of files to extend
        echo ${raster} >> ${extendfiles}
        
    fi

done < ${uniquefiles}

rm ${uniquefiles}

# Check if list of files to extend exists
if [ -f ${extendfiles} ]; then

    echo "Files that need extending where found! Extending..."
    # Unload geoconda to allow R scripts
    module unload geoconda
    
    # Load r-env
    module load r-env
    
    # Run Rscript
    Rscript extend_files.R --rasters ${extendfiles} --output ${datadir}
    
    # Remove list of files
    rm ${extendfiles}
    
    # Unload r-env to avoid conflicts with geoconda
    module unload r-env
    
    # Load geoconda
    module load geoconda
    
else
    echo "No files in need of extension were found!"
    
fi 

# Run Python script that compares the rasters to basegrid

python build_index_map.py --rasterdir ${datadir} --outputname ${outputname}

echo "Indexmap built for ${outputname}!"

rm -rf ${datadir}






























