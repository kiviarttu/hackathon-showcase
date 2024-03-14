#!/bin/bash
#SBATCH --job-name=mosaic_tile_loop
#SBATCH --account=project_2005231
#SBATCH --partition=small
#SBATCH --time=15:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=50G
#SBATCH --output=/scratch/project_2005231/Sentinel-2/Slurm_outputs/%x_%j.out

# A pipeline script for
# 1. The necessary data management pre- and postprocessing
# 2. Finding the correct coordinates for tiles
# 3. Mosaicking a single Sentinel-2 -tile based on time window.
# Usage: bash mosaic_tile_pipeline.sh path/to/SAFEs path/to/tilecodes.txt path/to/timeframes resolution
# where
# path/to/SAFEs = full path to the location of all SAFE folders
# tilecode = code for the imaging tile (f. ex. T34WDB)
# starttime = beginning of the timeframe in format YYYYMMDD
# endtime = ending of the timeframe in format YYYYMMDD
# resolution = 10, 20 or 60 - determines on which resolution bands will be mosaicked

# Store command line arguments into variables

dirofsafes=$1
tilecodes=$2
timeframes=$3
resolution=$4

# Loop through timeframes (in format YYYYMMDD-YYYYMMDD)
while IFS= read -r timeframe; do

	# Cut the starting and ending period from the timeframe
	starttime=$(echo $timeframe | cut -d "-" -f 1)
	endtime=$(echo $timeframe | cut -d "-" -f 2)

	# Read through tilecodes and start processing:
	while IFS= read -r tilecode
	do

		echo 'Creating folder.'
		mkdir $tilecode
		# Move relevant SAFEs to another directory to shorten sen2mosaic processing time.
		echo 'Moving files.'

		mv $dirofsafes/*$tilecode*.SAFE $tilecode/

		# Find which folder is the biggest in size (to avoid reading the spatial extent from a partial image)
		echo 'Finding largest SAFE folder.'
		largest=$(du -h --max-depth=1 $tilecode | sort -hr | tail -n +2 | head -n 1)
		tilepath=$(echo $largest | cut -d " " -f 2)

		# Store the path to manifest.safe into a variable

		manifest=$tilepath/manifest.safe

		# Use get_boundingbox.py to find the min and max coordinates.
		echo 'Running get_boundingbox.py.'
		module load geoconda

		boundingbox=$(python get_boundingbox.py $manifest)
		module unload geoconda

		# Create an array from the min/max values with comma as 

		arrBB=(${boundingbox//,/ })

		xmin=${arrBB[0]}
		ymin=${arrBB[1]}
		xmax=${arrBB[2]}
		ymax=${arrBB[3]}

		echo 'Bounding box is' $xmin $ymin $xmax $ymax

		# Define the sbatch name
		sbatchname=mosaic-$tilecode-$starttime-$endtime
		

		echo 'Sending batch job'.
		# Send the batch job.
		
		sbatch -J $sbatchname mosaic_tile.sh $tilecode $starttime $endtime $xmin $ymin $xmax $ymax $resolution

	done <$tilecodes

	# Wait for some time for the mosaicking process to be ready (with 60 m resolution 1 hour should be enough unless Puhti is under heavy traffic)
	now=$(date +"%T")
	echo All jobs sent, time is $now, sleeping 10 minutes. 
	sleep 10m

	# Change working directory
	cd /scratch/project_2005231/Sentinel-2/maatu

	# List folders in mosaic_results into a text file
	ls -d /scratch/project_2005231/Sentinel-2/Mosaic_results_$timeframe/*/ > /scratch/project_2005231/Sentinel-2/tiledirs_$timeframe.txt

	# Run virtual pipeline
	sbatch virtual_pipeline.sh /scratch/project_2005231/Sentinel-2/tiledirs_$timeframe.txt /scratch/project_2005231/Sentinel-2/indices.txt $timeframe

	# Wait again for some time again to let the virtual_pipeline process finish (1 hour)

	# Change working directory back to origins
	cd /scratch/project_2002694/2021

	# Go back up
done < $timeframes