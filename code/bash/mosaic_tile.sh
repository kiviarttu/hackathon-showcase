#!/bin/bash
#SBATCH --account=project_2005231
#SBATCH --partition=small
#SBATCH --time=04:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=2G
#SBATCH --output=/scratch/project_2005231/Sentinel-2/Slurm_outputs/Mosaic_and_index_%J_%x.out

# Script for finding the tile with less clouds from a time series of Sentinel-2 imaging tile. 
# Store command line parameters into variables.
datadir=$1
outputdir=$2
xmin=$3
ymin=$4
xmax=$5
ymax=$6
timeframes=$7
UTM=$8
tile=$9

# Loading sen2mosaic module.
module load sen2mosaic
# Loading geoconda module.
# module load geoconda
# Loop over timeframes
while IFS= read -r timeframe; do 
    echo ""
    echo "Processing ${timeframe}"
    # Cut timeframe into starttime and endtime
    starttime=$(echo ${timeframe} | cut -d "-" -f 1)
    endtime=$(echo ${timeframe} | cut -d "-" -f 2)

    # Create output directory for this particular timeframe
    outpath=${outputdir}/${timeframe}
    mkdir -p ${outpath}

    # Determine the output name based on tilecode and timeframe.
    outputname=${tile}-${starttime}-${endtime}

    # Running sen2mosaic for both 10 and 20 meter resolutions
    s2m_mosaic ${datadir} -te ${xmin} ${ymin} ${xmax} ${ymax} -res 10 -e 326${UTM} -v -p 8 -st ${starttime} -en ${endtime} -n ${outputname} -o ${outpath}
    echo ""
    s2m_mosaic ${datadir} -te ${xmin} ${ymin} ${xmax} ${ymax} -res 20 -e 326${UTM} -v -p 8 -st ${starttime} -en ${endtime} -n ${outputname} -o ${outpath}
    echo ""

    python index_calculator.py --indices nbr ndmi ndsi ndvi sci sm kndvi tctg tctb tctw evi2 savi --directories ${outpath} --output /scratch/project_2005231/Sentinel-2/Mosaic/ --allas
done <${timeframes}



# After processing is done, we can remove the data.

echo "Removing input data... COMMENTED OFF"
#rm -r ${datadir}
echo "Input data removed!"







