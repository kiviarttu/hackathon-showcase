# A script for

# 1. Querying wanted Sentinel-2 buckets from LUKE's Allas storage (managed by Maria Yli-Heikkilä [maria.yli-heikkila@luke.fi]). 
# Query is based on: tilenames, year and month.
# 2. Dowloading queried products. 
# 3. Creating a mosaic.

# Before running, you must have s3cmd installed and configured to LUKE's Allas project.

year=$1
months=$2

# List all buckets in LUKE's Allas and store the names into a text file with awk:
s3cmd ls | awk '{ print $3 }' > buckets.txt
# Filter segment bucket names to be excluded.
grep -v 'segments' buckets.txt > no_segments.txt
# Remove buckets.txt
rm buckets.txt
# Filter only relevant tiles:
grep -f tiles.txt no_segments.txt > tilebuckets.txt
# Remove no_segments.txt
rm no_segments.txt
# Filter only years given by user:
grep "$year" tilebuckets.txt > yearbuckets.txt
# Remove tilebuckets.txt
rm tilebuckets.txt
# Go through tilebuckets.txt to see all the folders within the buckets.
cat yearbuckets.txt | while IFS= read -r line
do  
    # Store relevant information into a text file.
    s3cmd ls $line *.SAFE | awk '{ print $2 }' >> folders.txt 
done
# Remove yearbuckets.txt
rm yearbuckets.txt
# Remove rows that don't have s3:// in them
grep 's3://' folders.txt > folders_cleaned.txt
# Remove folders.txt
rm folders.txt

# From the remaining SAFE folders, only select for download that match user-given parameters 
# of year and months.  

grep "$year[0][$months]" folders_cleaned.txt > folders_download.txt

# Remove folders_cleaned.txt
rm folders_cleaned.txt


# Download SAFE-files in folders.txt
cat folders_download.txt | while IFS= read -r line
do  
    dldir="mosaic_download/"
    safedir=$(echo $line | cut -c 46-)
    dlpath=$dldir$safedir
    mkdir -p "$dlpath"
    s3cmd get --recursive $line $dlpath
    echo "$line downloaded!"
done
