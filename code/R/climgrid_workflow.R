#### WORKFLOW SCRIPT FOR DOWNLOADING AND PROCESSING CLIMGRID DATA ###
#### Author: Arttu Kivimäki | FGI-CHADE | May 2023 
#### Requires an interactive session or a batch job in Puhti! R scripts do not run (properly) in login nodes. 
#### Use Rscript climgrid_workflow.R --help for instructions.

suppressPackageStartupMessages(library(here, quietly = T))

# Source files containing functions
source(paste0(here(), "/functions_climgrid_workflow.R"))

# Read user arguments.
args <- validate_inputs(read_inputs())

# Fetch data from the public Allas bucket containing CLIMGRID data
print("Fetching file information from Allas...")
allas_data <- fetch_allas_data("s3://clim_grid/")

# Build timeframe 
timeframe <- get_timeframe(args$startyear, args$endyear)

# Filter data based on variable names and timeframe
print("Filtering data based on input criteria...")
allas_data <- filter_data(allas_data, args$variables, timeframe)

# Download data from Allas to local directory
print("Starting to download data...")
download_data("s3://clim_grid/", allas_data, args$ncdir)

# Extract values from rasters based on vector points
print("All files downloaded! Starting to extract values...")
extract_values(args$ncdir, allas_data$Filename, args$vectorfile, args$outputdir, args$vectorname, args$delete)
print("All values extracted! Workflow completed.")










