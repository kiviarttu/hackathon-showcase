# Load terra
library(terra)
library(parallel)
library(MASS)

# Read arguments
args <- commandArgs(trailingOnly=TRUE)

# Define pattern
pattern <- "*.tif$"

# Read path from arguments
tifpath <- args[1]

# List files recursively with full names
tifs <- list.files(path = tifpath, pattern = pattern, recursive = TRUE, full.names = TRUE)

# Define path for maamaski
maskpath <- "/scratch/project_2005231/Sentinel-2/maskit/maamaski/maamaski_50m_Fin_laaja.tif"

# Read maamaski into a SpatRaster
maamaski <- rast(maskpath)

# Define function trim_and_resample
trim_and_resample <- function(filepath) {
  # Read raster into an object
  to_reproject <- terra::rast(filepath) 
  # Reproject raster to epsg:3067
  to_trim <- terra::project(to_reproject, "EPSG:3067")
  # Trim nodata values from the edges
  to_resample <- terra::trim(to_trim)
  # Crop maamaski based on tile
  maamaski_cropped <- terra::crop(maamaski, to_resample)
  # Resample trimmed raster with maamaski
  resample(to_resample, maamaski_cropped, method = "bilinear", filename = filepath, overwrite = TRUE)
}

# Get starting time of script
starttime <- Sys.time()
# Run trim_and_resample function in parallel with 5 cores
print("Executing trim_and_resample function for all tifs...")
resampled <- mclapply(tifs, trim_and_resample, mc.cores = 5)
print("Processing ompleted.")
# Get ending time of script
endtime <- Sys.time()
# Print duration
endtime - starttime
# Print a status update
print("Processing complete.")