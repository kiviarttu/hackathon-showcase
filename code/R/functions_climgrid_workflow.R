library(argparse)
library(aws.s3)
library(dplyr, quietly = TRUE, warn.conflicts = FALSE)
library(terra, quietly = TRUE)

# Set AWS endpoint to Allas
Sys.setenv("AWS_S3_ENDPOINT" = "a3s.fi")

read_inputs <- function() {
  # Read user inputs with argparse library.
  # Returns: parsed arguments
 
  # Initialize argument parser
  parser <- argparse::ArgumentParser()
  
  # Add arguments
  parser$add_argument("--ncdir",
                      dest = "ncdir",
                      required = TRUE,
                      help = "Full path to the directory where netCDF files should be stored."
  )
  
  parser$add_argument("--outputdir",
                      dest = "outputdir",
                      default = NULL,
                      help = "Full path to the directory where the resulting csv files should be written. Defaults to ncdir."
  )
  
  parser$add_argument("--delete",
                      dest = "delete",
                      action = "store_true",
                      default = FALSE,
                      help = "Boolean indicator whether the netCDF files should be removed after raster extraction. Defaults to FALSE."
                       
  )
  
  parser$add_argument("--variables",
                      dest = "variables",
                      default = c("globrad", "psea", "rh", "rrday", "snow", "tday", "tgmin", "tmin", "tmax"),
                      nargs = "*",
                      help = "CLIMGRID variables to be downloaded and extracted."
  )
  
  parser$add_argument("--startyear",
                      dest = "startyear",
                      required = TRUE,
                      help = "Starting year for the timeframe of interest in format YYYY. No default value."
  )
  
  parser$add_argument("--endyear",
                      dest = "endyear",
                      nargs = 1,
                      default = NULL,
                      help = "Ending year for the timeframe of interest in format YYYY. No default value."
  )
  
  parser$add_argument("--vectorfile",
                      dest = "vectorname",
                      default = NULL,
                      required = TRUE,
                      help = "Full path to the vectorfile."
  )
  
  parser$add_argument("--idfield", 
                      dest = "id",#
                      required = TRUE,
                      help = "The name of unique identifier field in vectorfile."
  )
  
  parser$add_argument("--bucket_name",
                      dest = "bucketname",
                      default = "s3://clim_grid/",
                      help = "Define the bucket name where you wish to download CLIMGRID data from. Defaults to the public bucket s3://clim_grid/. Please note that you need to make your bucket public to access it with this script.")
  
  

  # Parse arguments
  inputs <- parser$parse_args()
  print("Reading and validating user inputs...")
  # Return parsed arguments
  return(inputs)
}

validate_inputs <- function(inputs) {
  # Validate user inputs.
  # Arguments:
  # inputs = user-defined inputs
  
  # Returns:
  # inputs = validated user inputs
  # If validation fails, execution will halt.
  
  # Check that ncdir exists and create if needed
  if (!dir.exists(inputs$ncdir)) {
    dir.create(path = inputs$ncdir, recursive = TRUE)
  }
  
  # Check if outputdir was given and assign to ncdir if not
  if (is.null(inputs$outputdir)) {
    inputs$outputdir <- inputs$ncdir
  # Check that outputdir exists and create if needed
  } else if (!dir.exists(inputs$outputdir)) {
    dir.create(path = inputs$outputdir, recursive = TRUE)
  }
  
  # Check that given variables are valid
  climgrid_vars <- c("globrad", "psea", "rh", "rrday", "snow", "tday", "tgmin", "tmin", "tmax")
  
  # Convert all variables to lowercase
  inputs$variables <- base::tolower(inputs$variables)
  # Remove variables that cannot be found from climgrid_vars
  inputs$variables <- inputs$variables[inputs$variables %in% climgrid_vars]
  
  # Check that at least 1 CLIMGRID variable remains
  if (! length(inputs$variables) > 0) {
    stop("No valid CLIMGRID variables given. Please check your inputs.", call. = FALSE)
  }
  
  # If end year was given, check that it is later than start year.
  if (!is.null(inputs$endyear)) {
    if (inputs$endyear < inputs$startyear) {
      stop("Given endyear is smaller than startyear. Please check your inputs.")
    }
  }
  
  # Check that start year is at least 1961
  if (inputs$startyear < 1961) {
    stop("Given startyear is smaller than 1961. Please check your inputs.")
  } else if (as.integer(format(Sys.Date(), "%Y")) < inputs$startyear) {
    stop("Given startyear is bigger than current year. Please check your inputs.")
  }
  
  # Check that vector file exists
  if (! file.exists(inputs$vectorname)) {
    stop("Given vectorfile does not exist! Please check your inputs.")
  } else {
    inputs$vectorfile <- read_vector(inputs$vectorname)
    
    # Check that vector file contains given ID field.
    if (! inputs$id %in% names(inputs$vectorfile)) {
      stop("Vectorfile does not include given ID field. Please check your inputs.")
    }
    
    # Filter out unnecessary columns (only ID field and geometry remain)
    inputs$vectorfile <- filter_vector(inputs$vectorfile, inputs$id)
    
    # Check geometry type of vector data and convert to points, if necessary
    if (! check_if_points_only(inputs$vectorfile) ) {
      print("Extracting feature centroids...")
      inputs$vectorfile <- get_centroids(inputs$vectorfile)
    }
  }
  return(inputs)
}

fetch_allas_data <- function(bucket_name) {
  # Gets all filenames in Allas as a dataframe
  # Arguments:
  # bucket_name = path to Amazon S3 bucket containing data.
  # Returns: 
  # allas_files = A dataframe that contains all filenames in given bucket
 
  allas_files <- aws.s3::get_bucket_df(bucket_name, region = "")
  return(allas_files)
}

get_timeframe <- function(startyear, endyear) {
  # Builds timeframe from given starting year and ending year.
  # If endyear was not given, timeframe consists of the starting year.
  
  # Arguments:
  # startyear = user-defined start year, integer
  # endyear = user-defined ending year, integer
  
  # Returns:
  # timeframe = sequence of integers between start and ending year
  
  if (is.null(endyear)) {
    timeframe <- c(startyear)
  # If endyear was given, build an integer sequence.
  } else {
    timeframe <- seq.int(startyear, endyear, 1)
  }
  return(timeframe)
}

filter_data <- function(dataframe, variables, timeframe) {
  # Filter data by variable names and timeframes.
  # Variable name and year must be included in user requests. 
  
  # Arguments:
  # dataframe = dataframe containing files available in Amazon S3 bucket
  # variables = user-defined CLIMGRID-variables
  # timeframe = sequence of integers containing years of interest
  
  # Returns:
  # filtered_data = original dataframe filtered by given criteria
  
  filtered_data <- dataframe %>% 
    dplyr::select(Key) %>%
    tidyr::separate(Key, sep = "/", into = c("Variable", "Filename"), remove = FALSE) %>%
    dplyr::filter(Variable %in% variables) %>%
    tidyr::separate(Filename, "_", into = c(NA, "Year"), remove = FALSE) %>%
    tidyr::separate(Year, ".nc", into = c("Year", NA)) %>%
    dplyr::filter(Year %in% timeframe) %>%
    dplyr::select(Key, Filename)
  
  return(filtered_data)
}

download_data <- function(bucket_name, dataframe, ncdir) {
  # Download filtered netCDF files to given directory.
  
  # Arguments:
  # bucket_name = name of the bucket where to download data from
  # dataframe = dataframe containing the relevant netCDF data for downloading
  # ncdir = local directory where netCDF files will be downloaded
  
  # Returns: 
  # None but downloads the data.
  
  for (key in dataframe$Key) {
    # Build filepath
    filepath = file.path(ncdir, basename(key))
    
    # Download only if file doesn't exist already
    if (! file.exists(filepath)) {
      aws.s3::save_object(
        object = key,
        bucket = bucket_name,
        region = "",
        file = file.path(ncdir,
                         basename(key)),
      )
      
    } else {
      print(paste0(filepath, " already exists, skipping..."))
    }
  }
}

reproject_vector <- function(vector) {
  # Reprojecting vector to EPSG:3067, in which the CLIMGRID rasters are.
  
  # Arguments:
  # vector = SpatVector object
  
  # Returns:
  # reprojected_vector = SpatVector object
  
  reprojected_vector = terra::project(x = vector, y = "epsg:3067")
  return(reprojected_vector)
}

read_vector <- function(vectorpath) {
  # Read vectorfile into a SpatVector object.
  
  # Arguments: 
  # vectorpath = full path to the vector file
  
  # Returns:
  # vector = SpatVector object
  
  vector <- terra::vect(vectorpath)
  vector <- reproject_vector(vector)
  # Only select relevant columns
  
  return(vector) 
}

extract_values <- function(ncdir, filenames, vector, outputdir, vectorname, delete) {
  # Extract values at point locations from raster.
  
  # Arguments:
  # raster = SpatRaster object to extract values from
  # vector = SpatVector object with geometry centroids to use for extraction
  
  # Returns:
  # valueframe = extracted values as a dataframe with IDs of vector as row names
  
  for (filename in filenames) {
    # Convert vectorfile into a dataframe
    variablename <- tools::file_path_sans_ext(filename)
    # Build filepath for file downloaded
    filepath = file.path(ncdir, filename)
    # Vectorfile name without extension
    vectorname <- tools::file_path_sans_ext(basename(vectorname))
    # Read netCDF file with terra
    netcdfbrick = terra::rast(filepath)
    # Assign coordinate reference system 
    terra::crs(netcdfbrick) <- "EPSG:3067"
    # Change layer names to DoY
    names(netcdfbrick) <- names_to_DoY(names(netcdfbrick))
    # Extract values
    valueframe = round(as.data.frame(terra::extract(netcdfbrick, vector)), digits = 1)
    # Define outputname
    outputname <- file.path(outputdir, paste0(variablename,"_", vectorname,  ".csv"))
    # Write csv
    write.csv(valueframe, file = outputname, row.names = FALSE, quote = FALSE)
    
    # Delete file if requested
    if (delete) {
      file.remove(filepath)
    }
  }

  
}

get_centroids <- function(vector) {
  # Converts other than point geometries into point geometries by calculating centroid coordinates.
  
  # Arguments:
  # vector = A SpatVector object
  
  # Returns:
  # vector = A SpatVector object with geometries replaced with centroids
  
  vector <- terra::centroids(vector)
  return(vector)
}

check_if_points_only <- function(vector) {
  # Check if geometries only contain points or if other types of features are included.
  
  # Arguments:
  # vector = a SpatVector object
  
  # Returns
  # points_only = boolean, TRUE if only points, FALSE otherwise
  
  points_only = terra::geomtype(vector) == "points"
  return(points_only) 
}

filter_vector <- function(vector, id_field_name) {
  # Remove unnecessary attribute columns in case they exist.
  
  # Arguments:
  # vector = SpatVector object
  # id_field_name = user-defined name for the unique identifier field
  
  # Returns:
  # vector = filtered SpatVector object only containing geometries and id field
  
  vectorfields <- names(vector)
  id_field_position <- match(id_field_name, vectorfields)
  vector <- vector[, id_field_position]
  return(vector)
}

names_to_DoY <- function(names) {
  # Converts original NetCDF layer names to days of year (DoY)
  
  # Arguments:
  # names = current names of the layers in netCDF file
  
  # Returns
  # names = converted names
  
  # Loop over names
  for (i in 1:length(names)) {
    # Split strings based on "=" and only keep the numerical part 
    names[i] <- unlist(strsplit(x = names[i], split = "="))[2]
  }
  
  # Convert the numerical part to DoY
  names <- as.Date(as.POSIXct(as.numeric(names) * 24 * 60 * 60, origin = "1970-01-01", tz = "UTC"))
  # Finish the naming convention to include DoY
  names <- paste0("DoY_", as.numeric(strftime(names, format = "%j")))
  
  return(names)
}