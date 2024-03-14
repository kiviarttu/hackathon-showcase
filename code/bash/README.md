# Bash scripts

This folder contains three bash scripts that were used in the country-wide digital peatland mapping project. 

## Indexmap
indexmap_workflow.sh was used to create a large raster stack as NumPy array and find out the extent of pixels with NoData within the stack, as the NoData values in any explanatory variable in the machine learning method we used in the project were inherited to the final predictions. Thus, indexmaps were created to compare whether the NoData in model predictions corresponds to the NoData in explanatory variable layers. 

## Mosaic files
mosaic_tile_loop_batch.sh was used to create Sentinel-2 mosaics with sen2mosaic tool, which was run in a separate batch job with mosaic_tile.sh After mosaicking and vegetation index calculation, the outputs were combined into virtual rasters and country-wide mosaics with GDAL. 

## Virtual rasters
Due to limitations in how big raster files sen2mosaic tool can save, I created a workaround that utilized sen2mosaic on the basis of Sentinel-2 tiling grid and after vegetation index calculations, the results were combined into a country-wide mosaic through virtual rasterization and convertion into a single GeoTIFF file. 

