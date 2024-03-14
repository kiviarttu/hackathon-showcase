# Python

Here are some examples of my Python scripts used in research projects. In addition to these, I have been a developer in [EODIE project](https://gitlab.com/fgi_nls/public/EODIE) which can be found from FGI GitLab. I was responsible for the dask-based parallelisation of EODIE. 

## Index calculator
index_calculator.py was used for vegetation index calculation from multispectral Sentinel-2 data. Several vegetation indices were calculated and converted into country-wide mosaics to be used as explanatory variables in machine learning models in the digital peatland mapping project. The code is partially the same as in EODIE. The process is parallelized with dask library so that for each tile and each index, a separate subprocess is created.

## Projection conversion
In order to build mosaics from Sentinel-2 images covering several UTM bands, they needed to be converted into a common projection, in this case EPSG:3067. As this process can be done to each raster individually, it was also parallelized with dask. 
