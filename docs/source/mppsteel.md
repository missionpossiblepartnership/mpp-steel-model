# mppsteel library

This is the full documentation for the functions inside the `mppsteel` library. The different modules are:

- Configuration, where all the constats, breaking point, units, and more values are defined.
- Data loading, a collection of modules with functions used to ingest data, standarize the datasets, and create the basis for the process.
- Mini models, collection of modules with functions to process the data and calculate the technology emissions, create the timeseries for the capex and opex, investment cycles for the plants and calculate a summary for the different variables that interacts with the plants.
- Model, this collection of modules and functions is the main solver, it contains the full process to calculate the plant transision based on the input data.
- Results, functions to calculate the results and outputs of the model.
- Utilities, set of utility functions that are used along all the library, such as handling the logs.

