[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# MPP Steel Project

## Project Context

As of 2020, the Steel Industry accounted for approximately 7% of Global Greenhouse Gas Emissions. The Steel Sector Transition Strategy Model (ST-STSM) calculates potential pathways to net-zero emissions for the steel sector.

For a more detailed description of the steel sector, possible net-zero pathways and their trade-offs, please refer to the Mission Possible Partnership's [Net-Zero Steel Sector Transition Strategy](https://missionpossiblepartnership.org/action-sectors/steel/).

## Model Introduction

The ST-STSM is an agent-based simulation model - decisions are made on the level of an individual steel plant. Each plant assesses the business case for switching to a new technology archetype when it faces a major decision in its investment cycle.

20 technology archetypes for steel production are considered in the model. These include production routes in use today, such as Blast Furnace Blast Open Furnaces (BF-BOF), Electric Arc Furnaces (EAF) or Direct Reduced Iron EAFs (DRI-EAF) as well as emerging technologies with a greater emissions abatement potential, such as DRI-EAF with 100% green hydrogen and DRI-melt-BOF with Carbon Capture and Storage (CCS).

Business cases for each of these technology archetypes consider feedstock, fuel, energy consumption, associated emissivity rates, Technology Readiness Levels (TRLs), operating and capital expenditures from publicly available data sources and organisations in the Steel Industry.

The model assesses the technological, economic and ecological implications of 700+ steel plants in 12 geopolitical regions transitioning to net-zero production. Each of the 12 regions is represented with an individual set of assumptions, such as resource availability, feedstock prices, crude steel demand, scrap availability and steel production capacity.

The model is intended to be a flexible tool for interested parties to determine the milestones necessary in order to be on the right trajectory for the steel sector to decarbonize sufficiently and initiate concrete action, considering different net-zero scenarios.

The model is parameterized according to predefined scenarios, but can be further customized at a more granular level via custom parameter settings (outlined in the [MPP Steel Model Documentation](https://mpp.gitbook.io/mpp-steel-model/)).

## Model Installation

### Step 1: Clone the repository

To install the library, use `pip` with the following command:

```bash
git clone https://github.com/missionpossiblepartnership/mpp-steel-model.git
cd mpp-steel-model
pip install -r requirements.txt
```

### Step 2: Install the dependencies

Before running the model, first set up a virtual environment in which to install the model dependencies. Please read [this article](https://docs.python-guide.org/dev/virtualenvs) to understand how to install a virtual environment.

Once you have set up your virtual environment, install the model dependencies using the command below.

```bash
pip install -r requirements.txt
```

## Running The Model

For full instructions on how to run the model, please visit the [MPP Steel Model Documentation](https://mpp.gitbook.io/mpp-steel-model/).
The instructions below are to conduct a basic full model run. The model will execute specified scenario(s) in part or in full based on commands passed to the terminal.

### Step 1: Choosing a specific scenario

There are six scenarios to choose from

- baseline, baseline_high_circ, abatement, fastest_abatement, carbon_cost, tech_moratorium

To run a specific scenario you can append the command line flag of -c and then the name of the scenario you want to run e.g.

```bash
python main.py -c baseline
```

### Step 1: Running all scenarios

To run all six scenarios, you can append -a flag (example below)

```bash
python main.py -a
```

Please check the following link to the documentation to understand the various ways of running the model such executing a partial run of the model, running the model multiple times, or running different scenarios iterations.

### Step 2: Getting the model outputs

The model produces output files that are stored under the following relative path from the directory:

```bash
mppsteel > data > output_data
```

The output folders are timestamped at the moment the model is run e.g.

```bash
mppsteel > data > output_data
```

## Useful resources

- [Steel Model Documentation](https://mpp.gitbook.io/mpp-steel-model/)
- [Official MPP Website](https://missionpossiblepartnership.org/)
- [Energy Transition Comission](https://www.energy-transitions.org/)

## Technical and subject matter expert questions

If you have any questions or comments regarding the ST-STSM, please get in touch with the Mission Possible Partnership Steel Team via the following email [steel@missionpossiblepartnership.com](mailto:steel@missionpossiblepartnership.com).

## Ongoing Maintenance

Please note, this is the initial release of the steel model. 
Until further notice, the repository will not be actively maintained. However the team will be tracking feature requests.