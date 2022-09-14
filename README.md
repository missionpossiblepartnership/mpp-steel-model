[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# MPP Steel Project

As of 2020, the Steel Industry accounts for approximately 7% of Global Greenhouse Gas Emissions. The Steel Sector Transition Strategy Model (ST-STSM) calculates potential pathways to net-zero emissions for the steel sector. For a more detailed description of the steel sector, possible net-zero pathways and their trade-offs, please refer to the Mission Possible Partnership's [Net-Zero Steel Sector Transition Strategy](https://missionpossiblepartnership.org/wp-content/uploads/2021/10/MPP-Steel-Transition-Strategy-Oct-2021.pdf).

## Project Context

The ST-STSM is an agent-based simulation model - decisions are made on the level of an individual steel plant. Each plant assesses the business case for switching to a new technology archetype when it faces a major decision in its investment cycle.

Twenty technology archetypes for steel production are considered in the model. These include production routes in use today, such as BF-BOF, EAF or DRI-EAF as well as emerging technologies with a greater emissions abatement potential, such as DRI-EAF with 100% green hydrogen and DRI-melt-BOF with CCS.

Business cases for each of these archetypes consider feedstock, fuel, and energy consumption, associated emissions, Technology Readiness Levels (TRLs), operating and capital expenditures from publicly available data sources and organisations in the Steel Industry.

The model assesses the technological, economic and ecological implications of 700+ steel plants in 12 geopolitical regions transitioning to net-zero production. Each of the 12 regions is represented with an individual set of assumptions, such as resource availability, feedstock prices, crude steel demand, scrap availability and steel production capacity.

## Project Goals

The model is intended to be a flexible tool for interested parties to determine the milestones necessary in order to be on the right trajectory for the steel sector to decarbonize sufficiently and initiate concrete action, considering different net-zero scenarios.

The model is parameterized according to predefined scenarios, but can be further customized at a more granular level via custom parameter settings (outlined in the [steel model documentation](https://mpp.gitbook.io/mpp-steel-model/)).

## Model Installation

### Step 1: Clone the repository

To install the library, use `pip` with the following command:

```bash
pip install git+https://${YOUR_GITHUB_TOKEN}@github.com/systemiqofficial/mpp-steel-model.git
```

Change the `Your_GITHUB_TOKEN` to your personal access token. You can create one in [https://github.com/settings/tokens](https://github.com/settings/tokens).

### Step 2: Install the dependencies

Before running the model, first set up a virtual environment in which to install the model dependencies. Please read [this article](https://docs.python-guide.org/dev/virtualenvs) to understand how to install a virtual environment.

Once you have set up your virtual environment, install the model dependencies using the command below.

```bash
pip install -r requirements.txt
```

## Running The Model

The model runs specified scenario(s) in part or in full based on commands passed to the terminal.

The base command to enter into the terminal is

```bash
python main.py {COMMAND}
```

### Step 1: Choosing a specific scenario

There are six scenarios to choose from

- baseline, baseline_high_circ, abatement, fastest_abatement, carbon_cost, tech_moratorium

To run a specific scenario you can append the command line flag of -c and then the name of the scenario you want to run

```bash
-c {SCENARIO_NAME}
```

To run all six scenarios, you can append -a

### Step 2: Choosing a specific part of the model to run

The model can either be run in full or in part. The model cna also be ran once or many times.

#### Full Model Run: All Scenarios

```bash
python main.py -a
```

#### Full Model Run: Individual scenario

```bash
python main.py -f -c {SCENARIO_NAME}
```

#### Partial model runs / Multiple model runs

There are a variety of ways to run the model partially.

- The first half of the model only
- The second half of the model only
- Specific steps of the model

Additionally, there are a variety of ways to run the model multiple times.

- The same scenario multiple times
- All scenarios multiple times
- Different iterations of scenario settings

Please check the following link to the documentation to understand the various ways of running the model in the Model Set-up > Running The Model section [Steel Model Documentation](https://mpp.gitbook.io/mpp-steel-model/)

### Step 3: Getting the model outputs

The model produces output files that are stored under the following relative path from the directory:

```bash
mppsteel > data > output_data
```

The output folders are timestamped at the moment the model is run.

## Steel Industry Resources

- [Steel Model Documentation](https://mpp.gitbook.io/mpp-steel-model/)
- [Official MPP Website](https://missionpossiblepartnership.org/)
- [Net-Zero Steel Sector Transition Strategy](https://missionpossiblepartnership.org/wp-content/uploads/2021/10/MPP-Steel-Transition-Strategy-Oct-2021.pdf)
- [Energy Transistions Commission](https://www.energy-transitions.org/)

## Technical and subject matter expert questions

If you have any questions of a technical or content nature, please get in touch with the Steel Team at Mission Possible Partnership with the following email [steel@missionpossiblepartnership.com](steel@missionpossiblepartnership.com).

## Ongoing Maintenance

Until further notice, there will be no ongoing maintenance of the codebase.
