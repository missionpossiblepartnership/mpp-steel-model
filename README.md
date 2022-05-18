[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


# MPP Steel Project

This repository contains the Steel Decarbonization Roadmap, as created by SYSTEMIQ for the Mission Possible Partnership (MPP).

The Steel Sector Transition Strategy Model (ST-STSM) calculates potential pathways to net-zero emissions for the steel sector. For a more detailed description of the steel sector, possible net-zero pathways and their trade-offs, please refer to the Mission Possible Partnership's Net-Zero Steel Sector Transition Strategy (links below).

## Project Context

The ST-STSM is an agent-based simulation model - decisions are made on the level of an individual steel plant. Each plant assesses the business case for switching to a new technology archetype when it faces a major decision in its investment cycle.

Twenty technology archetypes for steel production are considered in the model. These include production routes in use today, such as BF-BOF, EAF or DRI-EAF as well as emerging technologies with a greater emissions abatement potential, such as DRI-EAF with 100% green hydrogen and DRI-melt-BOF with CCS.

Business cases for each of these archetypes consider feedstock, fuel, and energy consumption, associated emissions, and operating and capital expenditures from publicly available data sources as well as Technology Readiness Levels (TRLs).

The model assesses the technological, economic and ecological implications of 700+ steel plants in 12 geopolitical regions transitioning to net-zero production. Each of the 12 regions is represented with an individual set of assumptions, such as resource availability, feedstock prices, crude steel demand, scrap availability and steel production capacity.

## Project Goals

The model is intended to be a flexible tool for interested parties to determine the milestones necessary in order to be on the right trajectory for the steel sector to decarbonize sufficiently and initiate concrete action, considering different net-zero scenarios.

The model is parameterized according to predefined scenarios, but can be further customized at a more granular level via custom parameter settings (outlined in the [steel model documentation](https://app.gitbook.com/o/vfc6mmxMh4Zr7LjKZ5yL/s/ZgShyrOk78V0ixbxKFrP/)).

## Installation

It is possible to install the library using `pip` with the following command:

```bash
pip install git+https://${YOUR_GITHUB_TOKEN}@github.com/systemiqofficial/mpp-steel-model.git
```

Change the `Your_GITHUB_TOKEN` to your personal access token. You can create one in [https://github.com/settings/tokens](https://github.com/settings/tokens).

## Useful resources

- [Steel Model Documentation](https://app.gitbook.com/o/vfc6mmxMh4Zr7LjKZ5yL/s/ZgShyrOk78V0ixbxKFrP/)
- [Official MPP Website](https://missionpossiblepartnership.org/)
- [Net-Zero Steel Sector Transition Strategy](https://missionpossiblepartnership.org/wp-content/uploads/2021/10/MPP-Steel-Transition-Strategy-Oct-2021.pdf)
- [Energy Transistions Commission](https://www.energy-transitions.org/)

## Setting up and running the model

See the model setup guide in the [official documentation](https://app.gitbook.com/o/vfc6mmxMh4Zr7LjKZ5yL/s/ZgShyrOk78V0ixbxKFrP/model-set-up).

## Contacts

### Technical questions

- andrew.isabirye@systemiq.earth

### Subject matter questions

- hannah.maral@systemiq.earth
- rafal.malinowski@systemiq.earth
