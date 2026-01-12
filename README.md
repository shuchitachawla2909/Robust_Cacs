# FogWorkflowSim
A toolkit for modeling and simulation of workflow scheduling in Internet of Things, Edge, and Fog Computing environments.

FogWorkflowSim extends WorkflowSim and CloudSim to support workflow-based applications in fog and edge architectures, enabling evaluation of scheduling, placement, and resource management strategies.

# Developer
* Developer organization:
1. School of Information Technology, Deakin University, Geelong, Australia  
2. CCIS Laboratory, School of Computer Science and Technology, Anhui University, Hefei, China

* Developer members: Xiao Liu, Xuejun Li, Lingmin Fan, Lina Gong, Jia Xu

# FogWorkflowSim User Tutorial
Access the official user tutorial at:  
<A href="https://github.com/CCIS-AHU/FogWorkflowSim/tree/master/usertutorial">https://github.com/CCIS-AHU/FogWorkflowSim/tree/master/usertutorial</A>

## How to run FogWorkflowSim ?

* Create a Java project in Eclipse.
* Inside the project directory, initialize an empty Git repository with the following command
```
git init
```
* Add the Git repository of FogWorkflowSim as the `origin` remote.
```
git remote add origin https://github.com/CCIS_AHU/FogWorkflowSim
```
* Pull the contents of the repository to your machine.
```
git pull origin master
```
* Run the example files (e.g. MainUI.java) to get started.

# Collision-Aware Cloud Scheduling (CACS) Extension

This repository has been extended with an implementation of the Collision-Aware Cloud Scheduling (CACS) strategy for workflow applications.

## What is CACS?

CACS is a workflow scheduling approach that explicitly models client-side communication contention, assuming a single I/O port at the client.  
Unlike traditional schedulers that ignore communication collisions, CACS avoids overlapping send/receive operations during workflow execution.

## How CACS Is Implemented in FogWorkflowSim

The CACS implementation is split into two clearly separated phases, consistent with the original design of the algorithm:

### 1. Offline Planning Phase

* Implemented in `CACSPlanningAlgorithm`
* Performs a pre-simulation of workflow execution
* Models a single communication port using non-overlapping send/receive intervals
* Computes:
  * Send start time (SVI)
  * Receive end time (EVI)
* Assigns a global priority rank to each task based on communication-aware execution order

### 2. Runtime Scheduling Phase

* Implemented in `CACSSchedulingAlgorithm`
* Uses priorities computed during offline planning
* At runtime:
  * selects the highest-priority READY task
  * assigns it to an IDLE VM with minimum earliest finish time (EFT)
* No DAG traversal or communication modeling is performed at runtime

## Task-Level Extensions

The `Task` class has been extended to support CACS-specific metadata:

* `cacsSvi`: send start time during offline planning
* `cacsEvi`: receive end time during offline planning
* `cacsRank`: final task priority used by the runtime scheduler

These extensions are isolated and do not interfere with existing scheduling algorithms.

## How to Use CACS

To run simulations with CACS:

* Use `CACSPlanningAlgorithm` as the workflow planner (Hardcoded right now)
* Use `CACSSchedulingAlgorithm` as the runtime scheduler
* Run simulations as usual via existing UI or test drivers (e.g., MainUI)

No changes are required to workflow definitions or input formats.

## Notes and Scope

* The current implementation focuses on communication collision awareness
* Bandwidth and data transfer are approximated using task output files
* Runtime scheduling remains lightweight and scalable
* Further robustness and contention modeling extensions will be added incrementally

# References
