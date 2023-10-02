# Self-Organizing Data Containers (SDCs)

## What is this project?
This project is a C++ implementation of an SDC prototype, as described in MIT's paper ["Self-Organizing Data Containers"](https://www.cidrdb.org/cidr2022/papers/p44-madden.pdf). For more details, see "Self-Organizing Data Containers.pdf" containing slides of the final presentation of the project.

## How does it work?
SDCs use metadata of a workload (various queries) to automatically create an optimized storage layout for the given workload, with the idea that future workloads will be similar. SDCs are self-learned and require no input from users, while potentially providing significant query performance improvements. The main index structure used in this prototype are [Qd-trees](https://arxiv.org/pdf/2004.10898.pdf).
