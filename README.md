[Work in progress]
# Self-Organizing Data Containers (SDCs)

## What is this project?
This project is a C++ implementation of an SDC prototype, as described in MIT's paper ["Self-Organizing Data Containers"](https://www.cidrdb.org/cidr2022/papers/p44-madden.pdf).

## How does it work?
SDCs use metadata of a workload (various queries) to automatically create an optimized storage layout for the given workload, with the idea that future workloads will be similar. The main index structure used in this prototype are [Qd-trees](https://arxiv.org/pdf/2004.10898.pdf).
