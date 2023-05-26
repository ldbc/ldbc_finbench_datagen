![logo](ldbc-logo.png)

# FinBench DataGen

![Build status](https://github.com/ldbc/ldbc_finbench_datagen/actions/workflows/ci.yml/badge.svg?branch=main)

The LDBC FinBench Data Generator (Datagen) produces the datasets for the [LDBC FinBench's workloads](https://ldbcouncil.org/benchmarks/finbench/).

This data generator produces labelled directred property graphs based on the simulation of financial activities in business systems. The key features include generation, factorization and transformation. A detailed description of the schema produced by Datagen, as well as the format of the output files, can be found in the latest version of official LDBC FinBench specification document.

## DataGen Design

### Data Schema

![Schema](https://github.com/ldbc/ldbc_finbench_docs/blob/fd326ec51ef4b3aa8ab5034f54013db18384f3c1/figures/data-schema.png)

### Implementation

#### Generation

Generation simulates financial activities in business systems to produce the raw data.

#### Factorization

Factorization profiles of the raw data to produce factor tables used for further parameter curation.

#### Transformation

Transformation transforms the raw data to the data for SUT and benchmark driver.

Note: SUT stands for System Under Test.

## Quick Start

### Pre-requisites

- Java 8 installed.
- Scala 2.12, note that it will be integrated when maven builds.
- Spark deployed. Spark 3.2.x is the recommended runtime to use. The rest of the instructions are provided assuming Spark 3.2.x.

### Build the project

``` bash
mvn clean package -DskipTests
```

### Run locally with scripts

See scripts/run.sh for details. It uses spark-submit to run the data generator.
Please make sure you have the pre-requisites installed and the build is successful.

``` bash
# Set the variables in scripts/run.sh
bash scripts/run.sh
```

### Run in cloud

To be supported.

## TroubleShooting

N/A yet

# Related Work

- FinBench Specification: https://github.com/ldbc/ldbc_finbench_docs
- FinBench Driver: https://github.com/ldbc/ldbc_finbench_driver
- FinBench Reference Implementation: https://github.com/ldbc/ldbc_finbench_transaction_impls
- FinBench ACID Suite: https://github.com/ldbc/finbench-acid

 