# CFDE C2M2 Utilities

A collection of utilities for simplifying the C2M2 submission process. If you prepare your submission with these utilities, you do not need to run the prepare_C2M2_submission.py script or download the CV reference files manually.

**Init:** Initializes a fresh C2M2 submission in the current working directory, creating blank versions of all tables defined in the current C2M2 datapackage schema (C2M2_datapackage.json). 

**Prepare:** Resolves ontology identifiers and populates the Common Vocabulary (CV) tables.

**Validate:** Checks validity of C2M2 submission and highlights tables for which corrections, if any, are needed. 

**Package:** Collects metadata tables in a zip file to be submitted to the DRC portal.

**Submit:** Submits C2M2 datapackage to DRC portal (coming soon)

## Setup

### Using pipx
```bash
# installation
pipx install cfde-c2m2
# upgrade
pipx upgrade cfde-c2m2
```

### Using uv

```bash
# installation
uv tool install cfde-c2m2
# upgrade
uv tool upgrade cfde-c2m2
```

## Usage

```bash
# create an empty c2m2 submission OR update your existing submission directory
cfde-c2m2 init

# fill in your tables however you like

# finish preparing your package by resolving iris
cfde-c2m2 prepare

# verify integrity of your package
cfde-c2m2 validate

# zip the necessary files for a bare minimum package
cfde-c2m2 package

# Coming Soon: submit to DRC portal
cfde-c2m2 submit
```
