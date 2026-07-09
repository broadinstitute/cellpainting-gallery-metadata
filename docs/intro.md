# Cell Painting Gallery Metadata

The [Cell Painting Gallery](https://github.com/broadinstitute/cellpainting-gallery) (CPG) is a collection of Cell Painting image datasets hosted on AWS S3 by the Broad Institute.
This book documents the metadata standard used to harmonize those datasets.

## What is metadata harmonization?

Each dataset in the CPG arrives with its own naming conventions and column structures.
The harmonization process maps raw metadata columns to a shared controlled vocabulary defined in [`harmonized_ontology.json`](https://github.com/broadinstitute/cellpainting-gallery-metadata/blob/main/harmonized_ontology.json), making datasets comparable across sources.

## Contents

- **[Metadata Requirements](metadata_requirements)** — the fields contributors must provide when submitting a dataset, including allowed values for controlled-vocabulary columns.
