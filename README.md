# Cell Painting Gallery Metadata Harmonization

Project to create metadata that is harmonized across the [Cell Painting Gallery](https://github.com/broadinstitute/cellpainting-gallery) (CPG). Our goal is to specify a minimally sufficient metadata specification that supports a variety of use cases for the data in the CPG and can easily integrate with community-wide metadata specification efforts.

This is version 1 of the harmonization specification. We welcome discussion and expect to further develop the specification as community standards evolve.

## Orientation

[Harmonized Ontology](harmonized_ontology.json) is the fundamental specfication. It defines the set of columns and for each column specifies its type (e.g. string, integer, etc.), if it is allowed to be sparse, and if there defined ontology for which the column values must comply.

Note that we currently specify our own per-column ontologies to a minimal list that we populate based on values specific to the CPG. We are able to do this because all of our data is from a single assay or close derivative. We will consider switching to community-defined ontologies in the future.

[Ontology Description](supporting_metadata/ontology_description.csv) provides an explanatory description of the harmonized columns.

## Harmonization

All data in the CPG has specific metadata in a specific structure. See the [Cell Painting Gallery Documentation](https://broadinstitute.github.io/cellpainting-gallery/) for more information. The [CPG Harmonizer](cpg_harmonizer.py) ingests and merges the load_data and metadata .csv's on the CPG. It uses the [Output Structure](output_structure.json) to infer harmonized column names and values from input values.

[Harmonize Checker](harmonize_checker.py) checks the merged dataframe for its compliance with the rules specified in the [Harmonized Ontology](harmonized_ontology.json), reports on any components that are non-compliant so that they can be manually corrected, and returns a harmonized dataframe.

[Inferable Relationships](inferable_relationships.json) is a dictionary that can be used to infer the values of given harmonized columns based on values in other columns.

## Individual Dataset Harmonization

Individual datasets within the CPG are harmonized using individual notebooks. [Data Harmonizing Template](dataset_notebooks/data_harmonizing_template.ipynb) is a template notebook that coordinates the various scripts and provides descriptive comments about usage.

We encourage notebook use instead of strictly CLI interaction with the harmonization scripts because they serve as documentation of the manual annotation and correction performed.
