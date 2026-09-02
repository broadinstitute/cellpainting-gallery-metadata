# Metadata requirements

Some entries have an allowed list of values that are noted below.
If your experiment should have a value that is not in the allowed list, please note your correct value and let us know so we can update the allowed list.

If you do not know the values for any of the following, leave them blank.
DO NOT GUESS.

## Per-Experiment

The values are likely to be consistent across your whole experiment and are therefore reported once.
If they are **not** (e.g. you used multiple cell lines in your experiment), mark that entry as `multiple` and include that entry as a column in each of your PLATEMAP.tsv's.

`Plate_Size` (Number of wells in multiwell plate):

```text
Currently allowed values are: 6, 24, 96, 384, 1536
```

`CP_Version` (Version of the Cell Painting Assay if using a published protocol):

```text
Currently allowed values are: v1, v2, v2.5, v3, lipocyte painting, neuro painting, live cell painting, other
```

`DOI_to_Cite` (DOI to cite when using data):  
`Year_Imaged` (Year of image acquisition):  
`Cell_Line_Name` (Name of the cell line, if named).*

```text
Currently allowed values are: A549, C2C12, HeLa, HepG2, HepaRG, HEK293T, Huh7, MCF-7, N2a, SH-SY5Y, U2OS, None
```

`Cell_Line_Type` (Cell type):

```text
Currently allowed values are: bone cancer, breast cancer, cervical cancer, iPSC, iPSC derived cardiomyocyte, liver cancer, lung cancer, primary human hepatocyte
```

`Cell_Line_Modification` (Line modifications (clone selection, Cas9 overexpression, day of differentiation etc.)):

```text
Currently allowed values are: Cas9 polyclonal overexpression, subclone A, None
```

`Cell_Line_Organism` (Organism from which the cell line was derived):

```text
Currently allowed values are: Homo sapiens, Mus musculus, Rattus norvegicus
```

`Microscope_Name` (Microscope manufacturer and type):

```text
Currently allowed values are: Revvity Opera Phenix, Revvity Operetta, Molecular Devices ImageXpress 5000A, Molecular Devices ImageXpress Micro, Molecular Devices ImageXpress Micro XL, Molecular Devices ImageXpress Micro XLS, Agilent BioTek Cytation 5
```

`Microscope_Binning` (Binning of the microscope):

```text
Currently allowed values are: 1, 2, 3, 4
```

`Microscope_Modality` (Microscope modality):

```text
Currently allowed values are: Widefield, Confocal
```

`Microscope_Objective_Magnification` (Magnification of the microscope objective):  
`Microscope_Objective_NA` (Numerical aperture of the microscope objective):  
`Microscope_Pixel_Size` (Size of a pixel in microns):  
`Image_Bit_Depth` (Bit depth of the image):

```text
Currently allowed values are: 8, 12, 16
```

`Image_Size_X` (Image size in pixels, X dimension):  
`Image_Size_Y` (Image size in pixels, Y dimension):  
`Timepoint_Primary_Treatment` (Time in hours since primary treatment):  
`Timepoint_Secondary_Treatment` (Time in hours since secondary treatment):  
`Timepoint_Acquisition` (For data with time dimension, time in hours since start of acquisition)  
For data without a time dimension, report as 0.
For data with a time dimension, indicate what `Timepoint_Acquisition` maps to each t and where that t is found (e.g. image filename, folder name, etc.).  
`Image_Position_Z` (Relative Z-plane of the image)  
For data with only one z position, report as 0.
For data with multiple z positions, indicate what `Image_Position_Z` maps to each z and where that z is found (e.g. image filename, folder name, etc.).
Note that `Image_Position_Z` is **not** an absolute value (e.g. microns) but should be **relative** to other z planes.

## Per-Channel

For each channel in your experiment, please report this set of values.
Examples of each of these labels is visible in our harmonized ontology.
(If you have a Brightfield channel the value for all of these is "None" for that channel.)

*Note that you can look up our standardized `Label_Reagent`, `Label_Structure`, `Label_Molecule`, and `Label_Mechanism` entries for any standard Cell Painting channels in [Inferable Relationships](inferable_relationships.md).*

`Label_Fluorophore` (Fluorophore conjugated to reagent or dye variant):

```text
Currently allowed values are: 14, 33342, Acridine Orange, Alexa Fluor 488, Alexa Fluor 555, Alexa Fluor 562/624, Alexa Fluor 568, Alexa Fluor 594, Deep Red, Texas Red, None
```

`Label_Mechanism` (Mechanism of labeling reagent):

```text
Currently allowed values are: Dye, Antibody, None
```

`Label_Molecule` (Molecule targeted by the label):

```text
Currently allowed values are: Acidic Compartments, beta-tubulin, DNA, DNA, RNA, f-Actin, f-Actin, N-acetyl-D-glucosamine, Glycoproteins, Mitochondria, N-acetyl-D-glucosamine, RNA, None, Unknown
```

`Label_Reagent` (Reagent used to label sample in image):

```text
Currently allowed values are: Acridine Orange, ChromaLive, ConcanavalinA (ConA), DAPI, Hoechst, Lysotracker, Mitotracker, Phalloidin, SYTO, Tocris MitoBrilliant, Wheat Germ Agglutinin (WGA), Wheat Germ Agglutinin (WGA), Phalloidin, anti-beta-tubulin IgG, None
```

`Label_Structure` (Expected structure that is labeled in image):

```text
Currently allowed values are: beta-tubulin, Endoplasmic Reticulum, f-Actin, Golgi, Plasma Membrane, Golgi, Plasma Membrane, f-Actin, Lysosomes, Lysosomes, Endosomes, Mitochondria, Nucleus, Nucleolus, Cytoplasmic RNA, Nucleus, Nucleolus, Cytoplasmic RNA, None, Unknown
```

`Microscope_Excitation_Peak` (Excitation peak of the microscope channel imaged):  
`Microscope_Excitation_Width` (Excitation width of the microscope channel imaged):  
`Microscope_Emission_Peak` (Emission peak of the microscope channel imaged):  
`Microscope_Emission_Width` (Emission width of the microscope channel imaged):

## Per-Treatment

The are columns to include in your PLATEMAP.tsv as the values are likely to be different for each well.
Not all experiments will have all of these metadata categories (particularly if your treatment is genetic instead of chemical), but please include as many as you are able.

`Treatment_Primary_Treatment` (Perturbation applied to cells, common name)  
`Treatment_Broad_Sample` (Perturbation applied to cells, Broad Sample ID)  
`Treatment_Concentration` (Concentration if chemical perturbation)  
`Treatment_InChIKey` (Perturbation applied to cells, InChiKey representation)  
`Treatment_Mechanism` (Annotation about mechanism of treatment)  
`Treatment_PubChem_CID` (Perturbation applied to cells, PubChem Cid)  
`Treatment_SMILES` (Perturbation applied to cells, SMILES representation)  
`Treatment_Solvent` (Solvent if chemical perturbation)  
`Treatment_Secondary_Treatment` (Secondary treatment applied to cells, if present)  
`Treatment_Category` (Type of treatment. e.g. compound, ORF, CRISPR, miRNA, etc.):

```text
Currently allowed values are: ORF, Compound, CRISPR, shRNA, miRNA, None
```

`Treatment_Control_Class` (Treatment is an experimental variable or a control):

```text
Currently allowed values are: Control, Treatment, NegCon, PosCon
```

## Other

There are additional columns in the harmonized metadata files that are created computationally from file paths and the load_data.csv.
They do not need to be included in your PLATEMAP.tsv.

`File Path` (S3 Path for image file)  
`File Name` (Name of image file)  
`Source` (Data source)  
`Batch` (Data batch)  
`Plate` (Plate name)  
`Well` (Well name)  
`Site` (Site number)

## Additional notes

Please check that the plate names in the `load_data.csv`'s and the `barcode_platemap.csv`'s are a perfect match.
