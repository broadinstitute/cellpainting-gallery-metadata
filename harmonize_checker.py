import os
import pandas as pd
import json
import glob as glob
import numpy as np


def load_dict(ontology_json):
    with open(ontology_json, "r") as f:
        ontology_dict = json.load(f)
    return ontology_dict


def check_columns(ontology_json, df, ret='full_df'):
    type_dict = {"str": "string", "float": "Float64", "int": "Int64"}
    ontology_dict = load_dict(ontology_json)
    for column in ontology_dict.keys():
        if column in df.columns:
            to_type = type_dict[ontology_dict[column]["Type"]]
            try:
                df[column] = df[column].astype(to_type)
            except:
                print(f"Could not convert column {column} to type {to_type}")
            if ontology_dict[column]["Harmonize"]:
                values = ontology_dict[column]["Values"]
                if np.all(df[column].dropna().isin(values)):
                    continue
                else:
                    u = df[column].dropna().unique()
                    outlier = u[~np.isin(u, values)]
                    print(f"Column {column} is not harmonized, {outlier} not in values")

    # Remove extra columns
    valid_cols = set(ontology_dict.keys())
    df_cols = set(df.columns)

    extra_cols = df_cols - valid_cols
    if extra_cols:
        print(f"Removing columns not in ontology: {list(extra_cols)}")
        df = df.drop(columns=list(extra_cols))

    # add missing columns
    missing_cols = valid_cols - df_cols
    if missing_cols:
        print(f"Adding missing ontology columns: {list(missing_cols)}")
        for col in missing_cols:
            df[col] = np.nan
    
    # confirm columns are not sparse
    improper_sparse = []
    for col in df.columns:
        if ontology_dict[col]['allow_sparse'] == False:
            if df[col].isna().any():
                improper_sparse += [col]
    if improper_sparse:
        print (f"Columns {improper_sparse} are sparse and should not be.")

    if ret == "full_df":
        return df
    elif ret == "extra_cols":
        return list(extra_cols)


if __name__ == "__main__":
    folders = sorted(glob.glob("./cpg00*"))
    for folder in folders:
        project = os.path.basename(folder)
        print(f"Working on project {project}")
        csv_files = glob.glob(os.path.join(folder, "*harmonized.csv"))
        if len(csv_files) > 0:
            df = pd.read_csv(csv_files[0])
            checked_df = check_columns("./harmonized_ontology.json", df)
            save_path = os.path.join(folder, f"{project}_checked.csv")
            checked_df.to_csv(save_path, index=False)
