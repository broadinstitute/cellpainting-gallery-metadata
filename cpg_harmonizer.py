import pandas as pd
import os
import glob as glob
import numpy as np
import json
import re


class Project:

    def __init__(self, master_path, structure_json, project_name=None):
        self.master_path = master_path
        self.structure_json = structure_json
        self.project_name = project_name
        self.df = None
        self.structure_dict = None
        self.main_df = None
        self.barcode_platemap_df = None
        self.platemap_df = None
        self.external_df = None
        self.master_df = None
        self.setup_cols = None

    def run_conversion(
        self,
        main_csvs,
        main_csv_batch,
        platemap_csvs,
        platemap_csv_batch,
        platemap_txt,
        platemap_txt_batch,
        platemap_txt_name,
        external_tsv,
    ):

        # Find and concatenate all the needed and available files
        # main_csv_paths, main_csv_batch, platemap_csv_paths, platemap_csv_batch, platemap_txt_paths, platemap_txt_batch, platemap_txt_pert, external_tsv_paths = self.parse_files()
        # Need to start with the setup columns
        self.setup_cols = self.initialize_df()

        self.master_df = self.collate_df(
            main_csvs, sep=",", add_col=["Batch"], add_value=[main_csv_batch]
        )

        if len(platemap_csvs) > 0:
            self.barcode_platemap_df = self.collate_df(
                platemap_csvs,
                sep=",",
                add_col=["Batch"],
                add_value=[platemap_csv_batch],
            )
        else:
            print("No platemap csv found")

        if len(platemap_txt) > 0:
            self.platemap_df = self.collate_df(
                platemap_txt,
                sep="\t",
                add_col=["Batch", "Platemap_Name"],
                add_value=[platemap_txt_batch, platemap_txt_name],
            )
        else:
            print("No platemap files found")

        if len(external_tsv) > 0:
            self.external_df = self.collate_df(external_tsv, sep="\t")
        else:
            print("No external metadata found")

        self.master_df = self.separate_channels(self.master_df)

        # Merge all df to make a master df
        if len(platemap_csvs) > 0:
            self.master_df = self.merge_two_df(
                self.master_df,
                [
                    self.structure_dict["Columns"]["Plate"],
                    self.structure_dict["Columns"]["Batch"],
                ],
                self.barcode_platemap_df,
                [
                    self.structure_dict["Columns"]["Plate"],
                    self.structure_dict["Columns"]["Batch"],
                ],
            )
        else:
            print("Skipping platemap csv")

        if len(platemap_txt) > 0:
            self.master_df = self.merge_two_df(
                self.master_df,
                [
                    self.structure_dict["Columns"]["Well"],
                    self.structure_dict["Columns"]["Plate_Map"],
                ],
                self.platemap_df,
                [
                    self.structure_dict["Columns"]["Well"],
                    self.structure_dict["Columns"]["Plate_Map"],
                ],
            )
        else:
            print("Skipping platemap files")

        if len(external_tsv) > 0:
            self.master_df = self.merge_two_df(
                self.master_df,
                [["broad_sample.*"]],
                self.external_df,
                [["broad_sample.*"]],
            )
        else:
            print("Skipping external metadata")

        self.master_df = self.combine_concentrations()

        # Correct column names based on regex dictionary
        self.master_df = self.correct_colnames()

        # Correct column values based on regex dictionary
        self.master_df = self.correct_colvalues()

        # Save final output df
        # dest_path = os.path.join(self.master_path, f"{self.project_name}_harmonized_metadata.csv")
        # self.master_df.to_csv(dest_path, index=False)
        return self.master_df

    def initialize_df(self):
        """
        Creates the baseline dataframe with File Name and File Path

        Returns:
        Dataframe template
        """
        with open(self.structure_json, "r") as f:
            structure_dict = json.load(f)
        self.structure_dict = structure_dict
        setup_cols = self.structure_dict["Setup"]
        self.df = pd.DataFrame(columns=setup_cols)
        return setup_cols

    def parse_files(self):
        """
        Finds all the csvs and existing test files

        Returns:
        List of paths for the main csvs in load_data_csv and the external metadata (which might not be present)
        """
        # grab all files
        main_csv_paths = []
        main_csv_batch = []

        platemap_csv_paths = []
        platemap_csv_batch = []

        platemap_txt_paths = []
        platemap_txt_batch = []
        platemap_txt_pert = []

        external_tsv_paths = []

        # Walk through directory from the master path and gather all csvs
        for dirs, dirnames, files in os.walk(self.master_path):

            for file in files:
                full_path = os.path.join(dirs, file)
                if full_path.endswith("load_data.csv"):
                    main_csv_paths.append(full_path)
                    batch = dirs.split("/")[-2]
                    main_csv_batch.append(batch)

                elif full_path.endswith("barcode_platemap.csv"):
                    platemap_csv_paths.append(full_path)
                    batch = dirs.split("/")[-1]
                    platemap_csv_batch.append(batch)

                # Gather any txt files about what is added to the plates & biologically relevant info
                if dirs.endswith("platemap"):
                    platemap_txt_paths.append(full_path)
                    batch = dirs.split("/")[-2]
                    pert = full_path.split(".txt")[0].split("/")[-1]
                    platemap_txt_batch.append(batch)
                    platemap_txt_pert.append(pert)

                elif dirs.endswith("external_metadata"):
                    external_tsv_paths.append(os.path.join(dirs, file))

        return (
            main_csv_paths,
            main_csv_batch,
            platemap_csv_paths,
            platemap_csv_batch,
            platemap_txt_paths,
            platemap_txt_batch,
            platemap_txt_pert,
            external_tsv_paths,
        )

    def collate_df(self, dfs, sep, add_col=None, add_value=None):
        """
        Concantenates the files from multiple batches or sources into one

        Parameters:
        dfs : List of dataframes
        add_col : str or List of str
            Column name to add more context to the dataframe (eg. Batch)
        add_value : List of str
            Values to fill the added column

        Returns:
        Concatenated dataframe
        """
        to_concat = []
        for i, small_df in enumerate(dfs):
            # small_df = pd.read_csv(csv, sep = sep)
            if add_col is not None:
                value = (
                    [value[i] for value in add_value]
                    if isinstance(add_value[0], list)
                    else add_value
                )
                small_df = self._fill_df(small_df, add_col, value)
            to_concat.append(small_df)

        complete_df = pd.concat(to_concat, ignore_index=True)

        return complete_df

    def s3_url(self, s3_path):
        """
        Converts the curent file path into s3 an url

        Parameters:
        s3_path : str
            Corrected file path

        Returns:
        Usable URL
        """
        # Conversion specific to the cell painting s3 bucket
        source_url = s3_path[len("s3://cellpainting-gallery/") :]
        dest_url = (
            f"https://cellpainting-gallery.s3.us-east-1.amazonaws.com/{source_url}"
        )
        return dest_url

    def separate_channels(self, df):
        """
        Reorganizes the URL channel columns into File Path, File Name and Label columns,
        each row in the dataframe will have one image to which it corresponds instead of
        all images for one site.

        Parameters:
        df : Dataframe to reorganize

        Returns:
        Dataframe with per image organization
        """
        self.setup_cols = self.initialize_df()
        # Grab columns that correspond to channels

        url_cols = [
            col for col in df.columns if re.search("URL.", col, flags=re.IGNORECASE)
        ]

        non_url_cols = df.loc[:, ~df.columns.isin(url_cols)].columns

        # Change structure of the dataframe to that each row corresponds to one file path
        df_long = df.melt(
            id_vars=non_url_cols,
            value_vars=url_cols,
            var_name="URL_Column",
            value_name="File Path",
        )
        # Remove all rows where "File Path" is NaN since it means that these file don't exist
        df_long = df_long.dropna(subset=["File Path"])
        # Change file path to be a URL

        df_long[self.setup_cols[0]] = df_long["File Path"].apply(self.s3_url)
        # Get Orig suffix (eg, RNA, DNA...)
        df_long["Label"] = df_long["URL_Column"].str.extract(r"URL_Orig(.+)")

        df_long[self.setup_cols[1]] = (
            df_long["File Path"].str.split("/").str[-1].str.split(".").str[0]
        )

        try:
            self._fill_df(self.df, self.setup_cols, df_long[self.setup_cols])
            self.df = self.df.set_index("File Path")
        except KeyError as e:
            print("Missing column in input data:", e)
        except Exception as e:
            print("Unexpected error:", e)

        return df_long

    def merge_two_df(self, df_l, key_l, df_r, key_r):
        """
        Merges two dataframes based on lists of regex type keys

        Parameters:
        df_l : Dataframe that will be merged into (left)
        df_r : Dataframe that will merged into the left dataframe (right)
        key_l: List of regex column keys to search for in the left dataframe
        key_r: List of regex column keys to search for in the right dataframe

        Returns:
        Dataframe of the merged result
        """
        # check if any of the patterns of regex columns
        for i, key in enumerate(key_l):
            try:
                new_key = self._find_matches(key, df_l.columns)
                key_l[i] = new_key[0]
                # df_l[new_key[0]] = df_l[new_key[0]].astype(str).str.upper()
            except:
                print("Could not find matching key, check file and dictionary")
        for i, key in enumerate(key_r):
            try:
                new_key = self._find_matches(key, df_r.columns)
                key_r[i] = new_key[0]
                # df_r[new_key[0]] = df_r[new_key[0]].astype(str).str.upper()
            except:
                print("Could not find matching key, check file and dictionary")

        merged_df = df_l.merge(df_r, left_on=key_l, right_on=key_r, how="left")

        # find which keys that we used for merging are different
        keys_to_drop = [right for left, right in zip(key_l, key_r) if left != right]
        keys_to_keep = [right for left, right in zip(key_l, key_r) if left == right]

        # Drop right-side keys that aren't duplicates of left-side
        merged_df.drop(columns=keys_to_drop, inplace=True)

        # Find columns that were duplicated in any merges and remove them if they are true duplications
        improper_cols = [
            x for x in merged_df.columns if x[-2:] == "_x" or x[-2:] == "_y"
        ]
        if len(improper_cols) > 0:
            improper_cols = list(set([x[:-2] for x in improper_cols]))
            for col in improper_cols:
                if merged_df[f"{col}_x"].equals(merged_df[f"{col}_x"]):
                    merged_df = merged_df.rename(columns={f"{col}_x":col}).drop(columns=[f"{col}_y"])
        # Add warning if duplicated columns couldn't be resolved
        improper_cols = [
            x for x in merged_df.columns if x[-2:] == "_x" or x[-2:] == "_y"
        ]
        if len(improper_cols) > 0:
            print(
                f"Warning: Likely need to resolve {improper_cols} columns that were duplicated"
            )

        return merged_df

    def combine_concentrations(self):
        """
        Corrects the columns that point to the concentration of the compound added

        Returns:
        Dataframe with concentration columns merged into one comma separated concentration column
        """
        conc_patterns = self.structure_dict["Treatment_Concentration"]
        conc_cols = [
            col
            for col in self.master_df.columns
            if any(re.search(pat, col, flags=re.IGNORECASE) for pat in conc_patterns)
        ]
        if conc_cols:
            self.master_df[conc_cols] = self.master_df[conc_cols].replace("nan", np.nan)
            self.master_df["Concentration"] = self.master_df.apply(
                lambda row: self._concentration_helper(row, conc_cols), axis=1
            )
            self.master_df = self.master_df.drop(conc_cols, axis=1)
        else:
            print("No concentration columns found — skipping concentration merge.")
        return self.master_df

    def _concentration_helper(self, row, conc_cols):
        """
        Concatenates concentration column into one column for one row

        Returns:
        Value
        """
        vals = []
        for col in conc_cols:
            if pd.notna(row[col]):
                vals.append(f"{row[col]} {col}")
        return ", ".join(vals) if vals else np.nan

    def correct_colnames(self):
        """
        Corrects the column names based on the standard nomenclature

        Returns:
        Dataframe with renamed columns
        """
        col_map = {}
        for new_name in self.structure_dict["Columns"].keys():
            for col in self.master_df.columns:
                if any(
                    re.search(pat, col, flags=re.IGNORECASE)
                    for pat in self.structure_dict["Columns"][new_name]
                ):
                    col_map[col] = new_name
        self.master_df = self.master_df.rename(columns=col_map)

        # Removal of any exactly duplicated columns caused by column name correction
        self.master_df = self.master_df.loc[:, ~self.master_df.columns.duplicated()]

        return self.master_df
    
    def correct_colvalues(self):
        """
        Corrects the column values based on the standard nomenclature

        Returns:
        Dataframe with renamed columns
        """
        for col in self.structure_dict["Values"].keys():
            if col in self.master_df.columns:
                print(f"Harmonizing values of {col}")
                for harmonized_name in self.structure_dict["Values"][col]:
                    self.master_df[col] = self.master_df[col].replace(self.structure_dict["Values"][col][harmonized_name], harmonized_name)

        return self.master_df

    def _find_matches(self, patterns, columns):
        """
        Finds column names which match the patterns

        Returns:
        List of matching columns
        """
        matches = [
            col
            for col in columns
            for pat in patterns
            if re.search(pat, col, flags=re.IGNORECASE)
        ]
        return matches

    def _fill_df(self, df, columns, values):
        """
        Add columns with set values to an existing dataframe

        Parameters:
        df: Dataframe to add columns into
        columns: list of column names to make the new columns
        values: list of values to fill the column with

        Returns:
        Dataframe with added columns
        """
        cols = columns if isinstance(columns, list) else [columns]
        values = values if isinstance(values, list) else [columns]
        for col, val in zip(cols, values):
            df[col] = val
        return df
