import pandas as pd
import logging

logger = logging.getLogger()


def compare_dataframes(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    key_columns: list[str] = ["Név", "Születési dátum"],
) -> pd.DataFrame:
    """
    Compare two DataFrames and return a DataFrame with the differences.
    The two DataFrames must have the same columns, but their order can differ.
    The DataFrames may have a different ordering of their rows.
    The resulting DataFrame will have the same columns as the input DataFrames, with an additional column "Változás".
    The "Változás" column will contain one of the following values:
    - "Új": The row is present in the new DataFrame, but not in the old one
    - "Törölt": The row is present in the old DataFrame, but not in the new one
    - "Adatváltozás": The row is present in both DataFrames, but the values differ

    Args:
        old_df: DataFrame with old data
        new_df: DataFrame with new data
        key_columns: Columns to use as keys for comparison. May be one or more columns, but must be present in the DataFrames.

    Returns:
        DataFrame containing the differences between the two DataFrames
    """
    # Find rows that are new or deleted
    new_rows = (
        new_df.merge(
            old_df,
            on=key_columns,
            how="left",
            indicator=True,
            suffixes=(" (új)", " (előző)"),
        )
        .query('_merge == "left_only"')
        .drop("_merge", axis=1)
    )
    deleted_rows = (
        old_df.merge(
            new_df,
            on=key_columns,
            how="left",
            indicator=True,
            suffixes=(" (előző)", " (új)"),
        )
        .query('_merge == "left_only"')
        .drop("_merge", axis=1)
    )

    # Find rows that have changed
    merged_df = old_df.merge(new_df, on=key_columns, suffixes=(" (előző)", " (új)"))

    # Create a mask for identifying changed rows
    changed_mask = pd.Series(False, index=merged_df.index)

    # Compare each non-key column individually to handle potential issues
    for col in [c for c in old_df.columns if c not in key_columns]:
        old_col = f"{col} (előző)"
        new_col = f"{col} (új)"

        # Skip if columns don't exist in the merged DataFrame
        if old_col not in merged_df.columns or new_col not in merged_df.columns:
            continue

        # Compare values and handle NaN properly
        col_diff = merged_df[old_col] != merged_df[new_col]
        # Handle the case where both values are NaN (should be considered equal)
        both_nan = merged_df[old_col].isna() & merged_df[new_col].isna()
        changed_mask |= col_diff & ~both_nan

    changed_rows = merged_df[changed_mask].copy()

    # Combine all changes
    changes_df = pd.concat([new_rows, deleted_rows, changed_rows], ignore_index=True)

    # Mark the type of change
    changes_df["Változás"] = ""

    # Set change types based on position
    new_count = len(new_rows)
    deleted_count = len(deleted_rows)

    if new_count > 0:
        changes_df.iloc[:new_count, -1] = "Új"
    if deleted_count > 0:
        changes_df.iloc[new_count : new_count + deleted_count, -1] = "Törölt"
    if len(changed_rows) > 0:
        changes_df.iloc[new_count + deleted_count :, -1] = "Adatváltozás"

    # Try eliminating the suffixes from the column names, where possible
    # Find columns with the same data (or NaN) in both DataFrames
    same_columns = []
    for col in old_df.columns:
        if col not in key_columns:
            old_col = f"{col} (előző)"
            new_col = f"{col} (új)"
            if (
                (changes_df[old_col] == changes_df[new_col])
                | changes_df[old_col].isna()
                | changes_df[new_col].isna()
            ).all():
                same_columns.append(col)

    # Remove suffixes from the column names for columns with the same data
    for col in same_columns:
        old_col = f"{col} (előző)"
        new_col = f"{col} (új)"
        changes_df[col] = changes_df[old_col].combine_first(changes_df[new_col])
        changes_df.drop([old_col, new_col], axis=1, inplace=True)

    # Reorder columns
    all_columns = key_columns.copy()
    for col in changes_df.columns:
        if col not in all_columns and col != "Változás":
            if " (új)" in col:
                # skip "új" columns, they will be added later
                continue
            elif " (előző)" in col:
                # add both "előző" and "új" columns right after each other
                prev_col = col
                next_col = col.replace(" (előző)", " (új)")
                all_columns.append(prev_col)
                all_columns.append(next_col)
            else:
                all_columns.append(col)
    all_columns.append("Változás")
    changes_df = changes_df[all_columns]

    return changes_df


if __name__ == "__main__":
    old_data_from = "missing_persons_old.xlsx"
    new_data_from = "missing_persons_new.xlsx"

    old_df = pd.read_excel(old_data_from)
    new_df = pd.read_excel(new_data_from)

    # old_df = pd.read_csv("old.csv")
    # new_df = pd.read_csv("new.csv")

    changes_df = compare_dataframes(old_df, new_df)

    changes_df.to_excel("missing_persons_changes.xlsx", index=False)
