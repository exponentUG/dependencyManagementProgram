import pandas as pd

#print out the final table that is used to segregate data for different divisions
def print_final_table(df: pd.DataFrame, cols: list[str]) -> None:
    print("\n=== Final Combined Table (post-filters) ===")
    view = df[[c for c in cols if c in df.columns]].copy()
    with pd.option_context("display.max_rows", None, "display.max_columns", None, "display.width", 200):
        print(view.to_string(index=False))
    print(f"\nTotal rows: {len(view)}")