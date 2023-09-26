import pandas as pd



def create_index(df_one, date_col, datetime=True, index=True, sort=True):
    """ This function will re-index a DataFrame with the specified date column.
        df_one - DataFrame to alter.
        date_col - String name of the column with the date.
        datetime  - Will convert column to datetime.
        index - Will make the column the index.
        sort - Will sort the values so that they are in chronological order."""
    df = df_one.copy()
    if datetime:
        df[date_col] = pd.to_datetime(df[date_col])
    if index:
        df = df.set_index(date_col)
    if sort:
        df = df.sort_values(date_col)
    return df


def add_cols(df_one, name_dd=True, name_mm=True, year=True):
    """ This function will add the desired columns to a dataframe and return it.
        DataFrame must have datetime object as index.
        df_one - DataFrame to alter.
        name_dd - Adds name of day.
        name_mm - Adds name of month.
        year - Adds year."""
    df = df_one.copy()
    if name_dd:
        df['day_of_week'] = df.index.day_name()
    if name_mm:
        df['month'] = df.index.month_name()
    if year:
        df['year'] = df.index.year
    return df
