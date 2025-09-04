import pandas as pd
import glob
"""
spark-submit ~/spark-etl-repo/PythonCourseForBeginners/.venv/ETL/Employee_Etl.py
"""
# 1. Extract: Read multiple branch CSV files
#files = glob.glob("./employee_data/*.csv")  # folder with multiple files
files = glob.glob("./employee_data/*.csv")  # folder with multiple files
dataframes = [pd.read_csv(file) for file in files]

# 2. Transform: Standardize column names
standardized_dfs = []
for df in dataframes:
    df.rename(columns={
        'empID': 'Employee_ID',
        'id': 'Employee_ID',
        'sal': 'Salary',
        'salary': 'Salary',
        'dept': 'Department'
    }, inplace=True)

    # Handle missing salary with median
    df['Salary'] = pd.to_numeric(df['Salary'], errors='coerce')
    df['Salary'].fillna(df['Salary'].median(), inplace=True)

    standardized_dfs.append(df)

# Merge all branches
master_df = pd.concat(standardized_dfs, ignore_index=True)

# 3. Load: Save clean master dataset
master_df.to_csv("clean_employee_master.csv", index=False)
print(" Employee master dataset saved!")
