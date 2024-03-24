import pandas as pd
from datetime import date
from Directories import *

# Fetch the current date, month, and year
Today_date = date.today().strftime("%d%m%y")

# Define the file name
File_Name = 'option_details'
ConsFileName = File_Name + Today_date

# Suppress the warning about chained assignment
pd.options.mode.chained_assignment = None  # default='warn'

# Read the CSV file while handling errors
try:
    data = pd.read_csv(filepath_or_buffer=WriteOptionDetailsFile)
except pd.errors.ParserError as e:
    print(f"Error reading CSV file: {e}")
    data = pd.read_csv(filepath_or_buffer=WriteOptionDetailsFile, skiprows=lambda x: x != 0)

# Print the read data to check its structure
print("Original Data:")
print(data)

# Save the data to a new CSV file
data.to_csv(WorkDirectory + str(ConsFileName) + '.csv', header=True, index=False, mode='a')

# Truncate the original file
f = open(WriteOptionDetailsFile, "w")
f.truncate()
f.close()
