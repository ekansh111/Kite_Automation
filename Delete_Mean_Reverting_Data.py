import os
import shutil
from Directories import *

def delete_contents_in_directory(directory):
    # Check if the directory exists
    if os.path.exists(directory):
        # Iterate over all contents in the directory (files and folders)
        for item in os.listdir(directory):
            item_path = os.path.join(directory, item)
            try:
                # Check if it's a file and delete it
                if os.path.isfile(item_path):
                    os.remove(item_path)
                    print(f"Deleted file: {item_path}")
                # Check if it's a folder and delete it
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                    print(f"Deleted folder: {item_path}")
            except Exception as e:
                print(f"Error deleting {item_path}: {e}")
    else:
        print(f"Directory {directory} does not exist.")

def delete_contents_in_directories():
    directories = [MeanReversionCharts,
        MeanReversionPortfolioValue,
        MeanReversionZScore,
        MeanReversionSpreadResiduals,
        MeanReversionClosingPrice,
        MeanReversionCointigrationHeatMap
    ]
    
    # Loop through each directory and delete files and folders
    for directory in directories:
        delete_contents_in_directory(directory)

if __name__ == "__main__":
    delete_contents_in_directories()
