import time
import pandas as pd
from datetime import date


data = pd.read_csv(filepath_or_buffer='C:/Users/ekans/Documents/inputs/option_details.csv')

#Fetch the current date,month and year
Today_date = date.today().strftime("%d%m%y")

#print(Today_date)

File_Name = 'option_details'
ConsFileName = File_Name + Today_date

data.to_csv('C:/Users/ekans/Documents/inputs/'+str(ConsFileName) + '.csv',header=True,index=False,mode='a')

f = open("C:/Users/ekans/Documents/inputs/option_details.csv", "w")
f.truncate()
f.close()