

def CheckForDateHoliday(HolidayDate):#HolidayDateDifference,entry):
    #Holidays in yyyy-mm-dd format for the year 2023
    ListOfHolidays = {'2023-10-02':'MG Jayanti','2023-10-24':'Dusshera','2023-11-14':'Deepawali','2023-11-27':'GuruNanank Jayanti',
                    '2023-12-25':'Christmas','2023-09-19':'Testing date'}
    #Previousday = str(date.today() + timedelta(HolidayDateDifference))
    #print(Previousday)
    for dates in ListOfHolidays:
        #Convert the date sent in the parameter to string , else will return false for different data type comparision
        if dates == str(HolidayDate):
            return True
    
    return False