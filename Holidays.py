

def CheckForDateHoliday(HolidayDate):#HolidayDateDifference,entry):
    #Holidays in yyyy-mm-dd format for the year 2023
    ListOfHolidays = {'2024-01-26':'Republic Day','2024-03-08':'MahaShivRatri','2024-03-25':'Holi','2024-03-29':'Good Friday',
                    '2024-04-11':'Eid','2024-04-17':'Shri Ram Navami','2024-05-01':'Maharastra Day','2024-06-17':'Bakri Eid'
                    ,'2024-07-17':'Moharram','2024-08-15':'Independence Day','2024-08-02':'MG Jayanti','2024-11-01':'Diwali Pooja'
                    ,'2024-11-15':'Guru Nanak Jayanti','2024-12-25':'Christmas'}
    #Previousday = str(date.today() + timedelta(HolidayDateDifference))
    #print(Previousday)
    for dates in ListOfHolidays:
        #Convert the date sent in the parameter to string , else will return false for different data type comparision
        if dates == str(HolidayDate):
            return True
    
    return False