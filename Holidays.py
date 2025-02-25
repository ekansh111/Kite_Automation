

def CheckForDateHoliday(HolidayDate):#HolidayDateDifference,entry):
    #Holidays in yyyy-mm-dd format for the year 2025
    ListOfHolidays = {
        '2025-02-26': 'MahaShivRatri',
        '2025-03-14': 'Holi',
        '2025-03-31': 'Eid',
        '2025-04-10': 'MahaVir Jayanti',
        '2025-04-14': 'Ambedkar Jayanti',
        '2025-04-18': 'Good Friday',
        '2025-05-01': 'Maharastra Day',
        '2025-08-15': 'Independence Day',
        '2025-08-27': 'Ganesh Chaturthi',
        '2025-10-02': 'MG Jayanti',
        '2025-10-21': 'Diwali Pooja',
        '2025-10-22': 'Balipratipada',
        '2025-11-05': 'Guru Nanak Jayanti',
        '2025-12-25': 'Christmas'
    }
    #Previousday = str(date.today() + timedelta(HolidayDateDifference))
    #print(Previousday)
    for dates in ListOfHolidays:
        #Convert the date sent in the parameter to string , else will return false for different data type comparision
        if dates == str(HolidayDate):
            return True
    
    return False