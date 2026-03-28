

def CheckForDateHoliday(HolidayDate):#HolidayDateDifference,entry):
    #Holidays in yyyy-mm-dd format
    ListOfHolidays = {
        # 2025
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
        '2025-12-25': 'Christmas',
        # 2026 (NSE/BSE/MCX/NCDEX trading holidays)
        '2026-01-26': 'Republic Day',
        '2026-02-17': 'MahaShivRatri',
        '2026-03-04': 'Holi',
        '2026-03-20': 'Eid-ul-Fitr',
        '2026-03-30': 'Ram Navami',
        '2026-04-03': 'Good Friday',
        '2026-04-14': 'Ambedkar Jayanti',
        '2026-05-01': 'Maharashtra Day',
        '2026-05-27': 'Eid-ul-Adha',
        '2026-06-25': 'Muharram',
        '2026-08-15': 'Independence Day',
        '2026-08-18': 'Ganesh Chaturthi',
        '2026-08-25': 'Milad-un-Nabi',
        '2026-10-02': 'MG Jayanti',
        '2026-10-09': 'Dussehra',
        '2026-10-29': 'Diwali Pooja',
        '2026-10-30': 'Balipratipada',
        '2026-11-19': 'Guru Nanak Jayanti',
        '2026-12-25': 'Christmas',
    }
    #Previousday = str(date.today() + timedelta(HolidayDateDifference))
    #print(Previousday)
    for dates in ListOfHolidays:
        #Convert the date sent in the parameter to string , else will return false for different data type comparision
        if dates == str(HolidayDate):
            return True
    
    return False