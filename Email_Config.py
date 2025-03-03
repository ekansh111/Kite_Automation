from datetime import datetime, timedelta

subject = "Test"
body = "Test for the python script"
sender_email = "bdv1121@gmail.com"
recipient_email = "ekansh.n@gmail.com"
sender_password = "lyephgcockzwkreo"
smtp_server = 'smtp.gmail.com'
smtp_port = 465

# Get today's date in 'YYYY-MM-DD' format
today_date = datetime.today().strftime('%Y-%m-%d')

DadMailDetails = {'To':'nararush@yahoo.com', 'From':'ekansh.n111@gmail.com', 'SenderPassword':'sgwl lnvt hewf wplo',
               'SMTPMail':'smtp.gmail.com','PortNo':'465',
               'Subject':f'Intraday Stocks for Trade {today_date}',
               'Body':'List of companies for long and short orders attached below \n\n Best Regards \n Ekansh'}


VMailDetails = {'To':'varunipherle@gmail.com', 'From':'ekansh.n111@gmail.com', 'SenderPassword':'sgwl lnvt hewf wplo',
               'SMTPMail':'smtp.gmail.com','PortNo':'465',
               'Subject':f'Intraday Stocks for Trade {today_date}',
               'Body':'Dear BestFriend\n\nList of companies for long and short orders attached below(Top 5 for long/Top 10 for short) \n\nBest Regards \nEkansh'}


EkanshMailDetails = {'To':'ekansh.n@gmail.com', 'From':'ekansh.n111@gmail.com', 'SenderPassword':'sgwl lnvt hewf wplo',
               'SMTPMail':'smtp.gmail.com','PortNo':'465',
               'Subject':f'Intraday Stocks for Trade {today_date}',
               'Body':'List of companies for long and short orders attached below \n\nBest Regards \nEkansh'}


DadMailDetailsAboveMACDSignal = {'To':'nararush@yahoo.com', 'From':'ekansh.n111@gmail.com', 'SenderPassword':'sgwl lnvt hewf wplo',
               'SMTPMail':'smtp.gmail.com','PortNo':'465',
               'Subject':f'Intraday Stocks for Trade {today_date}',
               'Body':'List of companies for long and short orders attached below \n\nCurrently the Index is bullish(Daily MACD +ve), not recommended to Short \n\nBest Regards \n Ekansh'}


VMailDetailsAboveMACDSignal = {'To':'varunipherle@gmail.com', 'From':'ekansh.n111@gmail.com', 'SenderPassword':'sgwl lnvt hewf wplo',
               'SMTPMail':'smtp.gmail.com','PortNo':'465',
               'Subject':f'Intraday Stocks for Trade {today_date}',
               'Body':'Dear BestFriend\n\nList of companies for long and short orders attached below(Top 5 for long/Top 10 for short) \n\nCurrently the Index is bullish, not recommended to Short \n\nBest Regards \nEkansh'}


EkanshMailDetailsAboveMACDSignal = {'To':'ekansh.n@gmail.com', 'From':'ekansh.n111@gmail.com', 'SenderPassword':'sgwl lnvt hewf wplo',
               'SMTPMail':'smtp.gmail.com','PortNo':'465',
               'Subject':f'Intraday Stocks for Trade {today_date}',
               'Body':'List of companies for long and short orders attached below \n\nCurrently the Index is bullish(Daily MACD +ve), not recommended to Short \n\nBest Regards \nEkansh'}