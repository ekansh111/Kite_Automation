  
BankNiftyContractWidth = 100
NiftyContractWidth = 50
FinNiftyContractWidth = 50
MidCPNiftyContractWidth = 25

def ContractStrikeValue(ContractStrikeFromATMPercent,ATM_ltp,IndexName):
  
    if IndexName == 'BANKNIFTY':
        ATM_CE_Strike = round(int(ATM_ltp*((100+int(ContractStrikeFromATMPercent))/100)))
        ATM_PE_Strike = round(int(ATM_ltp*((100-int(ContractStrikeFromATMPercent))/100)))  

        ATM_CE_Strike = round(float(ATM_CE_Strike/BankNiftyContractWidth)) * BankNiftyContractWidth
        ATM_PE_Strike = round(float(ATM_PE_Strike/BankNiftyContractWidth)) * BankNiftyContractWidth

    elif IndexName == 'NIFTY':
        ATM_CE_Strike = round(int(ATM_ltp*((100+int(ContractStrikeFromATMPercent))/100)))
        ATM_PE_Strike = round(int(ATM_ltp*((100-int(ContractStrikeFromATMPercent))/100)))  

        ATM_CE_Strike = round(float(ATM_CE_Strike/NiftyContractWidth)) * NiftyContractWidth
        ATM_PE_Strike = round(float(ATM_PE_Strike/NiftyContractWidth)) * NiftyContractWidth       
        

    elif IndexName == 'FINNIFTY':
        ATM_CE_Strike = round(int(ATM_ltp*((100+int(ContractStrikeFromATMPercent))/100)))
        ATM_PE_Strike = round(int(ATM_ltp*((100-int(ContractStrikeFromATMPercent))/100)))  

        ATM_CE_Strike = round(float(ATM_CE_Strike/FinNiftyContractWidth)) * FinNiftyContractWidth
        ATM_PE_Strike = round(float(ATM_PE_Strike/FinNiftyContractWidth)) * FinNiftyContractWidth

    elif IndexName == 'MIDCPNIFTY':
        ATM_CE_Strike = round(int(ATM_ltp*((100+int(ContractStrikeFromATMPercent))/100)))
        ATM_PE_Strike = round(int(ATM_ltp*((100-int(ContractStrikeFromATMPercent))/100)))  

        ATM_CE_Strike = round(float(ATM_CE_Strike/MidCPNiftyContractWidth)) * MidCPNiftyContractWidth
        ATM_PE_Strike = round(float(ATM_PE_Strike/MidCPNiftyContractWidth)) * MidCPNiftyContractWidth
    
    print('Contract Strike Value Function',ATM_ltp,ATM_CE_Strike,ATM_PE_Strike) 
    return ATM_CE_Strike,ATM_PE_Strike

if __name__ == '__main__':
    ContractStrikeValue(5,51000, 'BANKNIFTY')
    ContractStrikeValue(1,17030, 'NIFTY')
    ContractStrikeValue(3,17030, 'NIFTY')
    ContractStrikeValue(0,20030, 'FINNIFTY')
    ContractStrikeValue(3,20051, 'FINNIFTY')
    ContractStrikeValue(0,20030, 'MIDCPNIFTY')
    ContractStrikeValue(3,20051, 'MIDCPNIFTY')