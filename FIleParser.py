import pandas as pd
from os import walk
from os import path
import dateparser

class simpleparser():
    
    global pdOne
    global f
    
    def simpleFunction(filePath):
        parsed_file = pd.read_csv(filePath, delimiter='|', names=["REFERENCEPOOLID","LOANIDENTIFIER","MONTHLYREPORTINGPERIOD","ORIGINATIONCHANNEL","SELLERNAME","SERVICERNAME","MASTERSERVICER","ORIGINALINTERESTRATE","CURRENTINTERESTRATE","ORIGINALUPB","UPBATISSUANCE","CURRENTACTUALUPB","ORIGINALLOANTERM","ORIGINATIONDATE","FIRSTPAYMENTDATE","LOANAGE","REMAININGMONTHSTOLEGALMATURITY","ADJUSTEDMONTHSTOMATURITY","MATURITYDATE","ORIGINALLOANTOVALUERATIO(LTV)","ORIGINALCOMBINEDLOANTOVALUERATIO(CLTV)","NUMBEROFBORROWERS","ORIGINALDEBTTOINCOMERATIO","BORROWERCREDITSCOREATORIGINATION","CO-BORROWERCREDITSCOREATORIGINATION","FIRSTTIMEHOMEBUYERINDICATOR","LOANPURPOSE","PROPERTYTYPE","NUMBEROFUNITS","OCCUPANCYTYPE","PROPERTYSTATE","METROPOLITANSTATISTICALAREA(MSA)","ZIPCODESHORT","PRIMARYMORTGAGEINSURANCEPERCENT","PRODUCTTYPE","PREPAYMENTPREMIUMMORTGAGEFLAG","INTERESTONLYINDICATOR","FIRSTPRINCIPALANDINTERESTPAYMENTDATEFORINTERESTONLYPRODUCTS","MONTHSTOAMORTIZATIONFORINTERESTONLYPRODUCTS","CURRENTLOANDELINQUENCYSTATUS","LOANPAYMENTHISTORY","MODIFICATIONFLAG","MORTGAGEINSURANCECANCELLATIONINDICATOR","ZEROBALANCECODE","ZEROBALANCEEFFECTIVEDATE","UPBATTHETIMEOFREMOVALFROMTHEREFERENCEPOOL","REPURCHASEDATE","SCHEDULEDPRINCIPALCURRENT","TOTALPRINCIPALCURRENT","UNSCHEDULEDPRINCIPALCURRENT","LASTPAIDINSTALLMENTDATE","FORECLOSUREDATE","DISPOSITIONDATE","FORECLOSURECOSTS","PROPERTYPRESERVATIONANDREPAIRCOSTS","ASSETRECOVERYCOSTS","MISCELLANEOUSHOLDINGEXPENSESANDCREDITS","ASSOCIATEDTAXESFORHOLDINGPROPERTY","NETSALESPROCEEDS","CREDITENHANCEMENTPROCEEDS","REPURCHASESMAKEWHOLEPROCEEDS","OTHERFORECLOSUREPROCEEDS","NON-INTERESTBEARINGUPB","PRINCIPALFORGIVENESSAMOUNT","ORIGINALLISTSTARTDATE","ORIGINALLISTPRICE","CURRENTLISTSTARTDATE","CURRENTLISTPRICE","BORROWERCREDITSCOREAT-ISSUANCE","CO-BORROWERCREDITSCOREAT-ISSUANCE","BORROWERCREDITSCORECURRENT","CO-BORROWERCREDITSCORECURRENT","MORTGAGEINSURANCETYPE","SERVICINGACTIVITYINDICATOR","CURRENTPERIODMODIFICATIONLOSSAMOUNT","CUMULATIVEMODIFICATIONLOSSAMOUNT","CURRENTPERIODCREDITEVENTNETGAINORLOSS","CUMULATIVECREDITEVENTNETGAINORLOSS","HOMEREADYPROGRAMINDICATOR","FORECLOSUREPRINCIPALWRITE-OFFAMOUNT","RELOCATIONMORTGAGEINDICATOR","ZEROBALANCECODECHANGEDATE","LOANHOLDBACKINDICATOR","LOANHOLDBACKEFFECTIVEDATE","DELINQUENTACCRUEDINTEREST","PROPERTYINSPECTIONWAIVERINDICATOR","HIGHBALANCELOANINDICATOR","ARM<=5YRINDICATOR","ARMPRODUCTTYPE","MONTHSUNTILFIRSTPAYMENTRESET","MONTHSBETWEENSUBSEQUENTPAYMENTRESET","INTERESTRATECHANGEDATE","PAYMENTCHANGEDATE","ARMINDEX","ARMCAPSTRUCTURE","INITIALINTERESTRATECAP","PERIODICINTERESTRATECAP","LIFETIMEINTERESTRATECAP","MARGIN","BALLOONINDICATOR","PLANNUMBER","FORBEARANCEINDICATOR","HIGHLOAN-TO-VALUE(HLTV)REFINANCEOPTIONINDICATOR","DEALNAME"])
        return parsed_file
    
    def loadValues():
        simpleparser.defineFiles();
        print('Parsing file on init, # of files (' + str(len(simpleparser.f)) + ')')
        simpleparser.pdOne = pd.DataFrame()
        
        exists = path.isfile('cached_CIRTDealData.pkl')
        
        if exists:
                simpleparser.pdOne = pd.read_pickle('cached_CIRTDealData.pkl')
        else: 
            for key,name in simpleparser.f.items():
                print('Parsing file (' + name + ') -> # (' + str(key) + ')')
                if (simpleparser.pdOne.empty) :
                    simpleparser.pdOne = simpleparser.simpleFunction('files/' + name)
                simpleparser.pdOne = simpleparser.pdOne.append(simpleparser.simpleFunction('files/' + name))
                
            simpleparser.pdOne["MONTHLYREPORTINGPERIOD"] = pd.to_datetime(simpleparser.pdOne["MONTHLYREPORTINGPERIOD"], format='%m%Y')
            simpleparser.pdOne.to_pickle('cached_CIRTDealData.pkl')
            
    def getPdOne():
        return simpleparser.pdOne[["REFERENCEPOOLID","LOANIDENTIFIER"]]
    
    def defineFiles():
        simpleparser.f = {}
        count = 0
        for (root, dirs, filenames) in walk('files/'):
            for year in ['2019.csv','2018.csv','2017.csv']:
                for name in filenames:
                    #print(year)
                    if year in name:
                        count += 1
                        print("Found -> " + name + ' Count = ' + str(count))
                        simpleparser.f.update({count : name})
            break    

#simpleparser.loadValues()
#print(simpleparser.getPdOne()["REFERENCE POOL ID"].unique().tolist())
#simpleparser.defineFiles()
#simpleparser.loadValues()
#print(simpleparser.pdOne["REFERENCE POOL ID"])
#print(simpleparser.pdOne[simpleparser.pdOne["REFERENCEPOOLID"] == 1462].groupby(["MONTHLYREPORTINGPERIOD"],group_keys=True)["CURRENTACTUALUPB"].agg("sum").to_json())
#print(pd.to_datetime(simpleparser.pdOne["MONTHLY REPORTING PERIOD"].unique(), format='%m%Y').strftime('%B %Y').tolist())
#simpleparser.pdOne = simpleparser.pdOne.set_index(["LOAN IDENTIFIER"])
#simpleparser.pdOne.reset_index()
#print(simpleparser.pdOne[["LOAN IDENTIFIER","REFERENCE POOL ID"]].to_json(orient='records'))
#value = "LOAN IDENTIFIER","REFERENCE POOL ID"
#print(list(value))
#print(simpleparser.pdOne[["LOAN IDENTIFIER","REFERENCE POOL ID"]].to_json(orient='records'))
#print(simpleparser.pdOne["MONTHLY REPORTING PERIOD"])
#print(dateparser.parse("January 2018"))
#print(simpleparser.pdOne["MONTHLY REPORTING PERIOD"].sort_values(ascending=True).unique())