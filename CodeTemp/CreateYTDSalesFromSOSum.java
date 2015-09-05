public class CreateYTDSalesFromSOSum {

	private static Map<String, List<AccAndStoreSOSummary__c>> AccStoreSOSs;
	private static Map<String, List<CitySOSummary__c>> CitySOSs;

    private static Set<String> AccountIds;
    private static Set<String> RetailStoreIds;
    private static Set<String> Citys;
    private static Set<String> Years;
    private static Set<String> Months;

	private static Map<String, AccAndStoreYTDSales__c> ExistYTDSales;
    private static Map<String, AccAndStoreYTDSales__c> currYTDSales;
	private static Map<String, CityYTDSales__c> ExistCityYTDSales;
    private static Map<String, CityYTDSales__c> currCityYTDSales;

	public static void generateYTDSales(boolean isInsert, boolean isUpdate, boolean isDelete, Map<Id, AccAndStoreSOSummary__c> SOSummarysMap, Map<Id, AccAndStoreSOSummary__c> oldSOSummarysMap){
		AccStoreSOSs = new Map<String, List<AccAndStoreSOSummary__c>>();

        initAccStoreSOSummarys(isInsert, isUpdate, isDelete, SOSummarysMap, oldSOSummarysMap);
        integrateCurrYTDSales();
    }

	public static void generateYTDSales(boolean isInsert, boolean isUpdate, boolean isDelete, Map<Id, CitySOSummary__c> SOSummarysMap, Map<Id, CitySOSummary__c> oldSOSummarysMap){
		CitySOSs = new Map<String, List<CitySOSummary__c>>();

        initCitySOSummarys(isInsert, isUpdate, isDelete, SOSummarysMap, oldSOSummarysMap);
        integrateCurrYTDSales();
    }

	public static void initAccStoreSOSummarys(boolean isInsert, boolean isUpdate, boolean isDelete, Map<Id, AccAndStoreSOSummary__c> SOSummarysMap, Map<Id, AccAndStoreSOSummary__c> oldSOSummarysMap){
		AccountIds = new Set<String>();
        RetailStoreIds = new Set<String>();
        Years = new Set<String>();
        Months = new Set<String>();

		Set<Id> AllAccSOSIds = new Set<Id>();
        AllAccSOSIds.addAll(SOSummarysMap.keySet());
        AllAccSOSIds.addAll(oldSOSummarysMap.keySet());

		AccAndStoreSOSummary__c tempNew;
        AccAndStoreSOSummary__c tempOld;
        List<AccAndStoreSOSummary__c> tempSOS;
        for(Id sosid : AllAccSOSIds){
            tempNew = SOSummarysMap.get(sosid);
            tempOld = oldSOSummarysMap.get(sosid);

			if(tempNew != null){
				if(tempNew.RetailStore__c==null)
                AccountIds.add(tempNew.Account__c);
				if(tempNew.RetailStore__c!=null)
                RetailStoreIds.add(tempNew.RetailStore__c);
                Years.add(tempNew.Year__c);
                Months.add(tempNew.Month__c);

                tempSOS = AccStoreSOSs.get('plus');
                if(tempSOS==null){
                    tempSOS = new List<AccAndStoreSOSummary__c>();
                    AccStoreSOSs.put('plus', tempSOS);
                }
                tempSOS.add(tempNew);

            }
            if(tempOld != null){
				if(tempOld.RetailStore__c==null)
                AccountIds.add(tempOld.Account__c);
				if(tempOld.RetailStore__c!=null)
                RetailStoreIds.add(tempOld.RetailStore__c);
				Years.add(tempOld.Year__c);
                Months.add(tempOld.Month__c);

                tempSOS = AccStoreSOSs.get('minus');
                if(tempSOS==null){
                    tempSOS = new List<AccAndStoreSOSummary__c>();
                    AccStoreSOSs.put('minus', tempSOS);
                }
                tempSOS.add(tempOld);
            }
		}
	}

	public static void initCitySOSummarys(boolean isInsert, boolean isUpdate, boolean isDelete, Map<Id, CitySOSummary__c> SOSummarysMap, Map<Id, CitySOSummary__c> oldSOSummarysMap){
        Citys = new Set<String>();
        Years = new Set<String>();
        Months = new Set<String>();

		Set<Id> AllCitySOSIds = new Set<Id>();
        AllCitySOSIds.addAll(SOSummarysMap.keySet());
        AllCitySOSIds.addAll(oldSOSummarysMap.keySet());

		CitySOSummary__c tempNew;
        CitySOSummary__c tempOld;
        List<CitySOSummary__c> tempSOS;
        for(Id sosid : AllCitySOSIds){
            tempNew = SOSummarysMap.get(sosid);
            tempOld = oldSOSummarysMap.get(sosid);

			if(tempNew != null){
				Citys.add(tempNew.City__c);
                Years.add(tempNew.Year__c);
                Months.add(tempNew.Month__c);

                tempSOS = CitySOSs.get('plus');
                if(tempSOS==null){
                    tempSOS = new List<CitySOSummary__c>();
                    CitySOSs.put('plus', tempSOS);
                }
                tempSOS.add(tempNew);

            }
            if(tempOld != null){
				Citys.add(tempOld.City__c);
				Years.add(tempOld.Year__c);
                Months.add(tempOld.Month__c);

                tempSOS = CitySOSs.get('minus');
                if(tempSOS==null){
                    tempSOS = new List<CitySOSummary__c>();
                    CitySOSs.put('minus', tempSOS);
                }
                tempSOS.add(tempOld);
            }
		}
	}

	public static void initYTDSales(){
		ExistYTDSales = new Map<String, AccAndStoreYTDSales__c>();

        List<AccAndStoreYTDSales__c> YTDSalesList = new List<AccAndStoreYTDSales__c>();
        YTDSalesList = [SELECT Id, Account__c, RetailStore__c, Year__c, Month__c, YearActual__c, YearBudget__c,
						MonthActual__c, MonthBudget__c, YTMActual__c, YTMBudget__c
                         FROM AccAndStoreYTDSales__c
                         WHERE Year__c in :Years
                         AND Month__c in :Months
                         AND(Account__c in :AccountIds
                             OR RetailStore__c in :RetailStoreIds)
                        ];
        for(AccAndStoreYTDSales__c ytd : YTDSalesList){
			if(ytd.RetailStore__c==null)
                ExistYTDSales.put(ytd.Account__c+'&&'+ytd.Year__c+'&&'+ytd.Month__c, ytd);
            else
                ExistYTDSales.put(ytd.RetailStore__c+'&&'+ytd.Year__c+'&&'+ytd.Month__c, ytd);
        }

		Set<String> lastYears = new Set<String>();
		Set<String> lastMonths = new Set<String>();
		for(String y : Years){
			for(String m : Months){
				Date currDate = date.newInstance(Integer.valueOf(y), Integer.valueOf(m), 1);
		        Date lastDate = currDate-1;
		        lastYears.add(lastDate.year()+'');
				lastMonths.add(lastDate.month()+'');
			}
		}

		YTDSalesList = [SELECT Id, Account__c, RetailStore__c, Year__c, Month__c, YearActual__c, YearBudget__c,
						MonthActual__c, MonthBudget__c, YTMActual__c, YTMBudget__c
                         FROM AccAndStoreYTDSales__c
                         WHERE Year__c in :lastYears
                         AND Month__c in :lastMonths
                         AND(Account__c in :AccountIds
                             OR RetailStore__c in :RetailStoreIds)
                        ];
        for(AccAndStoreYTDSales__c ytd : YTDSalesList){
			if(ytd.RetailStore__c==null)
                ExistYTDSales.put(ytd.Account__c+'&&'+ytd.Year__c+'&&'+ytd.Month__c, ytd);
            else
                ExistYTDSales.put(ytd.RetailStore__c+'&&'+ytd.Year__c+'&&'+ytd.Month__c, ytd);
        }
	}

	public static void initCityYTDSales(){
        ExistCityYTDSales = new Map<String, CityYTDSales__c>();

        List<CityYTDSales__c> CityYTDSalesList = new List<CityYTDSales__c>();
        CityYTDSalesList = [SELECT Id, City__c, CityTier__c, Year__c, Month__c, YearActual__c, YearBudget__c,
						MonthActual__c, MonthBudget__c, YTMActual__c, YTMBudget__c
                         FROM CityYTDSales__c
                         WHERE Year__c in :Years
                         AND Month__c in :Months
                         AND City__c in :Citys
                        ];
        for(CityYTDSales__c ytd : CityYTDSalesList){
            ExistCityYTDSales.put(ytd.City__c+'##'+ytd.CityTier__c+'&&'+ytd.Year__c+'&&'+ytd.Month__c, ytd);
        }

		Set<String> lastYears = new Set<String>();
		Set<String> lastMonths = new Set<String>();
		for(String y : Years){
			for(String m : Months){
				Date currDate = date.newInstance(Integer.valueOf(y), Integer.valueOf(m), 1);
		        Date lastDate = currDate-1;
		        lastYears.add(lastDate.year()+'');
				lastMonths.add(lastDate.month()+'');
			}
		}

		CityYTDSalesList = [SELECT Id, City__c, CityTier__c, Year__c, Month__c, YearActual__c, YearBudget__c,
						MonthActual__c, MonthBudget__c, YTMActual__c, YTMBudget__c
                         FROM CityYTDSales__c
                         WHERE Year__c in :lastYears
                         AND Month__c in :lastMonths
                         AND City__c in :Citys
                        ];
        for(CityYTDSales__c ytd : CityYTDSalesList){
            ExistCityYTDSales.put(ytd.City__c+'##'+ytd.CityTier__c+'&&'+ytd.Year__c+'&&'+ytd.Month__c, ytd);
        }
    }

	public static void integrateCurrYTDSales(){
        currYTDSales = new Map<String, AccAndStoreYTDSales__c>();
        currCityYTDSales = new Map<String, CityYTDSales__c>();

        AccAndStoreYTDSales__c accytd;
        AccAndStoreYTDSales__c rtytd;
        CityYTDSales__c cityytd;
		AccAndStoreYTDSales__c lastaccytd;
        AccAndStoreYTDSales__c lastytd;
        CityYTDSales__c lastcityytd;

        Integer abs;
        String keySuffix;

		Date currDate;
		Date lastDate;
		String lastYear;
		String lastMonth;
		String lastKeySuffix;

		for(String key : AccStoreSOSs.keySet()){
			if(key.equals('plus')) abs = 1;
            if(key.equals('minus')) abs = -1;
			for(AccAndStoreSOSummary__c accsos : AccStoreSOSs.get(key)){
				currDate = date.newInstance(Integer.valueOf(accsos.Year__c), Integer.valueOf(accsos.Month__c), 1);
		        lastDate = currDate-1;
		        lastYear = lastDate.year()+'';
		        lastMonth = lastDate.month()+'';
				lastKeySuffix = '&&'+lastYear+'&&'+lastMonth;

				keySuffix = '&&'+accsos.Year__c+'&&'+accsos.Month__c;
				accytd = currYTDSales.get(accsos.Account__c+keySuffix);
                if(accytd==null) accytd = ExistYTDSales.get(accsos.Account__c+keySuffix);
                if(accytd==null) accytd = new AccAndStoreYTDSales__c();
                accytd.Year__c = accsos.Year__c;
                accytd.Month__c = accsos.Month__c;
                accytd.Account__c = accsos.Account__c;
	            if(accytd.MonthActual__c==null) accytd.MonthActual__c = 0;
	            accytd.MonthActual__c += abs*accsos.Amount__c;

				lastaccytd = ExistYTDSales.get(accsos.Account__c+lastKeySuffix);
				if(accytd.YTMActual__c==null && lastaccytd!=null && !lastMonth.equals('1'))
				accytd.YTMActual__c = lastaccytd.YTMActual__c;
                if(accytd.YTMActual__c==null) accytd.YTMActual__c = 0;
                accytd.YTMActual__c += abs*accsos.Amount__c;

				if(accytd.YearActual__c==null && lastaccytd!=null)
				accytd.YearActual__c = lastaccytd.YearActual__c;
                if(accytd.YearActual__c==null) accytd.YearActual__c = 0;
                accytd.YearActual__c += abs*accsos.Amount__c;
                currYTDSales.put(accsos.Account__c+keySuffix, accytd);


				rtytd = currYTDSales.get(accsos.RetailStore__c+keySuffix);
                if(rtytd==null) rtytd = ExistYTDSales.get(accsos.RetailStore__c+keySuffix);
                if(rtytd==null) rtytd = new AccAndStoreYTDSales__c();
                rtytd.Year__c = accsos.Year__c;
                rtytd.Month__c = accsos.Month__c;
                rtytd.RetailStore__c = accsos.RetailStore__c;
	            if(rtytd.MonthActual__c==null) rtytd.MonthActual__c = 0;
	            rtytd.MonthActual__c += abs*accsos.Amount__c;

				lastytd = ExistYTDSales.get(accsos.RetailStore__c+lastKeySuffix);
				if(rtytd.YTMActual__c==null && lastytd!=null && !lastMonth.equals('1'))
				rtytd.YTMActual__c = lastytd.YTMActual__c;
                if(rtytd.YTMActual__c==null) rtytd.YTMActual__c = 0;
                rtytd.YTMActual__c += abs*accsos.YTMActual__c;

				if(rtytd.YearActual__c==null && lastytd!=null)
				rtytd.YearActual__c = lastytd.YearActual__c;
                if(rtytd.YearActual__c==null) rtytd.YearActual__c = 0;
                rtytd.YearActual__c += abs*accsos.YearActual__c;
                currYTDSales.put(accsos.RetailStore__c+keySuffix, rtytd);
			}
		}

		for(String key : CitySOSs.keySet()){
			if(key.equals('plus')) abs = 1;
            if(key.equals('minus')) abs = -1;
			for(CitySOSummary__c citysos : CitySOSs.get(key)){
				currDate = date.newInstance(Integer.valueOf(citysos.Year__c), Integer.valueOf(citysos.Month__c), 1);
		        lastDate = currDate-1;
		        lastYear = lastDate.year()+'';
		        lastMonth = lastDate.month()+'';
				lastKeySuffix = '&&'+lastYear+'&&'+lastMonth;

				keySuffix = '&&'+citysos.Year__c+'&&'+citysos.Month__c;
				cityytd = currCityYTDSales.get(citysos.City__c+'##'+citysos.CityTier__c+keySuffix);
                if(cityytd==null) cityytd = ExistCityYTDSales.get(citysos.City__c+'##'+citysos.CityTier__c+keySuffix);
                if(cityytd==null) cityytd = new CityYTDSales__c();
                cityytd.Year__c = citysos.Year__c;
                cityytd.Month__c = citysos.Month__c;
                cityytd.Account__c = citysos.Account__c;
				if(cityytd.MonthActual__c==null) cityytd.MonthActual__c = 0;
	            cityytd.MonthActual__c += abs*citysos.Amount__c;

				lastcityytd = ExistCityYTDSales.get(citysos.City__c+'##'+citysos.CityTier__c+lastKeySuffix);
				if(cityytd.YTMActual__c==null && lastcityytd!=null && !lastMonth.equals('1'))
				cityytd.YTMActual__c = lastcityytd.YTMActual__c;
                if(cityytd.YTMActual__c==null) cityytd.YTMActual__c = 0;
                cityytd.YTMActual__c += abs*citysos.YTMActual__c;

				if(cityytd.YearActual__c==null && lastytd!=null)
				cityytd.YearActual__c = lastytd.YearActual__c;
                if(cityytd.YearActual__c==null) cityytd.YearActual__c = 0;
                cityytd.YearActual__c += abs*citysos.YearActual__c;
                currCityYTDSales.put(citysos.City__c+'##'+citysos.CityTier__c+keySuffix, cityytd);
			}
		}
	}
}
