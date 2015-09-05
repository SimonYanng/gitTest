public class CreateSOSummarysFromRSOrder {

    private static Map<String, List<RSOrder__c>> acountCodeSOs;
    private static Map<String, List<RSOrder__c>> sOAmountByStores;
    private static Map<String, List<RSOrder__c>> retailStoreSOs;
    private static Map<String, List<RSOrder__c>> citySOs;
    private static Map<String, List<RSOrder__c>> delacountCodeSOs;
    private static Map<String, List<RSOrder__c>> delsOAmountByStores;
    private static Map<String, List<RSOrder__c>> delretailStoreSOs;
    private static Map<String, List<RSOrder__c>> delcitySOs;

    private static Map<String, String> yearAcountCodes;
    private static Map<String, String> yearMonthRSAuthorizedNos;
    private static Map<String, String> yearRSAuthorizedNos;
    private static Map<String, String> yearCitys;

    private static Map<String, SOSummary__c> ExistAcountSOSummarys;
    private static Map<String, Decimal> ExistGiftCount;
    private static Map<String, SOSummary__c> ExistSOAmountByStores;
    private static Map<String, SOSummary__c> ExistRetailStoreSOSummarys;
    private static Map<String, SOSummary__c> ExistCitySOSummarys;

    private static Map<String, SOSummary__c> currentSOSummarys;

    public static void generateSOSummary(boolean isInsert, boolean isUpdate, boolean isDelete,
                                         Map<Id, RSOrder__c> RSOrdersMap, Map<Id, RSOrder__c> oldRSOrdersMap){

        initRSOrder(isInsert, isDelete, RSOrdersMap, oldRSOrdersMap);
        if(yearAcountCodes.size() <= 0 && yearMonthRSAuthorizedNos.size() <=0 && yearRSAuthorizedNos.size() <= 0 && yearCitys.size() <= 0) return;

        initExistSOSummary();

        currentSOSummarys = new Map<String, SOSummary__c>();
        integrateCurrentAcountSOs(isInsert, isDelete);
        integrateCurrentSOAmountByStores(isInsert, isDelete);
        integrateCurrentRetailStoreSOs(isInsert, isDelete);
        integrateCurrentCitySOs(isInsert, isDelete);

        Database.upsert(currentSOSummarys.values(), false);
    }

    public static void initRSOrder(boolean isInsert, boolean isDelete, Map<Id, RSOrder__c> RSOrdersMap, Map<Id, RSOrder__c> oldRSOrdersMap){
        acountCodeSOs = new Map<String, List<RSOrder__c>>();
        sOAmountByStores = new Map<String, List<RSOrder__c>>();
        retailStoreSOs = new Map<String, List<RSOrder__c>>();
        citySOs = new Map<String, List<RSOrder__c>>();
        delacountCodeSOs = new Map<String, List<RSOrder__c>>();
        delsOAmountByStores = new Map<String, List<RSOrder__c>>();
        delretailStoreSOs = new Map<String, List<RSOrder__c>>();
        delcitySOs = new Map<String, List<RSOrder__c>>();

        yearAcountCodes = new Map<String, String>();
        yearMonthRSAuthorizedNos = new Map<String, String>();
        yearRSAuthorizedNos = new Map<String, String>();
        yearCitys = new Map<String, String>();

        String key = '';
        List<RSOrder__c> rsos;
        if(!isDelete){
            List<RSOrder__c> RSOrdersList = new List<RSOrder__c>();
            Set<Id> ids = RSOrdersMap.keySet();
            RSOrdersList = [SELECT Id, SellType__c, SellAmount__c, Quantity__c, Date__c, Month__c,
                            RetailStore__r.Account__c, AccountCode__c, RetailStore__c, RSAuthorizedNo__c,
                            City__c, CityTier__c
                            FROM RSOrder__c
                            WHERE id in :ids
                            AND isHistorySO__c != true
                           ];

            for(RSOrder__c rso : RSOrdersList){
                String rsoAcountCode = rso.AccountCode__c;
                String rsoRSAuthorizedNo = rso.RSAuthorizedNo__c;
                String rsoCity = rso.City__c;
                String rsoYear;
                String rsoMonth;
                Date d = rso.Date__c;
                if(d == null) d= date.today();
                rsoYear = d.year()+'';
                rsoMonth= Util.getEnMonth(d);

                if(String.isNotEmpty(rsoAcountCode)){
                    key = 'AccountSOSum&&'+rsoYear+'##'+rsoAcountCode;
                    yearAcountCodes.put(key, key);
                    rsos = acountCodeSOs.get(key);
                    if(rsos == null) rsos = new List<RSOrder__c>();
                    rsos.add(rso);
                    acountCodeSOs.put(key, rsos);
                }
                if(String.isNotEmpty(rsoRSAuthorizedNo)){
                    key = 'RetailStoreSOSum&&'+rsoYear+'##'+rsoRSAuthorizedNo;
                    yearRSAuthorizedNos.put(key, key);
                    rsos = retailStoreSOs.get(key);
                    if(rsos == null) rsos = new List<RSOrder__c>();
                    rsos.add(rso);
                    retailStoreSOs.put(key, rsos);

                    key = 'SOAmountByStore&&'+rsoYear+'&&'+rsoMonth+'##'+rsoRSAuthorizedNo;
                    yearMonthRSAuthorizedNos.put(key, key);
                    rsos = sOAmountByStores.get(key);
                    if(rsos == null) rsos = new List<RSOrder__c>();
                    rsos.add(rso);
                    sOAmountByStores.put(key, rsos);
                }
                if(String.isNotEmpty(rsoCity)) {
                    key = 'CitySOSum&&'+rsoYear+'##'+rsoCity;
                    yearcitys.put(key, key);
                    rsos = citySOs.get(key);
                    if(rsos == null) rsos = new List<RSOrder__c>();
                    rsos.add(rso);
                    citySOs.put(key, rsos);
                }
            }
        }

        if(!isInsert){
            for(RSOrder__c delso : oldRSOrdersMap.values()){
                if(delso.isHistorySO__c != null && delso.isHistorySO__c) continue;

                String delsoAcountCode = delso.AccountCode__c;
                String delsoRSAuthorizedNo = delso.RSAuthorizedNo__c;
                String delsoCity = delso.City__c;
                String delsoYear;
                String delsoMonth;
                Date d = delso.Date__c;
                if(d == null) d= date.today();
                delsoYear = d.year()+'';
                delsoMonth = Util.getEnMonth(d);

                if(String.isNotEmpty(delsoAcountCode)){
                    key = 'AccountSOSum&&'+delsoYear+'##'+delsoAcountCode;
                    yearAcountCodes.put(key, key);
                    rsos = delacountCodeSOs.get(key);
                    if(rsos == null) rsos = new List<RSOrder__c>();
                    rsos.add(delso);
                    delacountCodeSOs.put(key, rsos);
                }
                if(String.isNotEmpty(delsoRSAuthorizedNo)){
                    key = 'RetailStoreSOSum&&'+delsoYear+'##'+delsoRSAuthorizedNo;
                    yearRSAuthorizedNos.put(key, key);
                    rsos = delretailStoreSOs.get(key);
                    if(rsos == null) rsos = new List<RSOrder__c>();
                    rsos.add(delso);
                    delretailStoreSOs.put(key, rsos);

                    key = 'SOAmountByStore&&'+delsoYear+'&&'+delsoMonth+'##'+delsoRSAuthorizedNo;
                    yearMonthRSAuthorizedNos.put(key, key);
                    rsos = delsOAmountByStores.get(key);
                    if(rsos == null) rsos = new List<RSOrder__c>();
                    rsos.add(delso);
                    delsOAmountByStores.put(key, rsos);
                }
                if(String.isNotEmpty(delsoCity)) {
                    key = 'CitySOSum&&'+delsoYear+'##'+delsoCity;
                    yearcitys.put(key, key);
                    rsos = delcitySOs.get(key);
                    if(rsos == null) rsos = new List<RSOrder__c>();
                    rsos.add(delso);
                    delcitySOs.put(key, rsos);
                }
            }
        }
        ExistGiftCount = new Map<String, Integer>();
        ExistGiftCount = getExistGiftCount(acountCodeSOs);
    }

    public static void initExistSOSummary(){
        if(yearAcountCodes.size() > 0) ExistAcountSOSummarys = getExistSOSummary('Account', yearAcountCodes);
        if(yearRSAuthorizedNos.size() > 0) ExistRetailStoreSOSummarys = getExistSOSummary('RetailStore', yearRSAuthorizedNos);
        if(yearMonthRSAuthorizedNos.size() > 0) ExistSOAmountByStores = getExistSOSummary('SOAmountByStores', yearMonthRSAuthorizedNos);
        if(yearCitys.size() > 0) ExistCitySOSummarys = getExistSOSummary('City', yearCitys);
    }

    public static Map<String, SOSummary__c > getExistSOSummary(String type, Map<String, String> yearCodes){
        Map<String, SOSummary__c> ExistSOSummarys = new Map<String, SOSummary__c>();

        Map<String, List<String>> accountCodes = new Map<String, List<String>>();
        Map<String, List<String>> soaAuthorizedNos = new Map<String, List<String>>();
        Map<String, List<String>> rsAuthorizedNos = new Map<String, List<String>>();
        Map<String, List<String>> citys = new Map<String, List<String>>();

        for(String yc : yearCodes.keySet()){
            List<String> yAndCode = yc.split('##');
            if(yAndCode.size() < 2) continue;
            String year = yAndCode[0].split('&&')[1];

            if(type.equals('Account')){
                List<String> yacc = accountCodes.get(year);
                if(yacc == null) yacc = new List<String>();
                yacc.add(yAndCode[1]);
                accountCodes.put(year, yacc);
            }else if(type.equals('SOAmountByStores')){
                year += '&&'+yAndCode[0].split('&&')[2];
                List<String> ysoas = soaAuthorizedNos.get(year);
                if(ysoas == null) ysoas = new List<String>();
                ysoas.add(yAndCode[1]);
                soaAuthorizedNos.put(year, ysoas);
            }else if(type.equals('RetailStore')){
                List<String> yrs = rsAuthorizedNos.get(year);
                if(yrs == null) yrs = new List<String>();
                yrs.add(yAndCode[1]);
                rsAuthorizedNos.put(year, yrs);
            }else if(type.equals('City')){
                List<String> ycity = citys.get(year);
                if(ycity == null) ycity = new List<String>();
                ycity.add(yAndCode[1]);
                citys.put(year, ycity);
            }
        }

        List<SOSummary__c> SOSummarys = new List<SOSummary__c>();
        if(type.equals('SOAmountByStores')){
            for(String yr : soaAuthorizedNos.keySet()){
                String y = yr.split('&&')[0];
                String m = yr.split('&&')[1];
                List<String> yrs = soaAuthorizedNos.get(yr);
                SOSummarys = [SELECT Id, Year__c, RSAuthorizedNo__c, Month__c, RoadShowAmount__c, SaleAmount__c, ShoppeAmount__c,
                              VIPActivityAmount__c, AnotherAmount__c, SellAmount__c, Quantity__c, GiftCount__c
                              FROM SOSummary__c
                              WHERE Year__c = :y
                              AND Month__c =:m
                              AND Type__c = 'SOAmountByStore'
                              AND RSAuthorizedNo__c in :yrs
                             ];
                for(SOSummary__c sos : SOSummarys){
                    ExistSOSummarys.put('SOAmountByStore&&'+yr+'##'+sos.RSAuthorizedNo__c , sos);
                }
            }
        }else if(type.equals('Account')){
            for(String yr : accountCodes.keySet()){
                List<String> yaccount = accountCodes.get(yr);
                SOSummarys = [SELECT Id, Year__c, AccountCode__c, Month__c, SellAmount__c,
                              Jan__c, Feb__c, Mar__c, Apr__c, May__c, Jun__c, Jul__c, Aug__c, Sep__c, Oct__c, Nov__c, Dec__c
                              FROM SOSummary__c
                              WHERE Year__c = :yr
                              AND Type__c = 'AccountSOSum'
                              AND AccountCode__c in :yaccount
                             ];
                for(SOSummary__c sos : SOSummarys){
                    ExistSOSummarys.put('AccountSOSum&&'+yr+'##'+sos.AccountCode__c, sos);
                }
            }
        }else if(type.equals('RetailStore')){
            for(String yr : rsAuthorizedNos.keySet()){
                List<String> yrs = rsAuthorizedNos.get(yr);
                SOSummarys = [SELECT Id, Year__c, RSAuthorizedNo__c, Month__c, SellAmount__c,
                              Jan__c, Feb__c, Mar__c, Apr__c, May__c, Jun__c, Jul__c, Aug__c, Sep__c, Oct__c, Nov__c, Dec__c
                              FROM SOSummary__c
                              WHERE Year__c = :yr
                              AND Type__c = 'RetailStoreSOSum'
                              AND RSAuthorizedNo__c  in :yrs
                             ];
                for(SOSummary__c sos : SOSummarys){
                    ExistSOSummarys.put('RetailStoreSOSum&&'+yr+'##'+sos.RSAuthorizedNo__c , sos);
                }
            }
        }else if(type.equals('City')){
            for(String yr : citys.keySet()){
                List<String> ycity = citys.get(yr);
                SOSummarys = [SELECT Id, Year__c, City__c, Month__c, SellAmount__c,
                              Jan__c, Feb__c, Mar__c, Apr__c, May__c, Jun__c, Jul__c, Aug__c, Sep__c, Oct__c, Nov__c, Dec__c
                              FROM SOSummary__c
                              WHERE Year__c = :yr
                              AND Type__c = 'CitySOSum'
                              AND City__c in :ycity
                             ];
                for(SOSummary__c sos : SOSummarys){
                    ExistSOSummarys.put('CitySOSum&&'+yr+'##'+sos.City__c, sos);
                }
            }
        }
        return ExistSOSummarys;
    }

    public static Map<String, Decimal> getExistGiftCount(Map<String, List<RSOrder__c>> rsos){
        Map<String, Decimal> giftcount = new Map<String, Decimal>();
        List<String> RetailStoreIds = new List<String>();
        for(String key : rsos.keySet()){
            for(RSOrder__c rso : rsos.get(key)){
                RetailStoreIds.add(rso.Id);
            }
        }
        List<RSOrderItem__c> GiftItems = new List<RSOrderItem__c>();
        GiftItems = [SELECT Id, RSOrder__r.RSAuthorizedNo__c, RSOrder__r.Date__c
                     FROM RSOrderItem__c
                     WHERE RSOrder__c in : RetailStoreIds
                     AND RSorder__r.Active__c = true
                     AND Product__r.Brand__c = '礼盒'
                    ];
        Map<String, List<RSOrderItem__c>> accGiftItems = new Map<String, List<RSOrderItem__c>>();
        String accKey;
        List<RSOrderItem__c> accRSOIs;
        for(RSOrderItem__c gi : GiftItems){

            Date d =  gi.RSOrder__r.Date__c;
            if(d == null) d= date.today();
            String y = d.year()+'';
            String m = Util.getEnMonth(d);

            accKey = 'SOAmountByStore&&'+y+'&&'+m+'##'+gi.RSOrder__r.RSAuthorizedNo__c;
            accRSOIs = accGiftItems.get(accKey);
            if(accRSOIs == null) accRSOIs = new List<RSOrderItem__c>();
            accRSOIs.add(gi);
            accGiftItems.put(accKey, accRSOIs);
        }

        for(String key : accGiftItems.keySet()){
            giftcount.put(key, accGiftItems.get(key).size());
        }

        return giftcount;
    }

    public static void integrateCurrentSOAmountByStores(boolean isInsert, boolean isDelete){
        for(String key : yearMonthRSAuthorizedNos.keySet()){
            List<String> yAndCode = key.split('##');
            if(yAndCode.size()<2) continue;
            yAndCode = yAndCode[0].split('&&');

            boolean isExist = true;
            SOSummary__c soSummary = ExistSOAmountByStores.get(key);
            if(soSummary == null) {isExist = false;soSummary = currentSOSummarys.get(key);}
            if(soSummary == null) soSummary = new SOSummary__c();

            soSummary = verifySOSummary('SOAmountByStore', soSummary);
            currentSOSummarys.put(key, soSummary);

            Decimal giftount = ExistGiftCount.get(key);
            if(giftount == null) giftount = 0;
            soSummary.GiftCount__c = giftount;
            soSummary.Year__c = yAndCode[1];

            List<RSOrder__c> orders;
            String type;
            String month;
            Decimal eachAmount;
            Decimal eachQuantity;
            if(!isDelete){
                orders = sOAmountByStores.get(key);
                if(orders == null) orders = new List<RSOrder__c>();
                for(RSOrder__c order : orders){
                    type = order.SellType__c;
                    eachAmount = order.SellAmount__c;
                    if(eachAmount == null) eachAmount = 0;
                    eachQuantity = order.Quantity__c;
                    if(eachQuantity == null) eachQuantity = 0;

                    month = Util.getEnMonth(order.Date__c);
                    soSummary.Month__c = month;
                    soSummary.Account__c = order.RetailStore__r.Account__c;
                    soSummary.RetailStore__c = order.RetailStore__c;

                    if('路演销售'.equals(type)) soSummary.RoadShowAmount__c += eachAmount;
                    if('特卖区销售'.equals(type)) soSummary.SaleAmount__c += eachAmount;
                    if('专柜正常销售'.equals(type)) soSummary.ShoppeAmount__c += eachAmount;
                    if('VIP销售'.equals(type)) soSummary.VIPActivityAmount__c += eachAmount;
                    if('其他'.equals(type)) soSummary.AnotherAmount__c += eachAmount;

                    soSummary.Quantity__c += eachQuantity;
                    soSummary.SellAmount__c += eachAmount;
                }
            }

            if(!isInsert && isExist){
                orders = delsOAmountByStores.get(key);
                if(orders == null) orders = new List<RSOrder__c>();
                for(RSOrder__c order : orders){
                    type = order.SellType__c;
                    eachAmount = order.SellAmount__c;
                    if(eachAmount == null) eachAmount = 0;
                    eachQuantity = order.Quantity__c;
                    if(eachQuantity == null) eachQuantity = 0;

                    Date d = order.Date__c;
                    if(d == null) d= date.today();
                    month = Util.getEnMonth(d);
                    soSummary.Month__c = month;

                    if('路演销售'.equals(type)) soSummary.RoadShowAmount__c -= eachAmount;
                    if('特卖区销售'.equals(type)) soSummary.SaleAmount__c -= eachAmount;
                    if('专柜正常销售'.equals(type)) soSummary.ShoppeAmount__c -= eachAmount;
                    if('VIP销售'.equals(type)) soSummary.VIPActivityAmount__c -= eachAmount;
                    if('其他'.equals(type)) soSummary.AnotherAmount__c -= eachAmount;

                    soSummary.Quantity__c -= eachQuantity;
                    soSummary.SellAmount__c -= eachAmount;
                }
            }
        }
    }

    public static void integrateCurrentAcountSOs(boolean isInsert, boolean isDelete){
        for(String key : yearAcountCodes.keySet()){
            List<String> yAndCode = key.split('##');
            if(yAndCode.size()<2) continue;
            yAndCode = yAndCode[0].split('&&');

            boolean isExist = true;
            SOSummary__c soSummary = ExistAcountSOSummarys.get(key);
            if(soSummary == null) {isExist = false;soSummary = currentSOSummarys.get(key);}
            if(soSummary == null) soSummary = new SOSummary__c();

            soSummary = verifySOSummary('AccountSOSum', soSummary);
            currentSOSummarys.put(key, soSummary);

            soSummary.Year__c = yAndCode[1];

            List<RSOrder__c> orders;
            String month;
            Decimal eachAmount;
            if(!isDelete){
                orders = acountCodeSOs.get(key);
                if(orders == null) orders = new List<RSOrder__c>();
                for(RSOrder__c order : orders){
                    eachAmount = order.SellAmount__c;
                    if(eachAmount == null) eachAmount = 0;

                    Date d = order.Date__c;
                    if(d == null) d= date.today();
                    month = Util.getEnMonth(d);
                    soSummary.Month__c = month;
                    soSummary.Account__c = order.RetailStore__r.Account__c;

                    if(month.equals('Jan')) soSummary.Jan__c += eachAmount;
                    if(month.equals('Feb')) soSummary.Feb__c += eachAmount;
                    if(month.equals('Mar')) soSummary.Mar__c += eachAmount;
                    if(month.equals('Apr')) soSummary.Apr__c += eachAmount;
                    if(month.equals('May')) soSummary.May__c += eachAmount;
                    if(month.equals('Jun')) soSummary.Jun__c += eachAmount;
                    if(month.equals('Jul')) soSummary.Jul__c += eachAmount;
                    if(month.equals('Aug')) soSummary.Aug__c += eachAmount;
                    if(month.equals('Sep')) soSummary.Sep__c += eachAmount;
                    if(month.equals('Oct')) soSummary.Oct__c += eachAmount;
                    if(month.equals('Nov')) soSummary.Nov__c += eachAmount;
                    if(month.equals('Dec')) soSummary.Dec__c += eachAmount;

                    soSummary.SellAmount__c += eachAmount;
                }
            }

            if(!isInsert && isExist){
                orders = delacountCodeSOs.get(key);
                if(orders == null) orders = new List<RSOrder__c>();
                for(RSOrder__c order : orders){
                    eachAmount = order.SellAmount__c;
                    if(eachAmount == null) eachAmount = 0;

                    Date d = order.Date__c;
                    if(d == null) d= date.today();
                    month = Util.getEnMonth(d);
                    soSummary.Month__c = month;

                    if(month.equals('Jan')) soSummary.Jan__c -= eachAmount;
                    if(month.equals('Feb')) soSummary.Feb__c -= eachAmount;
                    if(month.equals('Mar')) soSummary.Mar__c -= eachAmount;
                    if(month.equals('Apr')) soSummary.Apr__c -= eachAmount;
                    if(month.equals('May')) soSummary.May__c -= eachAmount;
                    if(month.equals('Jun')) soSummary.Jun__c -= eachAmount;
                    if(month.equals('Jul')) soSummary.Jul__c -= eachAmount;
                    if(month.equals('Aug')) soSummary.Aug__c -= eachAmount;
                    if(month.equals('Sep')) soSummary.Sep__c -= eachAmount;
                    if(month.equals('Oct')) soSummary.Oct__c -= eachAmount;
                    if(month.equals('Nov')) soSummary.Nov__c -= eachAmount;
                    if(month.equals('Dec')) soSummary.Dec__c -= eachAmount;

                    soSummary.SellAmount__c -= eachAmount;
                }
            }
        }
    }

    public static void integrateCurrentRetailStoreSOs(boolean isInsert, boolean isDelete){
        for(String key : yearRSAuthorizedNos.keySet()){
            List<String> yAndCode = key.split('##');
            if(yAndCode.size()<2) continue;
            yAndCode = yAndCode[0].split('&&');

            boolean isExist = true;
            SOSummary__c soSummary = ExistRetailStoreSOSummarys.get(key);
            if(soSummary == null) {isExist = false;soSummary = currentSOSummarys.get(key);}
            if(soSummary == null) soSummary = new SOSummary__c();

            soSummary = verifySOSummary('RetailStoreSOSum', soSummary);
            currentSOSummarys.put(key, soSummary);

            soSummary.Year__c = yAndCode[1];

            List<RSOrder__c> orders;
            String month;
            Decimal eachAmount;
            if(!isDelete){
                orders = retailStoreSOs.get(key);
                if(orders == null) orders = new List<RSOrder__c>();
                for(RSOrder__c order : orders){
                    eachAmount = order.SellAmount__c;
                    if(eachAmount == null) eachAmount = 0;

                    Date d = order.Date__c;
                    if(d == null) d= date.today();
                    month = Util.getEnMonth(d);
                    soSummary.Month__c = month;
                    soSummary.Account__c = order.RetailStore__r.Account__c;
                    soSummary.RetailStore__c = order.RetailStore__c;

                    if(month.equals('Jan')) soSummary.Jan__c += eachAmount;
                    if(month.equals('Feb')) soSummary.Feb__c += eachAmount;
                    if(month.equals('Mar')) soSummary.Mar__c += eachAmount;
                    if(month.equals('Apr')) soSummary.Apr__c += eachAmount;
                    if(month.equals('May')) soSummary.May__c += eachAmount;
                    if(month.equals('Jun')) soSummary.Jun__c += eachAmount;
                    if(month.equals('Jul')) soSummary.Jul__c += eachAmount;
                    if(month.equals('Aug')) soSummary.Aug__c += eachAmount;
                    if(month.equals('Sep')) soSummary.Sep__c += eachAmount;
                    if(month.equals('Oct')) soSummary.Oct__c += eachAmount;
                    if(month.equals('Nov')) soSummary.Nov__c += eachAmount;
                    if(month.equals('Dec')) soSummary.Dec__c += eachAmount;

                    soSummary.SellAmount__c += eachAmount;
                }
            }

            if(!isInsert && isExist){
                orders = delretailStoreSOs.get(key);
                if(orders == null) orders = new List<RSOrder__c>();
                for(RSOrder__c order : orders){
                    eachAmount = order.SellAmount__c;
                    if(eachAmount == null) eachAmount = 0;

                    Date d = order.Date__c;
                    if(d == null) d= date.today();
                    month = Util.getEnMonth(d);
                    soSummary.Month__c = month;

                    if(month.equals('Jan')) soSummary.Jan__c -= eachAmount;
                    if(month.equals('Feb')) soSummary.Feb__c -= eachAmount;
                    if(month.equals('Mar')) soSummary.Mar__c -= eachAmount;
                    if(month.equals('Apr')) soSummary.Apr__c -= eachAmount;
                    if(month.equals('May')) soSummary.May__c -= eachAmount;
                    if(month.equals('Jun')) soSummary.Jun__c -= eachAmount;
                    if(month.equals('Jul')) soSummary.Jul__c -= eachAmount;
                    if(month.equals('Aug')) soSummary.Aug__c -= eachAmount;
                    if(month.equals('Sep')) soSummary.Sep__c -= eachAmount;
                    if(month.equals('Oct')) soSummary.Oct__c -= eachAmount;
                    if(month.equals('Nov')) soSummary.Nov__c -= eachAmount;
                    if(month.equals('Dec')) soSummary.Dec__c -= eachAmount;

                    soSummary.SellAmount__c -= eachAmount;
                }
            }
        }
    }

    public static void integrateCurrentCitySOs(boolean isInsert, boolean isDelete){
        for(String key : yearCitys.keySet()){
            List<String> yAndCode = key.split('##');
            if(yAndCode.size()<2) continue;
            yAndCode = yAndCode[0].split('&&');

            boolean isExist = true;
            SOSummary__c soSummary = ExistCitySOSummarys.get(key);
            if(soSummary == null) {isExist = false;soSummary = currentSOSummarys.get(key);}
            if(soSummary == null) soSummary = new SOSummary__c();

            soSummary = verifySOSummary('CitySOSum', soSummary);
            currentSOSummarys.put(key, soSummary);

            soSummary.Year__c = yAndCode[1];

            List<RSOrder__c> orders;
            String month;
            Decimal eachAmount;
            if(!isDelete){
                orders = citySOs.get(key);
                if(orders == null) orders = new List<RSOrder__c>();
                for(RSOrder__c order : orders){
                    eachAmount = order.SellAmount__c;
                    if(eachAmount == null) eachAmount = 0;

                    Date d = order.Date__c;
                    if(d == null) d= date.today();
                    month = Util.getEnMonth(d);
                    soSummary.Month__c = month;
                    soSummary.Account__c = order.RetailStore__r.Account__c;
                    soSummary.City__c = order.City__c;
                    soSummary.CityTier__c = order.CityTier__c;

                    if(month.equals('Jan')) soSummary.Jan__c += eachAmount;
                    if(month.equals('Feb')) soSummary.Feb__c += eachAmount;
                    if(month.equals('Mar')) soSummary.Mar__c += eachAmount;
                    if(month.equals('Apr')) soSummary.Apr__c += eachAmount;
                    if(month.equals('May')) soSummary.May__c += eachAmount;
                    if(month.equals('Jun')) soSummary.Jun__c += eachAmount;
                    if(month.equals('Jul')) soSummary.Jul__c += eachAmount;
                    if(month.equals('Aug')) soSummary.Aug__c += eachAmount;
                    if(month.equals('Sep')) soSummary.Sep__c += eachAmount;
                    if(month.equals('Oct')) soSummary.Oct__c += eachAmount;
                    if(month.equals('Nov')) soSummary.Nov__c += eachAmount;
                    if(month.equals('Dec')) soSummary.Dec__c += eachAmount;

                    soSummary.SellAmount__c += eachAmount;
                }
            }

            if(!isInsert && isExist){
                orders = delcitySOs.get(key);
                if(orders == null) orders = new List<RSOrder__c>();
                for(RSOrder__c order : orders){
                    eachAmount = order.SellAmount__c;
                    if(eachAmount == null) eachAmount = 0;

                    Date d = order.Date__c;
                    if(d == null) d= date.today();
                    month = Util.getEnMonth(d);
                    soSummary.Month__c = month;

                    if(month.equals('Jan')) soSummary.Jan__c -= eachAmount;
                    if(month.equals('Feb')) soSummary.Feb__c -= eachAmount;
                    if(month.equals('Mar')) soSummary.Mar__c -= eachAmount;
                    if(month.equals('Apr')) soSummary.Apr__c -= eachAmount;
                    if(month.equals('May')) soSummary.May__c -= eachAmount;
                    if(month.equals('Jun')) soSummary.Jun__c -= eachAmount;
                    if(month.equals('Jul')) soSummary.Jul__c -= eachAmount;
                    if(month.equals('Aug')) soSummary.Aug__c -= eachAmount;
                    if(month.equals('Sep')) soSummary.Sep__c -= eachAmount;
                    if(month.equals('Oct')) soSummary.Oct__c -= eachAmount;
                    if(month.equals('Nov')) soSummary.Nov__c -= eachAmount;
                    if(month.equals('Dec')) soSummary.Dec__c -= eachAmount;

                    soSummary.SellAmount__c -= eachAmount;
                }
            }
        }
    }

    public static SOSummary__c verifySOSummary(String type, SOSummary__c sos){

        if(String.isEmpty(sos.Year__c)) sos.Year__c = date.today().year()+'';
        if(String.isEmpty(sos.Month__c)) sos.Month__c = Util.getEnMonth(date.today());
        if(sos.SellAmount__c == null) sos.SellAmount__c = 0;
        sos.Type__c = type;

        if(type.equals('SOAmountByStore')){
            if(sos.RoadShowAmount__c == null) sos.RoadShowAmount__c = 0;
            if(sos.SaleAmount__c == null) sos.SaleAmount__c = 0;
            if(sos.ShoppeAmount__c == null) sos.ShoppeAmount__c = 0;
            if(sos.VIPActivityAmount__c == null) sos.VIPActivityAmount__c = 0;
            if(sos.AnotherAmount__c == null) sos.AnotherAmount__c = 0;
            if(sos.Quantity__c == null) sos.Quantity__c = 0;
            if(sos.GiftCount__c == null) sos.GiftCount__c = 0;
        }else {
            if(sos.Jan__c == null) sos.Jan__c = 0;
            if(sos.Feb__c == null) sos.Feb__c = 0;
            if(sos.Mar__c == null) sos.Mar__c = 0;
            if(sos.Apr__c == null) sos.Apr__c = 0;
            if(sos.May__c == null) sos.May__c = 0;
            if(sos.Jun__c == null) sos.Jun__c = 0;
            if(sos.Jul__c == null) sos.Jul__c = 0;
            if(sos.Aug__c == null) sos.Aug__c = 0;
            if(sos.Sep__c == null) sos.Sep__c = 0;
            if(sos.Oct__c == null) sos.Oct__c = 0;
            if(sos.Nov__c == null) sos.Nov__c = 0;
            if(sos.Dec__c == null) sos.Dec__c = 0;
        }

        return sos;
    }
}
