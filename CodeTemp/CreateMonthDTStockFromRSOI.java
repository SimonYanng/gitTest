public class CreateMonthDTStockFromRSOI {

    private static Set<String> acc_pro_ymkeys;
    private static Set<String> acc_brand_series_ymkeys;
    private static Map<String, List<RSOrderItem__c>> accproDTStocks;
    private static Map<String, List<RSOrderItem__c>> delaccproDTStocks;
    private static Map<String, List<RSOrderItem__c>> accbrandseriesDTStocks;
    private static Map<String, List<RSOrderItem__c>> delaccbrandseriesDTStocks;

    private static Map<String, Product__c> ExistProducts;
    private static Map<String, Account> ExistAccounts;
    private static Map<String, Stock__c> ExistStocks;
    private static Map<String, List<Stock__c>> ExistBrandStocks;
    private static Map<String, DTStock__c> ExistDTStocks;

    private static Map<String, Decimal> SOAvgsOf12Month;

    private static Map<String, DTStock__c> currentDTStocks;

    public static void generateDTStocks(String fromSobj, boolean isInsert, boolean isUpdate, boolean isDelete, Map<Id, RSOrderItem__c> RSOrderItemsMap, Map<Id, RSOrderItem__c> oldRSOrderItemsMap){
        acc_pro_ymkeys = new Set<String>();
        acc_brand_series_ymkeys = new Set<String>();
        initRSOrderItems(fromSobj, isInsert, isUpdate, isDelete, RSOrderItemsMap, oldRSOrderItemsMap);
        create(isInsert, isUpdate, isDelete);
    }

    public static void initRSOrderItems(String fromSobj, boolean isInsert, boolean isUpdate, boolean isDelete, Map<Id, RSOrderItem__c> RSOrderItemsMap, Map<Id, RSOrderItem__c> oldRSOrderItemsMap){

        accproDTStocks = new Map<String, List<RSOrderItem__c>>();
        delaccproDTStocks = new Map<String, List<RSOrderItem__c>>();
        accbrandseriesDTStocks = new Map<String, List<RSOrderItem__c>>();
        delaccbrandseriesDTStocks = new Map<String, List<RSOrderItem__c>>();
        List<RSOrderItem__c> accproRSIList;
        List<RSOrderItem__c> accbrandseriesRSIList;

        String key;
        Date d;
        boolean active;
        boolean oldActive;
        if(isInsert){
            for(RSOrderItem__c rsi : RSOrderItemsMap.values()){
                if(!rsi.Active__c || rsi.isTest__c) continue;

                d = rsi.Date__c;
                if(d == null) d = rsi.CreatedDate.date();
                key = rsi.AccountCode__c+'%%'+rsi.ProductCode__c+'&&'+d.year()+'&&'+d.month();

                accproRSIList = accproDTStocks.get(key);
                if(accproRSIList == null) accproRSIList = new List<RSOrderItem__c>();
                accproRSIList.add(rsi);
                accproDTStocks.put(key, accproRSIList);
                acc_pro_ymkeys.add(key);

                key = rsi.AccountCode__c+'%%'+rsi.ProductBrand__c+'%%'+rsi.ProductSeries__c+'&&'+d.year()+'&&'+d.month();
                accbrandseriesRSIList = accbrandseriesDTStocks.get(key);
                if(accbrandseriesRSIList == null) accbrandseriesRSIList = new List<RSOrderItem__c>();
                accbrandseriesRSIList.add(rsi);
                accbrandseriesDTStocks.put(key, accbrandseriesRSIList);
                acc_brand_series_ymkeys.add(key);
            }
        }
        if(isUpdate){
            for(RSOrderItem__c rsi : RSOrderItemsMap.values()){
                if(rsi.isTest__c) continue;
                RSOrderItem__c oldrsi = oldRSOrderItemsMap.get(rsi.Id);
                active = rsi.Active__c;
                oldActive = oldrsi.Active__c;

                d = rsi.Date__c;
                if(d == null) d = rsi.CreatedDate.date();
                key = rsi.AccountCode__c+'%%'+rsi.ProductCode__c+'&&'+d.year()+'&&'+d.month();
                if(active || oldActive != active) acc_pro_ymkeys.add(key);

                if(fromSobj.equals('RSOrder__c')){
                    acc_pro_ymkeys.add(key);
                    //active-->inactive 减掉旧的数量即可
                    accproRSIList = delaccproDTStocks.get(key);
                    if(accproRSIList == null) accproRSIList = new List<RSOrderItem__c>();
                    accproRSIList.add(oldrsi);
                    delaccproDTStocks.put(key, accproRSIList);

                    key = rsi.AccountCode__c+'%%'+rsi.ProductBrand__c+'%%'+rsi.ProductSeries__c+'&&'+d.year()+'&&'+d.month();
                    acc_brand_series_ymkeys.add(key);
                    accbrandseriesRSIList = delaccbrandseriesDTStocks.get(key);
                    if(accbrandseriesRSIList == null) accbrandseriesRSIList = new List<RSOrderItem__c>();
                    accbrandseriesRSIList.add(oldrsi);
                    delaccbrandseriesDTStocks.put(key, accbrandseriesRSIList);
                    continue;
                }

                if((oldActive && !active)){
                    //active-->inactive 减掉旧的数量即可
                    accproRSIList = delaccproDTStocks.get(key);
                    if(accproRSIList == null) accproRSIList = new List<RSOrderItem__c>();
                    accproRSIList.add(oldrsi);
                    delaccproDTStocks.put(key, accproRSIList);

                    key = rsi.AccountCode__c+'%%'+rsi.ProductBrand__c+'%%'+rsi.ProductSeries__c+'&&'+d.year()+'&&'+d.month();
                    acc_brand_series_ymkeys.add(key);
                    accbrandseriesRSIList = delaccbrandseriesDTStocks.get(key);
                    if(accbrandseriesRSIList == null) accbrandseriesRSIList = new List<RSOrderItem__c>();
                    accbrandseriesRSIList.add(oldrsi);
                    delaccbrandseriesDTStocks.put(key, accbrandseriesRSIList);
                }else if(!oldActive && active){
                    //active-->inactive 加上新的即可
                    accproRSIList = accproDTStocks.get(key);
                    if(accproRSIList == null) accproRSIList = new List<RSOrderItem__c>();
                    accproRSIList.add(rsi);
                    accproDTStocks.put(key, accproRSIList);
                    acc_pro_ymkeys.add(key);

                    key = rsi.AccountCode__c+'%%'+rsi.ProductBrand__c+'%%'+rsi.ProductSeries__c+'&&'+d.year()+'&&'+d.month();
                    accbrandseriesRSIList = accbrandseriesDTStocks.get(key);
                    if(accbrandseriesRSIList == null) accbrandseriesRSIList = new List<RSOrderItem__c>();
                    accbrandseriesRSIList.add(rsi);
                    accbrandseriesDTStocks.put(key, accbrandseriesRSIList);
                    acc_brand_series_ymkeys.add(key);
                }

                if(oldActive == active && active){
                    accproRSIList = accproDTStocks.get(key);
                    if(accproRSIList == null) accproRSIList = new List<RSOrderItem__c>();
                    accproRSIList.add(rsi);
                    accproDTStocks.put(key, accproRSIList);

                    accproRSIList = delaccproDTStocks.get(key);
                    if(accproRSIList == null) accproRSIList = new List<RSOrderItem__c>();
                    accproRSIList.add(oldrsi);
                    delaccproDTStocks.put(key, accproRSIList);


                    key = rsi.AccountCode__c+'%%'+rsi.ProductBrand__c+'%%'+rsi.ProductSeries__c+'&&'+d.year()+'&&'+d.month();
                    acc_brand_series_ymkeys.add(key);
                    accbrandseriesRSIList = accbrandseriesDTStocks.get(key);
                    if(accbrandseriesRSIList == null) accbrandseriesRSIList = new List<RSOrderItem__c>();
                    accbrandseriesRSIList.add(rsi);
                    accbrandseriesDTStocks.put(key, accbrandseriesRSIList);

                    accbrandseriesRSIList = delaccbrandseriesDTStocks.get(key);
                    if(accbrandseriesRSIList == null) accbrandseriesRSIList = new List<RSOrderItem__c>();
                    accbrandseriesRSIList.add(oldrsi);
                    delaccbrandseriesDTStocks.put(key, accbrandseriesRSIList);
                }
            }
        }
        if(isDelete){
            for(RSOrderItem__c oldrsi : oldRSOrderItemsMap.values()){
                if(!oldrsi.Active__c || oldrsi.isTest__c) continue;

                d = oldrsi.Date__c;
                if(d == null) d = oldrsi.CreatedDate.date();
                key = oldrsi.AccountCode__c+'%%'+oldrsi.ProductCode__c+'&&'+d.year()+'&&'+d.month();

                accproRSIList = delaccproDTStocks.get(key);
                if(accproRSIList == null) accproRSIList = new List<RSOrderItem__c>();
                accproRSIList.add(oldrsi);
                delaccproDTStocks.put(key, accproRSIList);
                acc_pro_ymkeys.add(key);

                key = oldrsi.AccountCode__c+'%%'+oldrsi.ProductBrand__c+'%%'+oldrsi.ProductSeries__c+'&&'+d.year()+'&&'+d.month();
                acc_brand_series_ymkeys.add(key);
                accbrandseriesRSIList = delaccbrandseriesDTStocks.get(key);
                if(accbrandseriesRSIList == null) accbrandseriesRSIList = new List<RSOrderItem__c>();
                accbrandseriesRSIList.add(oldrsi);
                delaccbrandseriesDTStocks.put(key, accbrandseriesRSIList);
            }
        }
    }

    public static void create(boolean isInsert, boolean isUpdate, boolean isDelete){
        if(acc_pro_ymkeys.size() <= 0 && acc_brand_series_ymkeys.size() <= 0) return;

        initExistRecords();
        if(acc_brand_series_ymkeys.size() > 0) getSOAvgsOf12Month();

        currentDTStocks = new Map<String, DTStock__c>();
        integrateCurrentDTStocks();
        system.debug(currentDTStocks);
        upsert currentDTStocks.values();
        //Database.upsert(currentDTStocks.values(), false);
    }

    public static void initExistRecords(){
        ExistProducts = new Map<String, Product__c>();
        ExistAccounts = new Map<String, Account>();
        ExistStocks = new Map<String, Stock__c>();
        ExistDTStocks = new Map<String, DTStock__c>();
        ExistBrandStocks = new Map<String, List<Stock__c>>();

        List<String> proCodes = new List<String>();
        List<String> accCodes = new List<String>();
        List<String> accBrands = new List<String>();
        List<String> accSeries = new List<String>();
        List<String> codeAndym;
        List<String> accAndpro;
        List<String> years = new List<String>();
        List<String> months = new List<String>();
        for(String key : acc_pro_ymkeys){
            //store_prokey = AccountCode__c+'%%'+ProductCode__c+'&&'+year()+'&&'+month();
            codeAndym = key.split('&&');
            if(codeAndym.size() < 3) continue;
            accAndpro = codeAndym[0].split('%%');
            accCodes.add(accAndpro[0]);
            proCodes.add(accAndpro[1]);
            years.add(codeAndym[1]);
            months.add(codeAndym[2]);
        }
        for(String key : acc_brand_series_ymkeys){
            //store_prokey = AccountCode__c+'%%'+ProductCode__c+'&&'+year()+'&&'+month();
            codeAndym = key.split('&&');
            if(codeAndym.size() < 3) continue;
            accAndpro = codeAndym[0].split('%%');
            accCodes.add(accAndpro[0]);
            accBrands.add(accAndpro[1]);
            accSeries.add(accAndpro[2]);
            years.add(codeAndym[1]);
            months.add(codeAndym[2]);
        }

        List<Account> accs = new List<Account>();
        accs = [SELECT Id, AccountCode__c
                FROM Account
                WHERE AccountCode__c in :accCodes
               ];

        for(Account acc : accs){
            ExistAccounts.put(acc.AccountCode__c, acc);
        }

        List<Product__c> pros = new List<Product__c>();
        pros = [SELECT Id, Name, Brand__c, Series__c
                FROM Product__c
                WHERE Name in :proCodes
                OR(Brand__c in :accBrands
                   AND Series__c in :accSeries)
               ];

        for(Product__c pro : pros){
            ExistProducts.put(pro.Name, pro);
        }

        String tempkey;
        List<Stock__c> stocks = new List<Stock__c>();
        stocks = [SELECT Id, Account__c, Account__r.AccountCode__c,
                  Product__c, Product__r.Name, Product__r.Brand__c, Product__r.Series__c,
                  Quantity__c, Amount__c, Year__c, Month__c
                  FROM Stock__c
                  WHERE Account__r.AccountCode__c in :accCodes
                  AND(Product__r.Name in :proCodes
                      OR(Product__r.Brand__c in :accBrands
                         AND Product__r.Series__c in :accSeries))
                  AND Year__c in :years
                  AND Month__c in :months
                 ];

        List<Stock__c> tempStocks;
        for(Stock__c stock : stocks){
            tempkey = stock.Account__r.AccountCode__c +'%%'+stock.Product__r.Name+'&&'+stock.Year__c+'&&'+stock.Month__c;
            ExistStocks.put(tempkey, stock);

            tempkey = stock.Account__r.AccountCode__c +'%%'+stock.Product__r.Brand__c+'%%'+ stock.Product__r.Series__c+'&&'+stock.Year__c+'&&'+stock.Month__c;
            tempStocks = ExistBrandStocks.get(tempkey);
            if(tempStocks==null) tempStocks = new List<Stock__c>();
            tempStocks.add(stock);
            ExistBrandStocks.put(tempkey, tempStocks);
        }

        List<DTStock__c> dtstockList = new List<DTStock__c>();
        dtstockList = [SELECT Id, Account__c, Account__r.AccountCode__c, Stock__c,
                       Product__c, Product__r.Name, Quantity__c, Amount__c, Price__c,
                       Year__c, Month__c, ProductBrand__c, ProductSeries__c
                       FROM DTStock__c
                       WHERE Account__r.AccountCode__c in :accCodes
                       AND(Product__r.Name in :proCodes
                           OR(ProductBrand__c in :accBrands
                              AND ProductSeries__c in :accSeries))
                       AND Year__c in :years
                       AND Month__c in :months
                      ];

        for(DTStock__c dtstock : dtstockList){
            if(dtstock.Product__c != null){
                tempkey = dtstock.Account__r.AccountCode__c +'%%'+dtstock.Product__r.Name+'&&'+dtstock.Year__c+'&&'+dtstock.Month__c;
                ExistDTStocks.put(tempkey, dtstock);
            }

            if(String.isNotEmpty(dtstock.ProductBrand__c) && String.isNotEmpty(dtstock.ProductSeries__c)){
                tempkey = dtstock.Account__r.AccountCode__c +'%%'+dtstock.ProductBrand__c+'%%'+ dtstock.ProductSeries__c+'&&'+dtstock.Year__c+'&&'+dtstock.Month__c;
                ExistDTStocks.put(tempkey, dtstock);
            }
        }
    }

    public static void getSOAvgsOf12Month(){
        SOAvgsOf12Month = new Map<String, Decimal>();

        List<String> accCodes = new List<String>();
        List<String> accBrands = new List<String>();
        List<String> accSeries = new List<String>();
        String acc;
        String brand;
        String series;
        List<String> codeAndym;
        List<String> accAndpro;
        List<String> years = new List<String>();
        List<String> months = new List<String>();
        Integer minY = 10000;
        Integer minM = 13;

        for(String key : acc_brand_series_ymkeys){
            //store_prokey = AccountCode__c+'%%'+ProductCode__c+'&&'+year()+'&&'+month();
            codeAndym = key.split('&&');
            if(codeAndym.size() < 3) continue;
            accAndpro = codeAndym[0].split('%%');
            accCodes.add(accAndpro[0]);
            accBrands.add(accAndpro[1]);
            accSeries.add(accAndpro[2]);
            years.add(codeAndym[1]);
            months.add(codeAndym[2]);

            if(minY>Integer.valueOf(codeAndym[1])) minY = Integer.valueOf(codeAndym[1]);
            if(minM>Integer.valueOf(codeAndym[2])) minM = Integer.valueOf(codeAndym[2]);
        }

        Date d = date.newInstance(minY-1, minM, 1);
        List<RSOrderItem__c> rsoitems = new List<RSOrderItem__c>();
        rsoitems = [SELECT Id, SellAmount__c, AccountCode__c, Active__c, Date__c,
                    isTest__c, ProductBrand__c, ProductSeries__c
                    FROM RSOrderItem__c
                    WHERE isTest__c = false
                    AND Active__c = true
                    AND AccountCode__c in :accCodes
                    AND ProductBrand__c in :accBrands
                    AND ProductSeries__c in :accSeries
                    AND Date__c > :d
                   ];

        Map<String, Decimal> SOs = new Map<String, Decimal>();
        Map<String, Set<String>> SOMonths = new Map<String, Set<String>>();
        Set<String> tempMonth;
        Decimal tempSo;
        for(String key : acc_brand_series_ymkeys){
            codeAndym = key.split('&&');
            accAndpro = codeAndym[0].split('%%');
            acc = accAndpro[0];
            brand = accAndpro[1];
            series = accAndpro[2];
            d = date.newInstance(Integer.valueOf(codeAndym[1])-1, Integer.valueOf(codeAndym[2]), 1);
            for(RSOrderItem__c item : rsoitems){
                if(acc.equals(item.AccountCode__c) && brand.equals(item.ProductBrand__c) && series.equals(item.ProductSeries__c) && d<=item.Date__c){
                    tempMonth = SOMonths.get(key);
                    if(tempMonth == null) tempMonth = new Set<String>();
                    tempMonth.add(codeAndym[1] +'&&'+codeAndym[2]);
                    SOMonths.put(key, tempMonth);

                    tempSo = SOs.get(key);
                    if(tempSo==null) tempSo = 0;
                    tempSo += item.SellAmount__c;
                    SOs.put(key, tempSo);
                }
            }
        }

        Decimal temphistorySO;
        Integer count;
        Set<String> temphistorySOM;
        Decimal turnover;
        for(String som : SOMonths.keySet()){
            temphistorySOM = SOMonths.get(som);
            if(temphistorySOM!=null) count = temphistorySOM.size();
            temphistorySO = SOs.get(som);
            if(temphistorySO==null) temphistorySO = 0;
            if(count==0) count = 1;

            turnover = temphistorySO.divide(count, 2, System.RoundingMode.HALF_UP);
            SOAvgsOf12Month.put(som, turnover);
        }
    }

    public static void integrateCurrentDTStocks(){
        List<String> codeAndym;
        List<String> accAndpro;
        String accCode;
        String proCode;
        String brand;
        String series;
        String year;
        String month;
        Set<String> all_keys = new Set<String>();
        Account tempAcc;
        Product__c tempPro;
        boolean isBySKU;

        all_keys.addAll(acc_pro_ymkeys);
        all_keys.addAll(acc_brand_series_ymkeys);
        for(String key : all_keys){
            codeAndym = key.split('&&');
            if(codeAndym.size() < 3) continue;
            accAndpro = codeAndym[0].split('%%');
            accCode = accAndpro[0];
            if(accAndpro.size()<3){
                proCode = accAndpro[1];
                brand = '';
                series = '';
                isBySKU = true;
            }else{
                proCode = '';
                brand = accAndpro[1];
                series = accAndpro[2];
                isBySKU = false;
            }
            year = codeAndym[1];
            month = codeAndym[2];

            boolean isExist = true;
            Map<String, boolean> isExistAndNotZeroMap;
            DTStock__c dtstock = ExistDTStocks.get(key);
            if(dtstock==null){isExist=false;dtstock=currentDTStocks.get(key);}
            if(dtstock==null) dtstock = new DTStock__c();

            tempAcc = ExistAccounts.get(accCode);
            if(tempAcc!=null)dtstock.Account__c = tempAcc.Id;
            tempPro = ExistProducts.get(proCode);
            if(tempPro!=null)dtstock.Product__c = tempPro.Id;
            dtstock.ProductBrand__c = brand;
            dtstock.ProductSeries__c = series;
            if(ExistStocks.get(key)!=null)
                dtstock.Stock__c = ExistStocks.get(key).Amount__c;
            if(ExistBrandStocks.get(key) != null){
                dtstock.Stock__c = 0;
                for(Stock__c st : ExistBrandStocks.get(key)){
                    dtstock.Stock__c += st.Amount__c;
                }
            }
            dtstock.Year__c = year;
            dtstock.Month__c = month;

            if(isBySKU){
                isExistAndNotZeroMap = new Map<String, boolean>();
                verifyDTStock(dtstock, isExist, isExistAndNotZeroMap);
                Map<String, Decimal> currVals = new Map<String, Decimal>();
                currVals = getCurrentVal(accproDTStocks.get(key), delaccproDTStocks.get(key), isExistAndNotZeroMap);
                dtstock.Amount__c += currVals.get('Amount');
                Decimal Quantity = currVals.get('Quantity');
                dtstock.Quantity__c += Quantity;
                Quantity = dtstock.Quantity__c;
                if(Quantity==0) Quantity = 1;
                dtstock.Price__c = dtstock.Amount__c.divide(Quantity, 2, System.RoundingMode.HALF_UP);
                dtstock.Type__c = 'Stock by SKU';
            }else{
                dtstock.Type__c = 'Stock MOH by Series';
                dtstock.SOAvgsOf12Month__c = SOAvgsOf12Month.get(key);
            }

            currentDTStocks.put(key, dtstock);
        }
    }

    public static Map<String, Decimal> getCurrentVal(List<RSOrderItem__c> newrsis, List<RSOrderItem__c> oldrsis, Map<String, boolean> isExistAndNotZeroMap){
        Map<String, Decimal> val = new Map<String, Decimal>();
        val.put('Quantity', 0);
        val.put('Amount', 0);

        if(newrsis == null && oldrsis == null) return val;
        if(newrsis == null) newrsis = new List<RSOrderItem__c>();
        if(oldrsis == null) oldrsis = new List<RSOrderItem__c>();
        if(newrsis.size()==0 && oldrsis.size() == 0) return val;

        Decimal Quantity = 0;
        Decimal Amount = 0;
        for(RSOrderItem__c rsi : newrsis){
            Quantity += rsi.Quantity__c;
            Amount += rsi.SellAmount__c;
        }

        for(RSOrderItem__c rsi : oldrsis){
            if(isExistAndNotZeroMap.get('Quantity')) Quantity -= rsi.Quantity__c;
            if(isExistAndNotZeroMap.get('Amount')) Amount -= rsi.SellAmount__c;
        }

        val.put('Quantity', Quantity);
        val.put('Amount', Amount);
        return val;
    }

    public static void verifyDTStock(DTStock__c dtstock, boolean isExist,  Map<String, boolean> isExistAndNotZeroMap){
        if(dtstock.Quantity__c==null) dtstock.Quantity__c = 0;
        if(dtstock.Amount__c==null) dtstock.Amount__c = 0;

        isExistAndNotZeroMap.put('Quantity', isExist && dtstock.Quantity__c != 0);
        isExistAndNotZeroMap.put('Amount', isExist && dtstock.Amount__c != 0);
    }
}
