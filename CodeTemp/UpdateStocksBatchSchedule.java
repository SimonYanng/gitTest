global class UpdateStocksBatchSchedule implements Database.batchable<Map<String, AggregateResult>>, Schedulable {
    private Date currDate;
    private DateTime currTime;
    private String currYear;
    private String currMonth;
    private Date endDay;
    private Date startDay;
    private String lastYear;
    private String lastMonth;

    global void execute(SchedulableContext SC) {
        //init(null, null, null, null, null);
        initDate(null);
        Database.executebatch(this);
    }

    global UpdateStocksBatchSchedule(Date d){
        initDate(d);
    }

    global Iterable<Map<String, AggregateResult>> start(Database.BatchableContext BC){
        return new HandleDataIterable(currDate);
    }

    global void execute(Database.BatchableContext BC, List<Map<String, AggregateResult>> dataScope){

        List<String> tempList;
        Set<String> currAccIds = new Set<String>();
        Set<String> currProIds = new Set<String>();
        for(Map<String, AggregateResult> data : dataScope){
            for(String key : data.keySet()){
                tempList = key.split('&&');
                currAccIds.add(tempList[0]);
                currProIds.add(tempList[1]);
            }
        }

        Map<String, Stock__c> thisMonthStocks = new Map<String, Stock__c>();
        thisMonthStocks = getThisMonthStock(currAccIds, currProIds);
        Map<String, DTStock__c> thisMonthSKUDTStocks = new Map<String, DTStock__c>();
        thisMonthSKUDTStocks = getThisMonthSKUDTStock(currAccIds, currProIds);
        Map<String, DTStock__c> thisMonthSeriesDTStocks = new Map<String, DTStock__c>();
        thisMonthSeriesDTStocks = getThisMonthSeriesDTStock(currAccIds);
        Map<String, DTStock__c> thisMonthAccDTStocks = new Map<String, DTStock__c>();
        thisMonthAccDTStocks = getThisMonthAccDTStock(currAccIds);

        Map<String, Stock__c> currStocks = new Map<String, Stock__c>();
        Stock__c currStock = new Stock__c();
        Map<String, DTStock__c> currDTStocks = new Map<String, DTStock__c>();
        DTStock__c currDTStock = new DTStock__c();
        String currKey;
        Decimal Quantity;
        Decimal Amount;
        Integer abs;
        for(Map<String, AggregateResult> data : dataScope){
            for(String key : data.keySet()){
                tempList = key.split('&&');

                currKey = tempList[0] +'&&'+ tempList[1];
                currStock = currStocks.get(currKey);
                if(currStock==null) currStock = thisMonthStocks.get(currKey);
                if(currStock==null) currStock = new Stock__c();
                if(currStock.LastModifiedDate!=null && currStock.LastModifiedDate<currTime){
                    currStock.Quantity__c = 0;
                    currStock.Amount__c = 0;
                }
                currDTStock = currDTStocks.get(currKey);
                if(currDTStock==null) currDTStock = thisMonthSKUDTStocks.get(currKey);
                if(currDTStock!=null && currDTStock.LastModifiedDate!=null && currDTStock.LastModifiedDate<currTime){
                    currDTStock.Quantity__c = 0;
                    currDTStock.Stock__c = 0;
                }

                currStock.Account__c = tempList[0];
                currStock.Product__c = tempList[1];
                currStock.Year__c = currYear;
                currStock.Month__c = currMonth;
                if(currStock.Quantity__c==null) currStock.Quantity__c = 0;
                if(currStock.Amount__c==null) currStock.Amount__c = 0;

                Quantity = (Decimal)data.get(key).get('Quantity');
                Amount = (Decimal)data.get(key).get('Amount');

                if('LastStock'.equals(tempList[2]) || 'LastSellIn'.equals(tempList[2])) abs=1;
                if('HistorySO'.equals(tempList[2]) || 'RSOrderItem'.equals(tempList[2])) abs=-1;
                currStock.Quantity__c += abs*Quantity;
                currStock.Amount__c += abs*Amount;
                currStocks.put(currKey, currStock);

                if(currDTStock!=null){
                    if(currDTStock.Stock__c==null) currDTStock.Stock__c = 0;
                    if(currDTStock.Quantity__c==null) currDTStock.Quantity__c = 0;
                    currDTStock.Quantity__c += abs*Quantity;
                    currDTStock.Stock__c += abs*Amount;
                    currDTStocks.put(currKey, currDTStock);
                }
            }
        }

        upsert currStocks.values();
        upsert currDTStocks.values();
    }

    global void finish(Database.BatchableContext BC){

    }

    global void initDate(Date d){
        currDate = d;
        if(currDate==null) currDate = date.today();
        currTime = datetime.now();
        currYear = String.valueOf(currDate.year());
        currMonth = String.valueOf(currDate.month());
        endDay = currDate.toStartOfMonth()-1;
        startDay = endDay.toStartOfMonth();
        lastYear = String.valueOf(startDay.year());
        lastMonth = String.valueOf(startDay.month());
    }

    global Map<String, Stock__c> getThisMonthStock(Set<String> accIds, Set<String> proIds){
        List<Stock__c> stocks = new List<Stock__c>();
        stocks=[SELECT Id, Account__c, Product__c, Quantity__c, Amount__c, LastModifiedDate
                FROM Stock__c
                WHERE Year__c = :currYear
                AND Month__c = :currMonth
                AND Account__c in :accIds
                AND Product__c in :proIds
               ];

        Map<String, Stock__c> result = new Map<String, Stock__c>();
        for(Stock__c st : stocks){
            result.put(st.Account__c+'&&'+st.Product__c, st);
        }
        return result;
    }

    global Map<String, DTStock__c> getThisMonthSKUDTStock(Set<String> accIds, Set<String> proIds){
        List<DTStock__c> dtstocks = new List<DTStock__c>();
        dtstocks=[SELECT Id, Account__c, Product__c, ProductBrand__c, ProductSeries__c, Quantity__c, Stock__c, Type__c, LastModifiedDate
                FROM DTStock__c
                WHERE Year__c = :currYear
                AND Month__c = :currMonth
                AND Account__c in :accIds
                AND Product__c in :proIds
                AND Type__c = 'Stock by SKU'
               ];

        Map<String, DTStock__c> result = new Map<String, DTStock__c>();
        for(DTStock__c dtst : dtstocks){
            result.put(dtst.Account__c+'&&'+dtst.Product__c, dtst);
        }
        return result;
    }

    global Map<String, DTStock__c> getThisMonthSeriesDTStock(Set<String> accIds){
        List<DTStock__c> dtstocks = new List<DTStock__c>();
        dtstocks=[SELECT Id, Account__c, ProductBrand__c, ProductSeries__c, Quantity__c, Stock__c, Type__c, LastModifiedDate
                FROM DTStock__c
                WHERE Year__c = :currYear
                AND Month__c = :currMonth
                AND Account__c in :accIds
                AND Type__c = 'Stock MOH by Series'
               ];

        Map<String, DTStock__c> result = new Map<String, DTStock__c>();
        for(DTStock__c dtst : dtstocks){
            result.put(dtst.Account__c+'&&'+dtst.ProductBrand__c+'&&'+dtst.ProductSeries__c, dtst);
        }
        return result;
    }

    global Map<String, DTStock__c> getThisMonthAccDTStock(Set<String> accIds){
        List<DTStock__c> dtstocks = new List<DTStock__c>();
        dtstocks=[SELECT Id, Account__c, Product__c, Quantity__c, Stock__c, Type__c, LastModifiedDate
                FROM DTStock__c
                WHERE Year__c = :currYear
                AND Month__c = :currMonth
                AND Account__c in :accIds
                AND Type__c = 'Stock MOH'
               ];

        Map<String, DTStock__c> result = new Map<String, DTStock__c>();
        for(DTStock__c dtst : dtstocks){
            result.put(dtst.Account__c, dtst);
        }
        return result;
    }

    global class HandleDataIterable implements Iterable<Map<String, AggregateResult>> {
        public Date d;
        global HandleDataIterable(Date d){
            this.d = d;
        }
        global Iterator<Map<String, AggregateResult>> Iterator(){
            return new HandleDataIterator(d);
        }
    }

    global class HandleDataIterator implements Iterator<Map<String, AggregateResult>>{
        private Date currDate{get;set;}
        private String currYear{get;set;}
        private String currMonth{get;set;}
        private Date endDay{get;set;}
        private Date startDay{get;set;}
        private String lastYear{get;set;}
        private String lastMonth{get;set;}

        List<Map<String, AggregateResult>> datas{get; set;}
        Integer index {get; set;}

        public HandleDataIterator(Date d){

            currDate = d;
            if(currDate==null) currDate = date.today();
            currYear = String.valueOf(currDate.year());
            currMonth = String.valueOf(currDate.month());
            endDay = currDate.toStartOfMonth()-1;
            startDay = endDay.toStartOfMonth();
            lastYear = String.valueOf(startDay.year());
            lastMonth = String.valueOf(startDay.month());

            datas = initHandleData();
            index = 0;
        }

        global boolean hasNext(){
            return datas != null && !datas.isEmpty() && index < datas.size();
        }
        global Map<String, AggregateResult> next(){
            return datas[index++];
        }

        public List<Map<String, AggregateResult>> initHandleData(){

            List<Map<String, AggregateResult>> results = new List<Map<String, AggregateResult>>();

            for(AggregateResult ar : initLastStocks())
                results.Add(new Map<String, AggregateResult>{ar.get('accId') + '&&' + ar.get('proId') +'&&LastStock' => ar});

            for(AggregateResult ar : initSellIns())
                results.Add(new Map<String, AggregateResult>{ar.get('accId') + '&&' + ar.get('proId') +'&&LastSellIn' => ar});

            //for(AggregateResult ar : initRSOrderItems())
            //  results.Add(new Map<String, AggregateResult>{ar.get('accId') + '&&' + ar.get('proId') +'&&RSOrderItem' => ar});

            for(AggregateResult ar : initHistorySOs())
                results.Add(new Map<String, AggregateResult>{ar.get('accId') + '&&' + ar.get('proId') +'&&HistorySO' => ar});

            return results;
        }

        public AggregateResult[] initLastStocks(){
            return [SELECT Account__c accId, Product__c proId, SUM(AdjustedQuantity__c) Quantity, SUM(Amount__c) Amount
                    FROM Stock__c
                    WHERE Year__c = :lastYear
                    AND Month__c = :lastMonth
                    //AND(Account__c in :curraccIds
                    //  AND Product__c in :currproIds)
                    GROUP BY Account__c, Product__c
                   ];

        }

        public AggregateResult[] initRSOrderItems(){
            return [SELECT RSOrder__r.RetailStore__r.Account__c accId, Product__c proId,
                    SUM(Quantity__c) Quantity, SUM(SellAmount__c) Amount
                    FROM RSOrderItem__c
                    WHERE Active__c = true
                    AND Date__c >= :startDay
                    AND Date__c <= :endDay
                    AND isTest__c = false
                    //AND(RSOrder__r.RetailStore__r.Account__c in :curraccIds
                    //  AND Product__c in :currproIds)
                    GROUP BY RSOrder__r.RetailStore__r.Account__c, Product__c
                   ];
        }

        public AggregateResult[] initSellIns(){
            return [SELECT Account__c accId, Product__c proId, SUM(Quantity__c) Quantity,
                    SUM(Amount__c) Amount
                    FROM SellIn__c
                    WHERE isTest__c = false
                    AND Year__c = :lastYear
                    AND Month__c = :lastMonth
                    //AND Account__c = '001N000000RJmvoIAD'
                    //AND(Account__c in :curraccIds
                    //  AND Product__c in :currproIds)
                    GROUP BY Account__c, Product__c
                   ];
        }

        public AggregateResult[] initHistorySOs(){
            return [SELECT RetailStore__r.Account__c accId, Product__c proId, SUM(Quantity__c) Quantity, SUM(Amount__c) Amount
                    FROM HistorySO__c
                    WHERE Year__c = :lastYear
                    AND Month__c = :lastMonth
                    //AND(RetailStore__r.Account__c in :curraccIds
                    //  AND Product__c in :currproIds)
                    GROUP BY RetailStore__r.Account__c, Product__c
                   ];
        }
    }

}
