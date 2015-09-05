global class UpdateStocksBatchSchedule implements Database.Batchable<Stock__c>, Schedulable {
    private Date currDate;
    private String currYear;
    private String currMonth;
    private Date endDay;
    private Date startDay;
    private String lastYear;
    private String lastMonth;
    private Set<String> doneaccIds;
    private Set<String> doneproIds;
    private Set<String> curraccIds;
    private Set<String> currproIds;
    private Integer currsize;
    private Integer batchSize;
    private Integer times;

    global void execute(SchedulableContext SC) {
        init(null, null, null, null, null);
        Database.executebatch(this);
    }

    global UpdateStocksBatchSchedule(Date d, Set<String> oldaccIds, Set<String> oldproIds, Integer batchSize, Integer count){
        init(d, oldaccIds, oldproIds, batchSize, count);
    }

    global List<Stock__c> start(Database.BatchableContext BC){
        integrateCurrentStocks();
        return new List<Stock__c>();
    }

    global void execute(Database.BatchableContext BC, List<Stock__c> scope) {

    }

    global void finish(Database.BatchableContext BC){
        doneaccIds.addAll(curraccIds);
        doneproIds.addAll(currproIds);
        times++;

        //system.debug('doneaccIds:'+doneaccIds);
        //system.debug('doneproIds:'+doneproIds);

        if(currsize < batchSize) system.debug('Done!');
        else if(times<100)Database.executebatch(new UpdateStocksBatchSchedule(currDate, doneaccIds, doneproIds, batchSize, times));
    }

    public void init(Date d, Set<String> oldaccIds, Set<String> oldproIds, Integer bsize, Integer count){
        currDate = d;
        if(currDate==null) currDate = date.today();
        currYear = String.valueOf(currDate.year());
        currMonth = String.valueOf(currDate.month());
        endDay = currDate.toStartOfMonth()-1;
        startDay = endDay.toStartOfMonth();
        lastYear = String.valueOf(startDay.year());
        lastMonth = String.valueOf(startDay.month());

        batchSize = bsize;
        if(batchSize==null) batchSize = 2000;

        times = count;
        if(times==null) times = 0;

        doneaccIds = new Set<String>();
        doneproIds = new Set<String>();
        if(oldaccIds!=null) doneaccIds.addAll(oldaccIds);
        if(oldproIds!=null) doneproIds.addAll(oldproIds);
        curraccIds = new Set<String>();
        currproIds = new Set<String>();

        system.debug('>>>oldaccIds<<<'+oldaccIds);
        system.debug('>>>oldproIds<<<'+oldproIds);

        currsize = 0;
        initKeySet();
    }

    public void initKeySet(){

        AggregateResult[] groupedResults;
        Integer size = batchSize;
        if(size>0){
            groupedResults = [SELECT Account__c accId, Product__c proId
                              FROM Stock__c
                              WHERE Year__c = :lastYear
                              AND Month__c = :lastMonth
                              //AND Account__c = '001N000000RJmvoIAD'
                              AND(Account__c not in :doneaccIds
                                  OR Product__c not in :doneproIds)
                              GROUP BY Account__c, Product__c
                              LIMIT :size
                             ];
            for (AggregateResult ar : groupedResults)  {
                curraccIds.add((String)ar.get('accId'));
                currproIds.add((String)ar.get('proId'));
            }
            currsize += groupedResults.size();
            size -= currsize;
        }

        /*
        if(size>0 && currsize < batchSize){
            groupedResults = [SELECT Account__c accId, Product__c proId
                              FROM SellIn__c
                              WHERE Year__c = :lastYear
                              AND Month__c = :lastMonth
                              AND(Account__c not in :doneaccIds
                                  OR Product__c not in :doneproIds)
                              GROUP BY Account__c, Product__c
                              LIMIT :size
                             ];

             for (AggregateResult ar : groupedResults)  {
                 curraccIds.add((String)ar.get('accId'));
                 currproIds.add((String)ar.get('proId'));
             }
             currsize += groupedResults.size();
             size -= currsize;
        }


        if(size>0 && currsize < batchSize){
            groupedResults = [SELECT RSOrder__r.RetailStore__r.Account__c accId, Product__c proId
                              FROM RSOrderItem__c
                              WHERE Date__c >= :startDay
                              AND Date__c <= :endDay
                              AND(RSOrder__r.RetailStore__r.Account__c not in :doneaccIds
                                  OR Product__c not in :doneproIds)
                              GROUP BY RSOrder__r.RetailStore__r.Account__c, Product__c
                              LIMIT :size
                             ];
                             for (AggregateResult ar : groupedResults)  {
                                 curraccIds.add((String)ar.get('accId'));
                                 currproIds.add((String)ar.get('proId'));
                             }


                             currsize += groupedResults.size();
                             size -= currsize;
        }

        if(size>0 && currsize < batchSize){
            groupedResults = [SELECT RetailStore__r.Account__c accId, Product__c proId
                              FROM HistorySO__c
                              WHERE Year__c = :lastYear
                              AND Month__c = :lastMonth
                              AND(RetailStore__r.Account__c not in :doneaccIds
                                  OR Product__c not in :doneproIds)
                              GROUP BY RetailStore__r.Account__c, Product__c
                              LIMIT :size
                             ];
                             for (AggregateResult ar : groupedResults)  {
                                 curraccIds.add((String)ar.get('accId'));
                                 currproIds.add((String)ar.get('proId'));
                             }
                             currsize += groupedResults.size();
        }
        */
    }

    public void integrateCurrentStocks(){
        Map<String, Stock__c> currStocks = new Map<String, Stock__c>();
        Map<String, DTStock__c> currDTStocks = new Map<String, DTStock__c>();

        Map<String, AggregateResult> lastMonthStocks = new Map<String, AggregateResult>();
        initLastStocks(lastMonthStocks);

        Map<String, AggregateResult> lastMonthOrderItems = new Map<String, AggregateResult>();
        initRSOrderItems(lastMonthOrderItems);

        Map<String, AggregateResult> lastMonthSellIns = new Map<String, AggregateResult>();
        initSellIns(lastMonthSellIns);

        Map<String, Stock__c> thisMonthStocks = new Map<String, Stock__c>();
        initStocks(thisMonthStocks);

        Map<String, DTStock__c> thisMonthDTStocks = new Map<String, DTStock__c>();
        initDTStocks(thisMonthDTStocks);


        Date d = date.newInstance(2015, 8, 1);
        Map<String, AggregateResult> historySOs = new Map<String, AggregateResult>();
        if(currDate <= d){
            initHistorySOs(historySOs);
        }

        //system.debug('lastMonthStocks:'+lastMonthStocks);
        //system.debug('lastMonthOrderItems:'+lastMonthOrderItems);
        //system.debug('lastMonthSellIns:'+lastMonthSellIns);
        //system.debug('thisMonthStocks:'+thisMonthStocks);
        //system.debug('thisMonthDTStocks:'+thisMonthDTStocks);
        //system.debug('historySOs:'+historySOs);

        String currAccountId;
        String currProductId;
        String currkey;
        Stock__c currStock;
        Set<Id> accIds = new Set<Id>();
        Set<Id> proIds = new Set<Id>();
        List<String> codeAndDate;
        Set<String> years = new Set<String>();
        Set<String> months = new Set<String>();

        for(String oldkey : lastMonthStocks.keySet()){
            codeAndDate = oldkey.split('&&');
            if(codeAndDate.size()<2) continue;
            currAccountId = codeAndDate[0];
            currProductId = codeAndDate[1];
            currkey = currAccountId + '&&' + currProductId;

            AggregateResult lastStock = lastMonthStocks.get(oldkey);
            //currStock = thisMonthStocks.get(currkey);
            //if(currStock == null) currStock = currStocks.get(currkey);
            //if(currStock == null) currStock = new Stock__c();
            currStock = new Stock__c();
            //currStock.Id = null;
            currStock.Account__c = currAccountId;
            currStock.Product__c = currProductId;
            currStock.Year__c = currYear;
            currStock.Month__c = currMonth;
            currStock.Amount__c = (Decimal)lastStock.get('Amount');
            currStock.Quantity__c = (Decimal)lastStock.get('Quantity');
            if(currStock.Amount__c == null) currStock.Amount__c = 0;
            if(currStock.Quantity__c == null) currStock.Quantity__c = 0;
            //currStock.Amount__c += (Decimal)lastStock.get('Amount');
            //currStock.Quantity__c += (Decimal)lastStock.get('Quantity');

            currStocks.put(currkey, currStock);
        }

        /*
        system.debug('lastMonthSellIns:'+lastMonthSellIns);
        Decimal val1 = 0;Decimal val2 = 0;
        for(String sellinkey : lastMonthSellIns.keySet()){
            val1 += (Decimal)lastMonthSellIns.get(sellinkey).get('Amount');
            val2 += (Decimal)lastMonthSellIns.get(sellinkey).get('Quantity');
        }
        system.debug('lastMonthSellIns.keySet().szie:'+lastMonthSellIns.keySet().size());
        system.debug('Amount:'+val1);
        system.debug('Quantity:'+val2);

        for(String sellinkey : lastMonthSellIns.keySet()){
            codeAndDate = sellinkey.split('&&');
            if(codeAndDate.size()<2) continue;
            currAccountId = codeAndDate[0];
            currProductId = codeAndDate[1];

            currkey = currAccountId + '&&' + currProductId ;
            currStock = currStocks.get(currkey);
            if(currStock == null) currStock = thisMonthStocks.get(currkey);
            if(currStock == null) currStock = new Stock__c();
            //currStock.Id = null;
            currStock.Account__c = currAccountId;
            currStock.Product__c = currProductId;
            currStock.Year__c = currYear;
            currStock.Month__c = currMonth;
            if(currStock.Amount__c == null) currStock.Amount__c = 0;
            if(currStock.Quantity__c == null) currStock.Quantity__c = 0;

            AggregateResult sellin = lastMonthSellIns.get(sellinkey);
            currStock.Amount__c += (Decimal)sellin.get('Amount');
            currStock.Quantity__c += (Decimal)sellin.get('Quantity');

            currStocks.put(currkey, currStock);
        }



        if(currDate > d){
            for(String oldkey : lastMonthOrderItems.keySet()){
                codeAndDate = oldkey.split('&&');
                if(codeAndDate.size()<2) continue;
                currAccountId = codeAndDate[0];
                currProductId = codeAndDate[1];

                currkey = currAccountId + '&&' + currProductId;
                currStock = currStocks.get(currkey);
                if(currStock == null) currStock = thisMonthStocks.get(currkey);
                if(currStock == null) currStock = new Stock__c();
                //currStock.Id = null;
                currStock.Account__c = currAccountId;
                currStock.Product__c = currProductId;
                currStock.Year__c = currYear;
                currStock.Month__c = currMonth;
                if(currStock.Amount__c == null) currStock.Amount__c = 0;
                if(currStock.Quantity__c == null) currStock.Quantity__c = 0;

                AggregateResult orderItem = lastMonthOrderItems.get(oldkey);
                currStock.Amount__c -= (Decimal)orderItem.get('Amount');
                currStock.Quantity__c -= (Decimal)orderItem.get('Quantity');

                currStocks.put(currkey, currStock);
            }
        }else{
            for(String oldkey : historySOs.keySet()){
                codeAndDate = oldkey.split('&&');
                if(codeAndDate.size()<2) continue;
                currAccountId = codeAndDate[0];
                currProductId = codeAndDate[1];

                currkey = currAccountId + '&&' + currProductId;
                currStock = currStocks.get(currkey);
                if(currStock == null) currStock = thisMonthStocks.get(currkey);
                if(currStock == null) currStock = new Stock__c();
                //currStock.Id = null;
                currStock.Account__c = currAccountId;
                currStock.Product__c = currProductId;
                currStock.Year__c = currYear;
                currStock.Month__c = currMonth;
                if(currStock.Amount__c == null) currStock.Amount__c = 0;
                if(currStock.Quantity__c == null) currStock.Quantity__c = 0;

                AggregateResult historySO = historySOs.get(oldkey);
                currStock.Amount__c -= (Decimal)historySO.get('Amount');
                currStock.Quantity__c -= (Decimal)historySO.get('Quantity');

                currStocks.put(currkey, currStock);
            }
        }


        Stock__c st;
        DTStock__c dt = new DTStock__c();
        for(String dtKey : thisMonthDTStocks.keySet()){
            st = currStocks.get(dtKey);
            if(st==null) st = new Stock__c();
            dt.Stock__c = st.Amount__c;

            currDTStocks.put(dtKey, dt);
        }
        */

        upsert currStocks.values();
        upsert currDTStocks.values();
    }

    public void initStocks(Map<String, Stock__c> stocks){
        List<Stock__c> stockList = new List<Stock__c>();
        stockList = [SELECT Id, Account__c, Product__c, Quantity__c, Amount__c
                     FROM Stock__c
                     WHERE Year__c = :currYear
                     AND Month__c = :currMonth
                     AND(Account__c  in :curraccIds
                         AND Product__c in :currproIds)
                    ];

        String key;
        for(Stock__c stock : stockList){
            key = stock.Account__c + '&&' + stock.Product__c;
            stocks.put(key, stock);
        }
    }

    public void initLastStocks(Map<String, AggregateResult> stocksAR){
        AggregateResult[] groupedResults;
        groupedResults = [SELECT Account__c accId, Product__c proId, SUM(AdjustedQuantity__c) Quantity, SUM(Amount__c) Amount
                          FROM Stock__c
                          WHERE Year__c = :lastYear
                          AND Month__c = :lastMonth
                          AND(Account__c in :curraccIds
                              AND Product__c in :currproIds)
                          GROUP BY Account__c, Product__c
                         ];

        for(AggregateResult ar : groupedResults)
            stocksAR.put(ar.get('accId') + '&&' + ar.get('proId'), ar);
    }

    public void initRSOrderItems(Map<String, AggregateResult> orderItemsAR){
        AggregateResult[] groupedResults;
        groupedResults = [SELECT RSOrder__r.RetailStore__r.Account__c accId, Product__c proId,
                          SUM(Quantity__c) Quantity, SUM(SellAmount__c) Amount
                          FROM RSOrderItem__c
                          WHERE Active__c = true
                          AND Date__c >= :startDay
                          AND Date__c <= :endDay
                          AND isTest__c = false
                          AND(RSOrder__r.RetailStore__r.Account__c in :curraccIds
                              AND Product__c in :currproIds)
                          GROUP BY RSOrder__r.RetailStore__r.Account__c, Product__c
                         ];

        for(AggregateResult ar : groupedResults)
            orderItemsAR.put(ar.get('accId') + '&&' + ar.get('proId'), ar);
    }

    public void initSellIns(Map<String, AggregateResult> sellInsAR){
        AggregateResult[] groupedResults;
        groupedResults = [SELECT Account__c accId, Product__c proId, SUM(Quantity__c) Quantity,
                          SUM(Amount__c) Amount
                          FROM SellIn__c
                          WHERE isTest__c = false
                          AND Year__c = :lastYear
                          AND Month__c = :lastMonth
                          //AND Account__c = '001N000000RJmvoIAD'
                          AND(Account__c in :curraccIds
                              AND Product__c in :currproIds)
                          GROUP BY Account__c, Product__c
                         ];

        for(AggregateResult ar : groupedResults)
            sellInsAR.put(ar.get('accId') + '&&' + ar.get('proId'), ar);
    }

    public void initHistorySOs(Map<String, AggregateResult> historySOsAR){
        AggregateResult[] groupedResults;
        groupedResults = [SELECT RetailStore__r.Account__c accId, Product__c proId, SUM(Quantity__c) Quantity, SUM(Amount__c) Amount
                          FROM HistorySO__c
                          WHERE Year__c = :lastYear
                          AND Month__c = :lastMonth
                          AND(RetailStore__r.Account__c in :curraccIds
                              AND Product__c in :currproIds)
                          GROUP BY RetailStore__r.Account__c, Product__c
                         ];

        for(AggregateResult ar : groupedResults)
            historySOsAR.put(ar.get('accId') + '&&' + ar.get('proId'), ar);
    }

    public void initDTStocks(Map<String, DTStock__c> dtstocks){
        List<DTStock__c> dtstockList = new List<DTStock__c>();
        dtstockList = [SELECT Id, Account__c, Product__c, Stock__c
                       FROM DTStock__c
                       WHERE Year__c = :currYear
                       AND Month__c = :currMonth
                       AND(Account__c in :curraccIds
                           AND Product__c in :currproIds)
                      ];

        String key;
        for(DTStock__c dt : dtstockList){
            key = dt.Account__c + '&&' + dt.Product__c;
            dtstocks.put(key, dt);
        }
    }
}
