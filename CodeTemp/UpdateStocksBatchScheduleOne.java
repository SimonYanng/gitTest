global class UpdateStocksBatchSchedule implements Database.Batchable<Stock__c>, Schedulable {
    private Date currDate;
    private String currYear;
    private String currMonth;
    private Date endDay;
    private Date startDay;
    private String year;
    private String month;

    global void execute(SchedulableContext SC) {
        init(null);
        Database.executebatch(this);
    }

    global UpdateStocksBatchSchedule(Date d){
        init(d);
    }

    global List<Stock__c> start(Database.BatchableContext BC){
        return integrateCurrentStocks();
    }

    global void execute(Database.BatchableContext BC, List<Stock__c> scope) {
        system.debug('scope.size:'+scope.size());
        upsert scope;
    }

    global void finish(Database.BatchableContext BC){}

    public void init(Date d){

        currDate = d;
        if(currDate==null) currDate = date.today();
        currYear = String.valueOf(currDate.year());
        currMonth = String.valueOf(currDate.month());
        endDay = currDate.toStartOfMonth()-1;
        startDay = endDay.toStartOfMonth();
        year = String.valueOf(startDay.year());
        month = String.valueOf(startDay.month());
    }

    global List<Stock__c> integrateCurrentStocks(){
        Map<String, Stock__c> currStocks = new Map<String, Stock__c>();

        Map<String, Stock__c> lastMonthStocks = new Map<String, Stock__c>();
        initStocks(year, month, lastMonthStocks);

        Map<String, List<RSOrderItem__c>> lastMonthOrderItems = new Map<String, List<RSOrderItem__c>>();
        initRSOrderItems(startDay, endDay, lastMonthOrderItems);

        Map<String, List<SellIn__c>> lastMonthSellIns = new Map<String, List<SellIn__c>>();
        initSellIns(year, month, lastMonthSellIns);

        Map<String, Stock__c> thisMonthStocks = new Map<String, Stock__c>();
        initStocks(currYear, currMonth, thisMonthStocks);

        Date d = date.newInstance(2015, 8, 1);
        Map<String, List<HistorySO__c>> historySOs = new Map<String, List<HistorySO__c>>();
        if(currDate <= d){
            initHistorySOs(year, month, historySOs);
        }

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
            if(codeAndDate.size()<4) continue;
            currAccountId = codeAndDate[0];
            currProductId = codeAndDate[1];
            currkey = currYear + '&&' + currMonth;
            currkey = currAccountId + '&&' + currProductId + '&&' + currkey;

            Stock__c lastStock = lastMonthStocks.get(oldkey);
            currStock = thisMonthStocks.get(currkey);
            if(currStock == null) currStock = new Stock__c();
            currStock.Id = null;
            currStock.Account__c = currAccountId;
            currStock.Product__c = currProductId;
            currStock.Year__c = currYear;
            currStock.Month__c = currMonth;
            currStock.Amount__c = lastStock.AdjustedAmount__c;
            currStock.Quantity__c = lastStock.Quantity__c;
            if(currStock.Amount__c == null) currStock.Amount__c = 0;
            if(currStock.Quantity__c == null) currStock.Quantity__c = 0;

            accIds.add(currStock.Account__c);
            proIds.add(currStock.Product__c);
            years.add(currYear);
            months.add(currMonth);
            currStocks.put(currkey, currStock);
        }


        for(String sellinkey : lastMonthSellIns.keySet()){
            codeAndDate = sellinkey.split('&&');
            if(codeAndDate.size()<4) continue;
            currAccountId = codeAndDate[0];
            currProductId = codeAndDate[1];

            currkey = currYear + '&&' + currMonth;
            currkey = currAccountId + '&&' + currProductId + '&&' + currkey;
            currStock = currStocks.get(currkey);
            if(currStock == null) currStock = new Stock__c();
            currStock.Id = null;
            currStock.Account__c = currAccountId;
            currStock.Product__c = currProductId;
            currStock.Year__c = currYear;
            currStock.Month__c = currMonth;
            if(currStock.Amount__c == null) currStock.Amount__c = 0;
            if(currStock.Quantity__c == null) currStock.Quantity__c = 0;

            List<SellIn__c> sellinList = lastMonthSellIns.get(sellinkey);
            if(sellinList == null) sellinList = new List<SellIn__c>();
            for(SellIn__c sellin : sellinList){
                if(sellin.Amount__c != null) currStock.Amount__c += sellin.Amount__c;
            	if(sellin.Quantity__c != null) currStock.Quantity__c += sellin.Quantity__c;
            }

            accIds.add(currStock.Account__c);
            proIds.add(currStock.Product__c);
            years.add(currYear);
            months.add(currMonth);
        }


        if(currDate > d){
            for(String oldkey : lastMonthOrderItems.keySet()){
                codeAndDate = oldkey.split('&&');
                if(codeAndDate.size()<4) continue;
                currAccountId = codeAndDate[0];
                currProductId = codeAndDate[1];

                currkey = currYear + '&&' + currMonth;
                currkey = currAccountId + '&&' + currProductId + '&&' + currkey;
                currStock = currStocks.get(currkey);
                if(currStock == null) currStock = new Stock__c();
                currStock.Id = null;
                currStock.Account__c = currAccountId;
                currStock.Product__c = currProductId;
                currStock.Year__c = currYear;
                currStock.Month__c = currMonth;
                if(currStock.Amount__c == null) currStock.Amount__c = 0;
                if(currStock.Quantity__c == null) currStock.Quantity__c = 0;

                List<RSOrderItem__c> orderItemList = lastMonthOrderItems.get(oldkey);
                if(orderItemList == null) orderItemList = new List<RSOrderItem__c>();
                for(RSOrderItem__c item : orderItemList){
                    if(item.SellAmount__c != null) currStock.Amount__c -= item.SellAmount__c;
                    if(item.Quantity__c != null) currStock.Quantity__c -= item.Quantity__c;
                }

                accIds.add(currStock.Account__c);
                proIds.add(currStock.Product__c);
                years.add(currYear);
                months.add(currMonth);
            }
        }else{
            for(String oldkey : historySOs.keySet()){
                codeAndDate = oldkey.split('&&');
                if(codeAndDate.size()<4) continue;
                currAccountId = codeAndDate[0];
                currProductId = codeAndDate[1];

                currkey = currYear + '&&' + currMonth;
                currkey = currAccountId + '&&' + currProductId + '&&' + currkey;
                currStock = currStocks.get(currkey);
                if(currStock == null) currStock = new Stock__c();
                currStock.Id = null;
                currStock.Account__c = currAccountId;
                currStock.Product__c = currProductId;
                currStock.Year__c = currYear;
                currStock.Month__c = currMonth;
                if(currStock.Amount__c == null) currStock.Amount__c = 0;
                if(currStock.Quantity__c == null) currStock.Quantity__c = 0;
                List<HistorySO__c> historySOList = historySOs.get(oldkey);
                if(historySOList == null) historySOList = new List<HistorySO__c>();
                for(HistorySO__c hso : historySOList){
                    if(hso.Amount__c != null) currStock.Amount__c -= hso.Amount__c;
                    if(hso.Quantity__c != null) currStock.Quantity__c -= hso.Quantity__c;
                }

                accIds.add(currStock.Account__c);
                proIds.add(currStock.Product__c);
                years.add(currYear);
                months.add(currMonth);
            }
        }

		/*
        List<DTStock__c> dtstocks = new List<DTStock__c>();
        dtstocks = [select Id, Account__c, Product__c, Stock__c, Year__c,Month__c
                    from DTStock__c
                    where Account__c in :accIds
                    and Product__c in :proIds
                    and Year__c in :years
                    and Month__c in :months
                   ];
        Stock__c st;
        String k;
        for(DTStock__c dt : dtstocks){
            k = dt.Account__c + '&&' + dt.Product__c + '&&' + dt.Year__c + '&&' + dt.Month__c;
            st = currStocks.get(k);
            if(st==null) st = new Stock__c();
            dt.Stock__c = st.Amount__c;
        }
        */

        return currStocks.values();
    }

    public void initStocks(String year, String month, Map<String, Stock__c> stocks){
        List<Stock__c> stockList = new List<Stock__c>();
        stockList = [SELECT Id, Account__c, Year__c, Month__c, Quantity__c, AdjustedAmount__c,
                     Amount__c, Product__c, Product__r.Name, Account__r.AccountCode__c
                     FROM Stock__c
                     WHERE Year__c = :year
                     AND Month__c = :month
                    ];

        String key;
        for(Stock__c stock : stockList){
            key = stock.Account__c + '&&' + stock.Product__c
                +'&&'+year+'&&'+month;
            stocks.put(key, stock);
        }
    }

    public void initRSOrderItems(Date startDay, Date endDay, Map<String, List<RSOrderItem__c>> orderItems){
        List<RSOrderItem__c> RSOItems = new List<RSOrderItem__c>();
        RSOItems = [SELECT Id, ProductCode__c, Date__c, Product__c,
                    Quantity__c, SellAmount__c, RSOrder__r.RetailStore__r.Account__c
                    FROM RSOrderItem__c
                    WHERE Active__c = true
                    AND Date__c >= :startDay
                    AND Date__c <= :endDay
                    AND isTest__c = false
                   ];

        String key;
        List<RSOrderItem__c> temp;
        for(RSOrderItem__c rsoitem : RSOItems){
            key = rsoitem.RSOrder__r.RetailStore__r.Account__c + '&&' + rsoitem.Product__c
                +'&&'+startDay.year()+'&&'+startDay.month();
            temp = orderItems.get(key);
            if(temp==null) temp = new List<RSOrderItem__c>();
            temp.add(rsoitem);
            orderItems.put(key, temp);
        }
    }

    public void initSellIns(String year, String month, Map<String, List<SellIn__c>> sellIns){
        List<SellIn__c> sellinList = new List<SellIn__c>();
        sellinList = [SELECT Id, Account__c, Year__c, Month__c, Quantity__c,
                      Amount__c, Product__c, Product__r.Name, Account__r.AccountCode__c
                      FROM SellIn__c
                      WHERE isTest__c = false
                      AND Year__c = :year
                      AND Month__c = :month
                     ];

        String key;
        List<SellIn__c> temp;
        for(SellIn__c sellin : sellinList){
            key = sellin.Account__c + '&&' + sellin.Product__c
                +'&&'+year+'&&'+month;
            temp = sellIns.get(key);
            if(temp==null) temp = new List<SellIn__c>();
            temp.add(sellin);
            sellIns.put(key, temp);
        }
    }

    public void initHistorySOs(String year, String month, Map<String, List<HistorySO__c>> historySOs){
        List<HistorySO__c> historySOList = new List<HistorySO__c>();
        historySOList = [SELECT Id, RetailStore__r.Account__c, Year__c, Month__c, Quantity__c,
                         Amount__c, Product__c
                         FROM HistorySO__c
                         WHERE Year__c = :year
                         AND Month__c = :month
                        ];

        String key;
        List<HistorySO__c> temp;
        for(HistorySO__c hso : historySOList){
            key = hso.RetailStore__r.Account__c + '&&' + hso.Product__c
                +'&&'+year+'&&'+month;
            temp = historySOs.get(key);
            if(temp==null) temp = new List<HistorySO__c>();
            temp.add(hso);
            historySOs.put(key, temp);
        }
    }
}
