global class CreateStoresDailyUploadDataStateBS implements Database.Batchable<RetailStore__c>, Schedulable{

    global void execute(SchedulableContext SC) {
        Database.executebatch(this);
    }

    global List<RetailStore__c> start(Database.BatchableContext BC){
        Date d1 = date.today() - 1000;
        Date d2 = date.today();
        String queryStr = 'SELECT Id';
        queryStr += ', (SELECT Id, SellAmount__c FROM RetailStoreOrders__r WHERE CreatedDate >= :d1 AND CreatedDate < :d2)';
        queryStr += ', (SELECT Id FROM PassengerFlows__r WHERE CreatedDate >= :d1 AND CreatedDate < :d2)';
        queryStr += ' FROM RetailStore__c';

        String whereCase = ' WHERE Status__c = \'Opened\'' + 'AND isAccountCode__c = false';

        queryStr += whereCase;

        return Database.query(queryStr);
    }

    global void execute(Database.BatchableContext BC, List<RetailStore__c> yesterdayStoresDatas){
        List<StoresDailyUploadDataState__c> currStoresStates = new List<StoresDailyUploadDataState__c>();

        StoresDailyUploadDataState__c currStore;
        for(RetailStore__c rs : yesterdayStoresDatas){
            currStore = new StoresDailyUploadDataState__c();
            currStore.RetailStore__c = rs.Id;
            currStore.Date__c = date.today()-1;
            currStore.NoPassengerFlow__c = false;
            currStore.NoSales__c = false;

            if(rs.getSObjects('PassengerFlows__r') == null)
                currStore.NoPassengerFlow__c = true;

            Decimal tempVal;
            if(rs.getSObjects('RetailStoreOrders__r') == null){
                currStore.NoSales__c = true;
            }else{
                tempVal = 0;
                for(RSOrder__c order : rs.getSObjects('RetailStoreOrders__r')){
                    tempVal += order.SellAmount__c;
                }
                if(tempVal==0) currStore.NoSales__c = true;
            }

            if(currStore.NoPassengerFlow__c || currStore.NoSales__c)
                currStoresStates.add(currStore);
        }

        upsert currStoresStates;
    }

    global void finish(Database.BatchableContext BC){}
}
