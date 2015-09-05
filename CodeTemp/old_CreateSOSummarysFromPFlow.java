public class CreateSOSummarysFromPFlow {

    private static Map<String, List<PassengerFlow__c>> sOAmountByStores;
    private static Map<String, List<PassengerFlow__c>> delsOAmountByStores;

    private static Map<String, String> yearMonthRSAuthorizedNos;

    private static Map<String, SOSummary__c> ExistSOAmountByStores;

    private static Map<String, SOSummary__c> currentSOSummarys;

    public static void generateSOSummary(boolean isInsert, boolean isUpdate, boolean isDelete, Map<Id, PassengerFlow__c> PassengerFlowsMap, Map<Id, PassengerFlow__c> oldPassengerFlowsMap){

        initPassengerFlow(isInsert, isDelete, PassengerFlowsMap, oldPassengerFlowsMap);
        if(yearMonthRSAuthorizedNos.size() <=0) return;

        initExistSOSummary();

        currentSOSummarys = new Map<String, SOSummary__c>();
        integrateCurrentSOAmountByStores(isInsert, isDelete);

        Database.upsert(currentSOSummarys.values(), false);
    }

    public static void initPassengerFlow(boolean isInsert, boolean isDelete, Map<Id, PassengerFlow__c> PassengerFlowsMap, Map<Id, PassengerFlow__c> oldPassengerFlowsMap){
        sOAmountByStores = new Map<String, List<PassengerFlow__c>>();
        delsOAmountByStores = new Map<String, List<PassengerFlow__c>>();

        yearMonthRSAuthorizedNos = new Map<String, String>();

        String key = '';
        List<PassengerFlow__c> pfs;
        if(!isDelete){
            List<PassengerFlow__c> PassengerFlowsList = new List<PassengerFlow__c>();
            Set<Id> ids = PassengerFlowsMap.keySet();
            PassengerFlowsList = [SELECT Id, RSAuthorizedNo__c, Date__c, RetailStore__r.Account__c,
                                  RetailStore__c, FloorPassengerFlow__c, StopoverPassengerFlow__c
                                  FROM PassengerFlow__c
                                  WHERE id in :ids
                                 ];

            for(PassengerFlow__c pf : PassengerFlowsList){
                String pfRSAuthorizedNo = pf.RSAuthorizedNo__c;
                String pfYear;
                String pfMonth;
                Date d = pf.Date__c;
                if(d == null) d= date.today();
                pfYear = d.year()+'';
                pfMonth= Util.getEnMonth(d);

                if(String.isNotEmpty(pfRSAuthorizedNo)){
                    key = 'SOAmountByStore&&'+pfYear+'&&'+pfMonth+'##'+pfRSAuthorizedNo;
                    yearMonthRSAuthorizedNos.put(key, key);
                    pfs = sOAmountByStores.get(key);
                    if(pfs == null) pfs = new List<PassengerFlow__c>();
                    pfs.add(pf);
                    sOAmountByStores.put(key, pfs);
                }
            }
        }

        if(!isInsert){
            for(PassengerFlow__c delpf : oldPassengerFlowsMap.values()){
                String delpfRSAuthorizedNo = delpf.RSAuthorizedNo__c;
                String delpfYear;
                String delpfMonth;
                Date d = delpf.Date__c;
                if(d == null) d= date.today();
                delpfYear = d.year()+'';
                delpfMonth = Util.getEnMonth(d);

                if(String.isNotEmpty(delpfRSAuthorizedNo)){
                    key = 'SOAmountByStore&&'+delpfYear+'&&'+delpfMonth+'##'+delpfRSAuthorizedNo;
                    yearMonthRSAuthorizedNos.put(key, key);
                    pfs = delsOAmountByStores.get(key);
                    if(pfs == null) pfs = new List<PassengerFlow__c>();
                    pfs.add(delpf);
                    delsOAmountByStores.put(key, pfs);
                }
            }
        }
    }

    public static void initExistSOSummary(){
        if(yearMonthRSAuthorizedNos.size() > 0) ExistSOAmountByStores = getExistSOSummary('SOAmountByStores', yearMonthRSAuthorizedNos);
    }

    public static Map<String, SOSummary__c > getExistSOSummary(String type, Map<String, String> yearCodes){
        Map<String, SOSummary__c> ExistSOSummarys = new Map<String, SOSummary__c>();

        Map<String, List<String>> soaAuthorizedNos = new Map<String, List<String>>();

        for(String yc : yearCodes.keySet()){
            List<String> yAndCode = yc.split('##');
            if(yAndCode.size() < 2) continue;
            String year = yAndCode[0].split('&&')[1];

            if(type.equals('SOAmountByStores')){
                year += '&&'+yAndCode[0].split('&&')[2];
                List<String> ysoas = soaAuthorizedNos.get(year);
                if(ysoas == null) ysoas = new List<String>();
                ysoas.add(yAndCode[1]);
                soaAuthorizedNos.put(year, ysoas);
            }
        }

        List<SOSummary__c> SOSummarys = new List<SOSummary__c>();
        if(type.equals('SOAmountByStores')){
            for(String yr : soaAuthorizedNos.keySet()){
                String y = yr.split('&&')[0];
                String m = yr.split('&&')[1];
                List<String> yrs = soaAuthorizedNos.get(yr);
                SOSummarys = [SELECT Id, Year__c, RSAuthorizedNo__c, Month__c,
                              FloorPassengerFlow__c, StopoverPassengerFlow__c
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
        }

        return ExistSOSummarys;
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

            soSummary.Year__c = yAndCode[1];

            List<PassengerFlow__c> pfs;
            String month;
            Decimal eachFloorPassengerFlow;
            Decimal eachStopoverPassengerFlow;
            if(!isDelete){
                pfs = sOAmountByStores.get(key);
                if(pfs == null) pfs = new List<PassengerFlow__c>();
                for(PassengerFlow__c pf : pfs){
                    eachFloorPassengerFlow = pf.FloorPassengerFlow__c;
                    if(eachFloorPassengerFlow == null) eachFloorPassengerFlow = 0;
                    eachStopoverPassengerFlow = pf.StopoverPassengerFlow__c;
                    if(eachStopoverPassengerFlow == null) eachStopoverPassengerFlow = 0;

                    month = Util.getEnMonth(pf.Date__c);
                    soSummary.Month__c = month;
                    soSummary.Account__c = pf.RetailStore__r.Account__c;
                    soSummary.RetailStore__c = pf.RetailStore__c;

                    soSummary.StopoverPassengerFlow__c += eachStopoverPassengerFlow;
                    soSummary.FloorPassengerFlow__c += eachFloorPassengerFlow;
                }
            }

            if(!isInsert && isExist){
                pfs = delsOAmountByStores.get(key);
                if(pfs == null) pfs = new List<PassengerFlow__c>();
                for(PassengerFlow__c pf : pfs){
                    eachFloorPassengerFlow = pf.FloorPassengerFlow__c;
                    if(eachFloorPassengerFlow == null) eachFloorPassengerFlow = 0;
                    eachStopoverPassengerFlow = pf.StopoverPassengerFlow__c;
                    if(eachStopoverPassengerFlow == null) eachStopoverPassengerFlow = 0;

                    Date d = pf.Date__c;
                    if(d == null) d = date.today();
                    month = Util.getEnMonth(d);
                    soSummary.Month__c = month;

                    soSummary.StopoverPassengerFlow__c -= eachStopoverPassengerFlow;
                    soSummary.FloorPassengerFlow__c -= eachFloorPassengerFlow;
                }
            }
        }
    }

    public static SOSummary__c verifySOSummary(String type, SOSummary__c sos){

        if(String.isEmpty(sos.Year__c)) sos.Year__c = date.today().year()+'';
        if(String.isEmpty(sos.Month__c)) sos.Month__c = Util.getEnMonth(date.today());
        sos.Type__c = type;

        if(type.equals('SOAmountByStore')){
            if(sos.StopoverPassengerFlow__c == null) sos.StopoverPassengerFlow__c = 0;
            if(sos.FloorPassengerFlow__c == null) sos.FloorPassengerFlow__c = 0;
        }

        return sos;
    }
}
