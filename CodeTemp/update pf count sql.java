delete [select id from MTDSales__c where Type__c = '当日报数店铺数'];
List<PassengerFlow__c> pfs = new List<PassengerFlow__c>();
pfs = [select id, Date__c, CreatedDate, LastModifiedDate
       from PassengerFlow__c
       where Active__c = true
      ];
Map<String, List<PassengerFlow__c>> keyPFs = new Map<String, List<PassengerFlow__c>>();
List<PassengerFlow__c> keyPFList;
for(PassengerFlow__c pf : pfs){
    pf.Date__c = pf.CreatedDate.date();
    String key = pf.CreatedDate.date().year()+'&&'+pf.CreatedDate.date().month()+'&&'+pf.CreatedDate.date().day();
    keyPFList = keyPFs.get(key);
    if(keyPFList == null) keyPFList = new List<PassengerFlow__c>();
    keyPFList.add(pf);
    keyPFs.put(key, keyPFList);
}
update pfs;

Map<String, MTDSales__c> mtds = new Map<String, MTDSales__c>();
MTDSales__c mtdSale;
List<String> ymd;
String day;
String mtdkey;
Decimal curPFCounts;
Decimal mtdCount;

for(String key : keyPFs.keySet()){
    ymd = key.split('&&');
    mtdkey = ymd[0] +'&&'+ ymd[1];

    mtdSale = mtds.get(mtdkey);
    if(mtdSale == null) mtdSale = new MTDSales__c();
    if(mtdSale.MTD__c == null) mtdSale.MTD__c = '0';
    mtdCount = Decimal.valueOf(mtdSale.MTD__c);
    day = ymd[2];

    mtdSale.Year__c = ymd[0];
    mtdSale.Month__c = ymd[1];
    mtdSale.Type__c = '当日报数店铺数';

    curPFCounts = keyPFs.get(key).size();
    system.debug(keyPFs);
    mtdCount += curPFCounts;
    mtdSale.MTD__c = String.valueOf(mtdCount);
    mtds.put(mtdkey, mtdSale);

    if(day.equals('1')) mtdSale.Day1__c = curPFCounts;
    else if(day.equals('2')) mtdSale.Day2__c = curPFCounts;
    else if(day.equals('3')) mtdSale.Day3__c = curPFCounts;
    else if(day.equals('4')) mtdSale.Day4__c = curPFCounts;
    else if(day.equals('5')) mtdSale.Day5__c = curPFCounts;
    else if(day.equals('6')) mtdSale.Day6__c = curPFCounts;
    else if(day.equals('7')) mtdSale.Day7__c = curPFCounts;
    else if(day.equals('8')) mtdSale.Day8__c = curPFCounts;
    else if(day.equals('9')) mtdSale.Day9__c = curPFCounts;
    else if(day.equals('10')) mtdSale.Day10__c = curPFCounts;
    else if(day.equals('11')) mtdSale.Day11__c = curPFCounts;
    else if(day.equals('12')) mtdSale.Day12__c = curPFCounts;
    else if(day.equals('13')) mtdSale.Day13__c = curPFCounts;
    else if(day.equals('14')) mtdSale.Day14__c = curPFCounts;
    else if(day.equals('15')) mtdSale.Day15__c = curPFCounts;
    else if(day.equals('16')) mtdSale.Day16__c = curPFCounts;
    else if(day.equals('17')) mtdSale.Day17__c = curPFCounts;
    else if(day.equals('18')) mtdSale.Day18__c = curPFCounts;
    else if(day.equals('19')) mtdSale.Day19__c = curPFCounts;
    else if(day.equals('20')) mtdSale.Day20__c = curPFCounts;
    else if(day.equals('21')) mtdSale.Day21__c = curPFCounts;
    else if(day.equals('22')) mtdSale.Day22__c = curPFCounts;
    else if(day.equals('23')) mtdSale.Day23__c = curPFCounts;
    else if(day.equals('24')) mtdSale.Day24__c = curPFCounts;
    else if(day.equals('25')) mtdSale.Day25__c = curPFCounts;
    else if(day.equals('26')) mtdSale.Day26__c = curPFCounts;
    else if(day.equals('27')) mtdSale.Day27__c = curPFCounts;
    else if(day.equals('28')) mtdSale.Day28__c = curPFCounts;
    else if(day.equals('29')) mtdSale.Day29__c = curPFCounts;
    else if(day.equals('30')) mtdSale.Day30__c = curPFCounts;
    else if(day.equals('31')) mtdSale.Day31__c = curPFCounts;
}
insert mtds.values();
