update [SELECT Id FROM RSOrder__c];

update [SELECT Id FROM PassengerFlow__c];

delete [SELECT id FROM SOSummary__c];

List<PassengerFlow__c> pfs = [SELECT id, Date__c, CreatedDate FROM PassengerFlow__c];
for(PassengerFlow__c pf : pfs) pf.Date__c = pf.CreatedDate.date();
update pfs;


--------------------------------------------------------------------
update [SELECT id FROM SOSummary__c];

update [SELECT id FROM Budget__c WHERE Account__c != null];
update [SELECT id FROM Budget__c WHERE RetailStore__c != null];
update [SELECT id FROM Budget__c WHERE City__c != null];

delete [SELECT id FROM YTDSales__c];




--------------------------------------------------------------------
update [select id from PassengerFlow__c];

delete [SELECT id FROM MTDSales__c];



--------------------------------------------------------------------
delete [select id from MTDSales__c];

Date d = date.newInstance(2015, 7, 1);
update [select id from RSOrder__c where CreatedDate >= :d];
update [select id from RSOrderItem__c];
update [select id from PassengerFlow__c];

update [select id from MTDSales__c];

--------------------------------------------------------------------
List<MTDSales__c> mtds = new List<MTDSales__c>();
mtds = [select id,Date__c,Year__c,Month__c from MTDSales__c];
for(MTDSales__c mtd : mtds){
    mtd.Date__c = Date.newInstance(Integer.valueOf(mtd.Year__c),
                                   Integer.valueOf(mtd.Month__c), 1);
}

update mtds;


List<Product__c> pros = new List<Product__c>();
pros = [select id,Name from Product__c];
for(Product__c p : pros){
    p.Name = p.Name.toUpperCase();
}

update pros;


delete [select id from ActivationTraffic__c];
update [select id from PassengerFlow__c];
Date d = date.newInstance(2015, 7, 1);
update [select id from RSOrder__c where CreatedDate >= :d];



--------------------------------------------------------------------
更新库存的数据的年月份
List<Stock__c> stocks = new List<Stock__c>();
stocks = [select id,year__c, month__c
          from Stock__c
          where year__c = ''
          limit 10000
         ];
for(Stock__c st : stocks){
    st.year__c = '2015';
    st.mont__c = '4';
}

update stocks;


List<Stock__c> stocks = new List<Stock__c>();
stocks = [select id,year__c, month__c, StockValue__c
          from Stock__c
          where Amount__c = null
          limit 10000
         ];
for(Stock__c st : stocks){
    st.Amount__c = st.StockValue__c;
}

update stocks;





Account acc = [select id, isTest__c from Account where AccountCode__c = '00000001'];
acc.isTest__c = true;



UpdateStocksBatchSchedule myBatchObject = new UpdateStocksBatchSchedule();
Database.executeBatch(myBatchObject);

Date d = date.newInstance(2015, 5, 4);
Database.executeBatch(new UpdateStocksBatchSchedule(d,null,null,null,null));

Date d = date.newInstance(2015, 5, 4);
Database.executeBatch(new UpdateStocksBatchSchedule(d), 200);

MergeStocksBatch myBatchObject = new MergeStocksBatch();
Database.executeBatch(myBatchObject);

Date d = date.newInstance(2015, 6, 30);
update [select id from RSOrderItem__c where RSOrder__r.CreatedDate>:d];


delete [select id from Stock__c limit 10000];

delete [select id from Stock__c where Month__c != '4' limit 10000];

---------------------------------------------------------------------------------

delete [select id from Stock__c where month__c != '4' limit 5000];

List<Account> accs = [select id from Account];
List<RetailStore__c> rss = new List<RetailStore__c>();
for(Account acc : accs){
    rss = [select Id,Region__c from RetailStore__c where Account__c = :acc.Id];
    if(rss.size()>0)acc.Region__c = rss[0].Region__c;
}

update accs;




Date d = date.newInstance(2015, 7, 1);
update [select id
        from RSOrderItem__c
        where RSOrder__r.CreatedDate>=:d
       ];


delete [select id from YTDSales__c];
delete [select id from SOSummary__c];
delete [select id from MTDSales__c];
delete [select id from DTStock__c];
delete [select id from ActivationTraffic__c];


    String query;
    Date d1 = date.newInstance(2015, 7, 1);
    Date d2 = date.newInstance(2015, 7, 31);
    //query = 'select id from RSOrder__c where CreatedDate>=:d1 And CreatedDate<=:d2 And isTest__c=false and Active__c = true';
    query = 'select id from RSOrderItem__c where RSOrder__r.CreatedDate>=:d1 And RSOrder__r.CreatedDate<=:d2 And isTest__c=false and Active__c = true';
    //query = 'select id from PassengerFlow__c where CreatedDate>=:d1 And CreatedDate<=:d2 And isTest__c=false and Active__c = true';
    Database.executeBatch(new updateJulRSOIBatch(query, d1, d2, null, null));




Datetime d1 = datetime.newInstance(2015, 7, 1, 0, 0, 0);
Datetime d2 = datetime.newInstance(2019, 7, 31, 0, 0, 0);

//Datetime d3 = datetime.newInstance(2015, 7, 27, 22, 0, 0);
//Datetime d4 = datetime.newInstance(2015, 7, 28, 0, 0, 0);

update [select id from RSOrderItem__c where RSOrder__r.CreatedDate>=:d1 And RSOrder__r.CreatedDate<=:d2 And isTest__c=false and Active__c = true];
//update [select id from RSOrder__c where CreatedDate>=:d3 And CreatedDate<=:d4 And isTest__c=false and Active__c = true];
//update [select id from RSOrderItem__c where RSOrder__r.CreatedDate>=:d1 And RSOrder__r.CreatedDate<=:d2 And isTest__c=false and Active__c = true];



Datetime d1 = datetime.newInstance(2015, 7, 31, 00, 00, 0);
Datetime d2 = datetime.newInstance(2019, 7, 31, 00, 00, 0);

//Datetime d3 = datetime.newInstance(2015, 7, 27, 22, 0, 0);
//Datetime d4 = datetime.newInstance(2015, 7, 28, 0, 0, 0);

update [select id from PassengerFlow__c where CreatedDate>=:d1 And CreatedDate<=:d2 And isTest__c=false and Active__c = true];
//update [select id from RSOrder__c where CreatedDate>=:d3 And CreatedDate<=:d4 And isTest__c=false and Active__c = true];
//update [select id from RSOrderItem__c where RSOrder__r.CreatedDate>=:d1 And RSOrder__r.CreatedDate<=:d2 And isTest__c=false and Active__c = true];

//String query;
//Datetime dt1 = datetime.newInstance(2015, 7, 7, 0, 0, 0);
//Datetime dt2 = datetime.newInstance(2015, 7, 8, 0, 0, 0);
//query = 'select id from PassengerFlow__c where CreatedDate>=:dt1 And CreatedDate<=:dt2 And isTest__c=false and Active__c = true';
//Database.executeBatch(new updateJulRSOIBatch(query, null, null, dt1, dt2));








select id, AccountCode__c, RSAuthorizedNo__c, Year__c, FormulaCity__c, Jan__c, Jul__c from Budget__c where FormulaCity__c = '北京市'


select Jul__c from CityBudget__c where City__c = '北京市'


select Sum(Jul__c) from Budget__c where FormulaCity__c = '北京市'





List<YTDSales__c> ytds = [SELECT OctBudget__c,NovBudget__c,DecBudget__c,
                          JanBudget__c,FebBudget__c,MarBudget__c,AprBudget__c,MayBudget__c,
                          JunBudget__c,JulBudget__c,AugBudget__c,SepBudget__c, Id
                          FROM YTDSales__c];

for(YTDSales__c ytd : ytds){
    ytd.OctBudget__c = 0;
    ytd.NovBudget__c = 0;
    ytd.DecBudget__c = 0;
    ytd.JanBudget__c = 0;
    ytd.FebBudget__c = 0;
    ytd.MarBudget__c = 0;
    ytd.AprBudget__c = 0;
    ytd.MayBudget__c = 0;
    ytd.JunBudget__c = 0;
    ytd.JulBudget__c = 0;
    ytd.AugBudget__c = 0;
    ytd.SepBudget__c = 0;
}

update ytds;




List<MTDSales__c> mtds;
mtds = [select id, Type__c, Day1__c, Day2__c, Day3__c, Day4__c, Day5__c,
        Day6__c, Day7__c, Day8__c, Day9__c, Day10__c, Day11__c, Day12__c,
        Day13__c, Day14__c, Day15__c, Day16__c, Day17__c, Day18__c,
        Day19__c, Day20__c, Day21__c, Day22__c, Day23__c, Day24__c,
        Day25__c, Day26__c, Day27__c, Day28__c, Day29__c, Day30__c, Day31__c
        from MTDSales__c where Type__c = '当日报数店铺数'
       ];
for(MTDSales__c mtd : mtds){
    if(mtd.Day1__c < 0 ) mtd.Day1__c = -mtd.Day1__c;
    if(mtd.Day2__c < 0 ) mtd.Day2__c = -mtd.Day2__c;
    if(mtd.Day3__c < 0 ) mtd.Day3__c = -mtd.Day3__c;
    if(mtd.Day4__c < 0 ) mtd.Day4__c = -mtd.Day4__c;
    if(mtd.Day5__c < 0 ) mtd.Day5__c = -mtd.Day5__c;
    if(mtd.Day6__c < 0 ) mtd.Day6__c = -mtd.Day6__c;
    if(mtd.Day7__c < 0 ) mtd.Day7__c = -mtd.Day7__c;
    if(mtd.Day8__c < 0 ) mtd.Day8__c = -mtd.Day8__c;
    if(mtd.Day9__c < 0 ) mtd.Day9__c = -mtd.Day9__c;
    if(mtd.Day10__c < 0 ) mtd.Day10__c = -mtd.Day10__c;
    if(mtd.Day11__c < 0 ) mtd.Day11__c = -mtd.Day11__c;
    if(mtd.Day12__c < 0 ) mtd.Day12__c = -mtd.Day12__c;
    if(mtd.Day13__c < 0 ) mtd.Day13__c = -mtd.Day13__c;
    if(mtd.Day14__c < 0 ) mtd.Day14__c = -mtd.Day14__c;
    if(mtd.Day15__c < 0 ) mtd.Day15__c = -mtd.Day15__c;
    if(mtd.Day16__c < 0 ) mtd.Day16__c = -mtd.Day16__c;
    if(mtd.Day17__c < 0 ) mtd.Day17__c = -mtd.Day17__c;
    if(mtd.Day18__c < 0 ) mtd.Day18__c = -mtd.Day18__c;
    if(mtd.Day19__c < 0 ) mtd.Day19__c = -mtd.Day19__c;
    if(mtd.Day20__c < 0 ) mtd.Day20__c = -mtd.Day20__c;
    if(mtd.Day21__c < 0 ) mtd.Day21__c = -mtd.Day21__c;
    if(mtd.Day22__c < 0 ) mtd.Day22__c = -mtd.Day22__c;
    if(mtd.Day23__c < 0 ) mtd.Day23__c = -mtd.Day23__c;
    if(mtd.Day24__c < 0 ) mtd.Day24__c = -mtd.Day24__c;
    if(mtd.Day25__c < 0 ) mtd.Day25__c = -mtd.Day25__c;
    if(mtd.Day26__c < 0 ) mtd.Day26__c = -mtd.Day26__c;
    if(mtd.Day27__c < 0 ) mtd.Day27__c = -mtd.Day27__c;
    if(mtd.Day28__c < 0 ) mtd.Day28__c = -mtd.Day28__c;
    if(mtd.Day29__c < 0 ) mtd.Day29__c = -mtd.Day29__c;
    if(mtd.Day30__c < 0 ) mtd.Day30__c = -mtd.Day30__c;
    if(mtd.Day31__c < 0 ) mtd.Day31__c = -mtd.Day31__c;
}
update mtds;


-----------------------------------------
List<RetailStore__c> temp1 = [select id from RetailStore__c where Account__c = '00128000003IjEG'];
List<RetailStore__c> temp2 = [select id from RetailStore__c where Account__c = '00128000003IjEE'];

for(RetailStore__c st : temp1){
	st.Account__c = '00128000003IjEE';
}

for(RetailStore__c st : temp2){
	st.Account__c = '00128000003IjEG';
}

update temp1;
update temp2;






    AggregateResult[] groupedResults;
    Set<String> doneaccIds = new Set<String>();
    Set<String> doneproIds = new Set<String>();

    groupedResults = [SELECT RetailStore__r.Account__c accId, Product__c proId
                      FROM HistorySO__c
                      WHERE Year__c = '2015'
                      AND Month__c = '4'
                      AND(RetailStore__r.Account__c not in :doneaccIds
                          OR Product__c not in :doneproIds)
                      GROUP BY RetailStore__r.Account__c, Product__c
                     ];
    List<String> accs = new List<String>();
    for (AggregateResult ar : groupedResults)  {
        accs.add((String)ar.get('accId'));
    }

    system.debug('accs'+accs);





    delete [select id from Stock__c where Month__c != '4' limit 10000];

Date d = date.newInstance(2015, 5, 4);
Database.executeBatch(new UpdateStocksBatchSchedule(d,null,null,null,null));


select sum(Amount__C) from stock__C where month__c = '7' and Product__r.Brand__c = '派克' and Product__r.Series__c = '卓尔' and Account__c = '00128000003IjED' group by Account__c, Product__r.Brand__c, Product__r.Series__c


select sum(Amount__C) from stock__C where month__c = '7' and Account__c = '00128000003IjEI' group by Account__c



-------------------------------------------------------------------------------
List<Attachment> attachs = [SELECT Name, Id FROM Attachment];
for(Attachment att : attachs){
    if(att.Name.indexOf('.jpg')<0) att.Name += '.jpg';
}
update attachs;


delete [select id from StoresDailyUploadDataState__c];

Database.executeBatch(new CreateStoresDailyUploadDataStateBS());



Date d = date.newInstance(2015, 6, 4);
Database.executeBatch(new UpdateStocksBatchSchedule(d));



-------------------------------------------------------------------------------
select id, RetailStore__c, RetailStore__r.AuthorizedNo__c,RetailStore__r.Status__c from PassengerFlow__c where Date__c = 2015-08-18 AND Active__c = true AND RetailStore__r.Status__c = 'Opened'



Database.executeBatch(new HistorySoCreateOrdersBS());
Database.executeBatch(new HistorySoCreateOrderItemsBS());



-- 查询某客户，某月的库存
SELECT Month__c,SUM(Quantity__c),SUM(Amount__c) from Stock__c where Account__c = '00128000005Bmv6' GROUP BY Account__C, Month__c

-- 查询某客户，某月的库存 按产品品牌和系列
SELECT Month__c,Account__c,Product__r.Brand__c, Product__r.Series__c,SUM(Quantity__c),SUM(Amount__c) from Stock__c where Account__c = '00128000005Bmv6' AND Month__c = '7' GROUP BY Account__C, Month__c,Product__r.Brand__c, Product__r.Series__c


-- 某时间段内的客户的HistorySO
SELECT SUM(Quantity__c), SUM(Amount__c) from HistorySO__c where RetailStore__r.Account__c = '00128000003IjE9' AND MonthDate__c >= 2014-07-01 AND MonthDate__c < 2015-08-01 GROUP BY RetailStore__r.Account__c

-- 按年月累计某时间段内的HistorySO
SELECT Year__c, Month__c,SUM(Quantity__c),SUM(Amount__c) from HistorySO__c where RetailStore__r.Account__c = '00128000003IjEK' AND MonthDate__c >= 2014-07-01  AND MonthDate__c < 2015-08-01 GROUP BY RetailStore__r.Account__c, Year__c, Month__c

-- 按月累计某时间段内城市的HistorySO
SELECT RetailStore__r.City__c City, RetailStore__r.CityTier__c CityTier,
                    SUM(Quantity__c) Quantity, SUM(Amount__c) Amount
                    FROM HistorySO__c
                    WHERE MonthDate__c >= 2015-01-01
                    AND MonthDate__c < 2015-08-01
                    AND RetailStore__r.City__c = '昆明市'
                    GROUP BY RetailStore__r.City__c, RetailStore__r.CityTier__c

-- 查询累计销售
SELECT Date__c,sUM(SellAmount__c),sUM(Quantity__c) from RSOrder__c where Date__c >= 2015-08-01 AND Active__c = true AND isTest__c = false GROUP BY Date__c

-- 查询累计客流和经停人数
SELECT Date__c, sUM(FloorPassengerFlow__c), SUM(StopoverPassengerFlow__c) from PassengerFlow__c where Date__c >= 2015-08-01 AND Active__c = true AND isTest__c = false GROUP BY Date__c


-- 按日期查看未提交数据的门店信息
SELECT Date__c,COUNT(id) from StoresDailyUploadDataState__c GROUP BY Date__c



-- 某客户的历史销售额(按年-月)
SELECT RetailStore__r.Account__C,Year__c,Month__c,SUM(Amount__c) from HistorySO__c WHERE MonthDate__c >= 2014-07-01 AND MonthDate__c < 2015-07-01 AND RetailStore__r.Account__C ='00128000003IjEi' GROUP BY RetailStore__r.Account__C,Year__c,Month__c
-- (不按年-月)
SELECT RetailStore__r.Account__C,SUM(Amount__c) from HistorySO__c WHERE MonthDate__c >= 2014-07-01 AND MonthDate__c < 2015-07-01 AND RetailStore__r.Account__C ='00128000003IjEi' GROUP BY RetailStore__r.Account__C


-- 按客户产品品牌节系列
SELECT RetailStore__r.Account__C,Product__r.Brand__c, Product__r.Series__c,SUM(Amount__c) from HistorySO__c WHERE MonthDate__c >= 2014-07-01 AND MonthDate__c < 2015-07-01 AND RetailStore__r.Account__C ='00128000003IjF3' GROUP BY RetailStore__r.Account__C, Product__r.Brand__c, Product__r.Series__c




---------------------------------------------------------------------------------------------
DataBase.executeBatch(new UpdateDTStockFromStockB('2015', '5'), 1);
DataBase.executeBatch(new UpdateYTDSalesFromHsoB('2015', '7'), 1);
DataBase.executeBatch(new UpdateYTDSalesFromCityBudgetB('2015', '7'));



-- 查询客户和零售的SOSuammary
select id,Account__c,Account__r.Name,RetailStore__c,RetailStore__r.Name,year__c,month__c,Quantity__c,Amount__c from AccAndStoreSOSummary__c

-- 查询城市的SOSuammary
select id,City__c,CityTier__c,year__c,month__c,Quantity__c,Amount__c from CitySOSummay__c
