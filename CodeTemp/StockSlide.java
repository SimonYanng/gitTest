Dev:

SELECT SUM(Amount__c) from Stock__c where Month__c = '4' and Account__c = '001N000000RJmwCIAT'
SELECT SUM(Amount__c) from Sellin__c where Month__c = '4' and Account__c = '001N000000RJmwCIAT'
SELECT SUM(Amount__c) from Historyso__c where Month__c = '4' and RetailStore__r.Account__c = '001N000000RJmwCIAT'

001N000000RJmvoIAD
11093092
 +240001.79
 -383358.92
-------------
10949734.87
10949734.87      √


001N000000RJmwBIAT
14032068
 +234421.35
 -612004
-------------
13654485.35
13654485.35      √


001N000000RJmwCIAT
5809286                  5483
+345267.57              +1659
-564869                 -1558
-------------------------------
5589684.57               5584
5589684.57      √        5584    √
