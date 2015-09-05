global with sharing class PhotoViewerCtrl {

    public Date searchDate {get; set;}
    public List<SelectOption> types {get; set;}
    public List<SelectOption> stores {get; set;}
    public List<SelectOption> accs {get; set;}

    public PhotoViewerCtrl(){
        init();
    }

    public void init(){
        searchDate = date.today();

        accs = new List<SelectOption>();
        for(Account acc : [select id, Name from Account]){
            accs.add(new SelectOption(acc.Id, acc.Name));
        }

        stores = new List<SelectOption>();
        for(RetailStore__c st : [select id, Name, AuthorizedNo__c from RetailStore__c]){
            stores.add(new SelectOption(st.Id, st.Name));
        }

        types = new List<SelectOption>();
        types.add(new SelectOption('客流提报', '客流提报'));
        types.add(new SelectOption('销量提报', '销量提报'));
    }

    @RemoteAction
    global static List<String> getRetailStores(String accId){
        List<String> stores = new List<String>();
        for(RetailStore__c st: [SELECT Id, Name FROM RetailStore__c WHERE Account__c = :accId]){
            stores.add(st.Id+'##'+st.Name);
        }

        return stores;
    }

    @RemoteAction
    global static List<String> getPhotosIds(String storeId, String type, String dt, String photoName){
        List<String> photoIds = new List<String>();
        /*
        List<Id> recordIds = [SELECT Id, Name
        RSOrder__c
        WHERE RetailStore__c = :storeId
        AND
        ];
        for(Attachment att: [SELECT Id FROM Attachment WHERE ParentId = :storeId, Name = :photoName]){
            photoIds.add(att.Id);
        }
        */
        return photoIds;
    }
}
