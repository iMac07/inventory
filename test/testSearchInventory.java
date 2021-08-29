
import java.util.ArrayList;
import org.xersys.commander.base.Nautilus;
import org.xersys.commander.base.Property;
import org.xersys.commander.base.SQLConnection;
import org.xersys.commander.crypt.CryptFactory;
import org.xersys.inventory.search.InvSearchF;

public class testSearchInventory {
    public static void main(String [] args){
        final String PRODUCTID = "AppX";
        
        //get database property
        Property loConfig = new Property("db-config.properties", PRODUCTID);
        if (!loConfig.loadConfig()){
            System.err.println(loConfig.getMessage());
            System.exit(1);
        } else System.out.println("Database configuration was successfully loaded.");
        
        //connect to database
        SQLConnection loConn = new SQLConnection();
        loConn.setProperty(loConfig);
        if (loConn.getConnection() == null){
            System.err.println(loConn.getMessage());
            System.exit(1);
        } else
            System.out.println("Connection was successfully initialized.");        
        
        //load application driver
        Nautilus loNautilus = new Nautilus();
        
        loNautilus.setConnection(loConn);
        loNautilus.setEncryption(CryptFactory.make(CryptFactory.CrypType.AESCrypt));
        
        if (!loNautilus.load(PRODUCTID)){
            System.err.println(loNautilus.getMessage());
            System.exit(1);
        } else
            System.out.println("Application driver successfully initialized.");
        
        
        InvSearchF instance = new InvSearchF(loNautilus, InvSearchF.SearchType.searchStocks);
        instance.setKey("sBarCodex");
        instance.setValue("");
        instance.setExact(false);
        
        System.out.println(instance.Search().toJSONString());
        
        
        ArrayList laFields = instance.getColumns();
        ArrayList laNames = instance.getColumnNames();
        
        for (int lnCtr = 0; lnCtr <= laFields.size()-1; lnCtr++){
            System.out.println(laNames.get(lnCtr) + " - " + laFields.get(lnCtr));
        }
    }
}