import org.junit.AfterClass;
import static org.junit.Assert.fail;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.xersys.commander.base.Nautilus;
import org.xersys.commander.base.Property;
import org.xersys.commander.base.SQLConnection;
import org.xersys.commander.crypt.CryptFactory;
import org.xersys.commander.iface.LMasDetTrans;
import org.xersys.commander.util.SQLUtil;
import org.xersys.inventory.base.InvTransfer;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class testInvTransferCancel {
    static Nautilus _nautilus;
    static InvTransfer _trans;
    static LMasDetTrans _listener;
    
    public testInvTransferCancel(){}
    
    @BeforeClass
    public static void setUpClass() {        
        setupConnection();
        setupObject();
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Test
    public void test01SearchTransaction(){
        System.out.println("----------------------------------------");
        System.out.println("test01SearchTransaction() --> Start");
        System.out.println("----------------------------------------");
        if (!_trans.OpenTransaction("000122000003")){
            fail(_trans.getMessage());
        }

        if (!_trans.CancelTransaction()){
            fail(_trans.getMessage());
        }
        System.out.println("----------------------------------------");
        System.out.println("test01SearchTransaction() --> End");
        System.out.println("----------------------------------------");
    }
    
    
    
    private static void setupConnection(){
        String PRODUCTID = "Daedalus";
        
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
        _nautilus = new Nautilus();
        
        _nautilus.setConnection(loConn);
        _nautilus.setEncryption(CryptFactory.make(CryptFactory.CrypType.AESCrypt));
        
        _nautilus.setUserID("0001210001");
        if (!_nautilus.load(PRODUCTID)){
            System.err.println(_nautilus.getMessage());
            System.exit(1);
        } else
            System.out.println("Application driver successfully initialized.");
    }
    
    private static void setupObject(){
        _listener = new LMasDetTrans() {          
            @Override
            public void MasterRetreive(int fnIndex, Object foValue) {
                System.out.println(fnIndex + " ->> " + foValue);
            }
            
            @Override
            public void DetailRetreive(int fnRow, int fnIndex, Object foValue) {
                System.out.println(fnRow + " ->> " + fnIndex + " ->> " + foValue);
            }
            
            @Override
            public void MasterRetreive(String fsFieldNm, Object foValue) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void DetailRetreive(int fnRow, String fsFieldNm, Object foValue) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        };
        
        _trans = new InvTransfer(_nautilus, (String) _nautilus.getBranchConfig("sBranchCd"), false);
        _trans.setListener(_listener);
        _trans.setSaveToDisk(true);
        _trans.setWithUI(false);
    }
}
