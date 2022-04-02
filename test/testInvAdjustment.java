import java.sql.SQLException;
import java.util.ArrayList;
import org.junit.AfterClass;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
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
import org.xersys.inventory.base.InvAdjustment;
import org.xersys.lib.pojo.Temp_Transactions;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class testInvAdjustment {
    static Nautilus _nautilus;
    static InvAdjustment _trans;
    static LMasDetTrans _listener;
    
    public testInvAdjustment(){}
    
    @BeforeClass
    public static void setUpClass() {        
        setupConnection();
        setupObject();
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Test
    public void test01NewTransaction(){
        System.out.println("----------------------------------------");
        System.out.println("test01NewTransaction() --> Start");
        System.out.println("----------------------------------------");
        try {
            
            ArrayList<Temp_Transactions> laTemp = (ArrayList<Temp_Transactions>) _trans.TempTransactions();
            
            if (laTemp.size() > 0){
                if (_trans.NewTransaction(laTemp.get(0).getOrderNo())){
                    _trans.displayMasFields();
                    _trans.displayDetFields();
                } else {
                    fail(_trans.getMessage());
                }
            } else {
                if (_trans.NewTransaction()){
                    _trans.displayMasFields();
                    _trans.displayDetFields();
                } else {
                    fail(_trans.getMessage());
                }
            }
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        System.out.println("----------------------------------------");
        System.out.println("test01NewTransaction() --> End");
        System.out.println("----------------------------------------");
    }
    
    @Test
    public void test02SetMaster(){
        System.out.println("----------------------------------------");
        System.out.println("test02SetMaster() --> Start");
        System.out.println("----------------------------------------");
        
        _trans.setMaster(6, "This is a test.");
        assertEquals((String) _trans.getMaster(6), "This is a test.");
        
        System.out.println("----------------------------------------");
        System.out.println("test02SetMaster() --> End");
        System.out.println("----------------------------------------");
    }
    
    @Test
    public void test03SetDetail(){
        System.out.println("----------------------------------------");
        System.out.println("test03SetDetail() --> Start");
        System.out.println("----------------------------------------");
        
        _trans.setDetail(1, 3, "000122007539");
        assertEquals((String) _trans.getDetail(1, 3), "000122007539");
        
        _trans.setDetail(1, 4, 5);
        assertEquals((int) _trans.getDetail(1, 4), 5);
        
        _trans.setDetail(1, 5, 5);
        assertEquals((int) _trans.getDetail(1, 5), 5);
        
        _trans.setDetail(1, 7, "Test only");
        assertEquals((String) _trans.getDetail(1, 7), "Test only");
        
        System.out.println("----------------------------------------");
        System.out.println("test03SetDetail() --> End");
        System.out.println("----------------------------------------");
    }    
    
    @Test
    public void test04SaveTransaction(){
        if (!_trans.SaveTransaction(true))
            fail(_trans.getMessage());
        
        Assert.assertTrue(_trans.OpenTransaction("000122000001"));
        if (!_trans.CloseTransaction()) fail(_trans.getMessage());
        
        Assert.assertTrue(_trans.OpenTransaction("000122000001"));
        if (!_trans.PostTransaction()) fail(_trans.getMessage());
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
        
        _nautilus.setUserID("000100210001");
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
        
        _trans = new InvAdjustment(_nautilus, (String) _nautilus.getBranchConfig("sBranchCd"), false);
        _trans.setListener(_listener);
        _trans.setSaveToDisk(true);
        _trans.setWithUI(false);
    }
}
