package cassdemo.backend;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

public class Runner extends Thread {

    String randomStartDate;
    String randomEndDate;
    String randomName;
    int randomSize;

    private static final String PROPERTIES_FILENAME = "config.properties";
    

    //Terrible random
    //TO-DO: Fix it
    public void randomize() {
        Random rnd = new Random(System.currentTimeMillis());
        int randomStartMonth = rnd.nextInt(11) + 1;
        int randomStartDay = rnd.nextInt(30) + 1;
        
        randomSize = rnd.nextInt(4) + 1;
        randomStartDate = 2020 + "-" + randomStartMonth + "-" + randomStartDay;

        int randomEndMonth = rnd.nextInt(11) + 1;
        randomEndMonth = (randomEndMonth < randomStartMonth) ? randomStartMonth : randomEndMonth;

        int randomEndDay = rnd.nextInt(30) + 1;
        randomEndDay = (randomEndDay < randomStartDay) ? randomStartDay + 1 : randomEndDay;
        randomEndDate = 2020 + "-" + randomEndMonth + "-" + randomEndDay;
        randomName = "" + (rnd.nextInt() % 1000);
    }
    
    private void reserve() throws IOException, BackendException {
        String contactPoint = null;
		String keyspace = null;

        Properties properties = new Properties();

		try {
			properties.load(Runner.class.getClassLoader().getResourceAsStream(PROPERTIES_FILENAME));
			contactPoint = properties.getProperty("contact_point");
			keyspace = properties.getProperty("keyspace");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		
		try {
            BackendSession session = new BackendSession(contactPoint, keyspace);
            session.reserveRoom(randomStartDate, randomEndDate, randomSize, randomName);
        } catch(BackendException e) {
            e.printStackTrace();
        }
    }

    public void run()  {
        try {
            randomize();
            reserve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}