package cassdemo.backend;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;

import com.datastax.driver.core.LocalDate;
import com.github.javafaker.Faker;

public class Runner extends Thread {

    String randomStartDateString;
    String randomEndDateString;
    LocalDate randomStartDate;
    LocalDate randomEndDate;
    String randomName;
    int randomSize;

    private static final String PROPERTIES_FILENAME = "config.properties";
    private static final int MAX_RESERVATION_DAYS = 30;
    private static final int MAX_ROOM_SIZE = 4;
    private static final int MAX_YEARS = 2;

    public static int randBetween(int start, int end){
        return start + (int)Math.round(Math.random() * (end - start));
    }

    public void randomize() {
        GregorianCalendar gc_start = new GregorianCalendar();
        GregorianCalendar gc_end = new GregorianCalendar();

        int year = randBetween(2020, (2020 + MAX_YEARS -1));

        gc_start.set(Calendar.YEAR, year);
        gc_end.set(Calendar.YEAR, year);

        int startDay = randBetween(1, gc_start.getActualMaximum(Calendar.DAY_OF_YEAR));
        gc_start.set(Calendar.DAY_OF_YEAR, startDay);

        int endDay = randBetween(gc_start.get(Calendar.DAY_OF_YEAR), (gc_start.get(Calendar.DAY_OF_YEAR) + MAX_RESERVATION_DAYS));
        gc_end.set(Calendar.DAY_OF_YEAR, endDay);
        
        randomStartDate = LocalDate.fromYearMonthDay(gc_start.get(Calendar.YEAR), gc_start.get(Calendar.MONTH)+1, gc_start.get(Calendar.DAY_OF_MONTH));
        randomEndDate = LocalDate.fromYearMonthDay(gc_end.get(Calendar.YEAR), gc_end.get(Calendar.MONTH)+1, gc_end.get(Calendar.DAY_OF_MONTH));
        
        Random rnd = new Random(System.currentTimeMillis());
        randomSize = rnd.nextInt(MAX_ROOM_SIZE) + 1;

        Faker faker = new Faker();
        randomName = faker.name().fullName();

        //System.out.println("Attemt to reserve: " + randomStartDate.toString() + ", " + randomEndDate.toString() + ", " + randomSize + ", " +randomName);
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
        
        
        BackendSession session = new BackendSession(contactPoint, keyspace);

		try {
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