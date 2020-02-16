package cassdemo.backend;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
 * For error handling done right see: 
 * https://www.datastax.com/dev/blog/cassandra-error-handling-done-right
 * 
 * Performing stress tests often results in numerous WriteTimeoutExceptions, 
 * ReadTimeoutExceptions (thrown by Cassandra replicas) and 
 * OpetationTimedOutExceptions (thrown by the client). Remember to retry
 * failed operations until success (it can be done through the RetryPolicy mechanism:
 * https://stackoverflow.com/questions/30329956/cassandra-datastax-driver-retry-policy )
 */

public class BackendSession {

	static int counter = 0;
	static ReentrantLock counterLock = new ReentrantLock(true);

	static void incrementCounter() {
		counterLock.lock();
		try {
			counter++;
			System.out.println("Counter [" + counter + "]");
		} finally {
			counterLock.unlock();
		}
	}

	private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);

	public static BackendSession instance = null;

	private Session session;

	public BackendSession(String contactPoint, String keyspace) throws BackendException {

		Cluster cluster = Cluster.builder().addContactPoint(contactPoint).build();
		try {
			session = cluster.connect(keyspace);
		} catch (Exception e) {
			throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
		}
		prepareStatements();
	}

	private static PreparedStatement SELECT_ALL_FROM_ROOMS;
	private static PreparedStatement SELECT_RDATE;
	private static PreparedStatement UPDATE_ROOM;
	private static PreparedStatement SELECT_EXACT;
	private static PreparedStatement SELECT_INIT;
	private static PreparedStatement CLEAR_ROOM;

	private void prepareStatements() throws BackendException {
		try {
			SELECT_ALL_FROM_ROOMS = session.prepare("SELECT * FROM Rooms;");
			SELECT_RDATE = session.prepare("SELECT * FROM Rooms where rDate = ?");
			SELECT_EXACT = session.prepare("SELECT * FROM Rooms WHERE rDate = ? and roomid = ?");
			SELECT_INIT = session.prepare("SELECT * FROM Initial_Rooms");
			CLEAR_ROOM = session.prepare("DELETE FROM Rooms where rDate = ? and roomid = ?");
			UPDATE_ROOM = session
					.prepare("INSERT INTO Rooms (roomId, rDate, name, size) VALUES (?, ?, ?, ?)");
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

	public Set<Room> selectAll() throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_ROOMS);

		ResultSet rs = null;
		Set<Room> roomInfo = new HashSet<Room>();
		HashMap<Integer, Room> rooms = new HashMap<>();
		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			int roomId = row.getInt("roomId");
			LocalDate rDate = row.getDate("rDate");
			String name = row.getString("name");
			int size = row.getInt("size");

			if (!rooms.keySet().contains(roomId)) {
				rooms.put(roomId,  new Room(roomId, size));
			}

			Reservation reservation = new Reservation(rDate, name);
			rooms.get(roomId).reservation = reservation;
		}

		for (Room room: rooms.values()) {
			roomInfo.add(room);
		}
		return roomInfo;
	}

	public Set<Room> selectRdate(LocalDate finalDate) throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_RDATE);
		bs.bind(finalDate);

		ResultSet rs = null;
		Set<Room> roomInfo = new HashSet<Room>();
		HashMap<Integer, Room> rooms = new HashMap<Integer, Room>();

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		if (rs.isExhausted()) {
			throw new BackendException("Empty ResultSet");
		}

		for (Row row : rs) {
			int roomId = row.getInt("roomId");
			LocalDate rDate = row.getDate("rDate");
			String name = row.getString("name");
			int size = row.getInt("size");

			if (!rooms.keySet().contains(roomId)) {
				rooms.put(roomId,  new Room(roomId, size));
			}

			Reservation reservation = new Reservation(rDate, name);
			rooms.get(roomId).reservation = reservation;
		}

		for (Room room: rooms.values()) {
			roomInfo.add(room);
		}
		return roomInfo;
	}

	public List<Room> selectInit() throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_INIT);
		bs.bind();

		ResultSet rs = null;
		List<Room> roomInfo = new ArrayList<Room>();
		HashMap<Integer, Room> rooms = new HashMap<Integer, Room>();

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		if (rs.isExhausted()) {
			throw new BackendException("Empty ResultSet");
		}

		for (Row row : rs) {
			int roomId = row.getInt("roomId");
			int size = row.getInt("size");

			if (!rooms.keySet().contains(roomId)) {
				rooms.put(roomId, new Room(roomId, size));
			}
		}

		for (Room room: rooms.values()) {
			roomInfo.add(room);
		}
		return roomInfo;
	}


	public void reserveRoom(LocalDate sDate, LocalDate eDate, int size, String name) throws BackendException {
		
		java.time.LocalDate tempSDate = java.time.LocalDate.of(sDate.getYear(), sDate.getMonth(), sDate.getDay());
        java.time.LocalDate tempEDate = java.time.LocalDate.of(eDate.getYear(), eDate.getMonth(), eDate.getDay());
        java.time.LocalDate rDay = tempSDate;
		int totalSize = 0;
		List<Room> roomInfo = selectInit();

		Set<Room> freeRooms = new HashSet<Room>();
		Set<Room> reservedRooms = new HashSet<Room>(); 
		HashMap<Integer, LocalDate> reservToRollBack = new HashMap<Integer, LocalDate>();

        LocalDate tempDate = LocalDate.fromYearMonthDay(rDay.getYear(), rDay.getMonthValue(), rDay.getDayOfMonth());

		Collections.shuffle(roomInfo);

		for (Room room: roomInfo) {
			if (totalSize <= size) {
				totalSize += room.size;
				reservedRooms.add(room);
			}
		}

		for (Room r: reservedRooms) {
			BoundStatement bs = new BoundStatement(SELECT_EXACT);
			bs.bind(tempDate, r.roomId);
			try {
				ResultSet rs = session.execute(bs);
				if (rs.isExhausted()) 
					freeRooms.add(r);
			} catch(Exception e) {
				throw new BackendException("Could not perform a reservation. " + e.getMessage() + ".", e);
			}		
		}

				

		if (freeRooms.size() == 0) {
			incrementCounter();
			throw new BackendException("No free rooms");
		}
		for (long daysBetween = ChronoUnit.DAYS.between(tempSDate, tempEDate); daysBetween >= 0; daysBetween--) {
			tempDate = LocalDate.fromYearMonthDay(rDay.getYear(), rDay.getMonthValue(), rDay.getDayOfMonth());
			
            rDay.plusDays(1);
		
			for (Room room: reservedRooms) {
            	reservToRollBack.put(room.roomId, tempDate);
				BoundStatement bs = new BoundStatement(UPDATE_ROOM);
				bs.bind(room.roomId, tempDate, name, room.size);
				try {
					session.execute(bs);
					logger.info("Room " + room.roomId + " reserved");
					bs = new BoundStatement(SELECT_EXACT);
					bs.bind(tempDate, room.roomId);
					ResultSet rs = session.execute(bs);
					for (Row row : rs) {
						if ((row.getString("name") == "") || (row.getString("name") != name)) { //TODO
							for (Map.Entry<Integer, LocalDate> r : reservToRollBack.entrySet()) {
								clearRoom(r.getKey(), r.getValue());
							}
							throw new BackendException("Failed");
						}
					}
				} catch (Exception e) {
					throw new BackendException("Could not perform a reservation. " + e.getMessage() + ".", e);
				}
			}
		}
	}

	public void clearRoom(int roomId, LocalDate date) throws BackendException {
		BoundStatement bs = new BoundStatement(CLEAR_ROOM);
		bs.bind(date, roomId);
		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a delete operation. " + e.getMessage() + ".", e);
		}

		logger.info("All users deleted");
	}

	protected void finalize() {
		try {
			if (session != null) {
				session.getCluster().close();
			}
		} catch (Exception e) {
			logger.error("Could not close existing cluster", e);
		}
	}

}
