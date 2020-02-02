package cassdemo.backend;

import java.util.HashSet;
import java.util.IllegalFormatException;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import org.apache.commons.math3.util.MultidimensionalCounter.Iterator;
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
	private static PreparedStatement SELECT_GREATER_THAN_END_DATE;
	private static PreparedStatement UPDATE_ROOM;

	private void prepareStatements() throws BackendException {
		try {
			SELECT_ALL_FROM_ROOMS = session.prepare("SELECT * FROM Rooms;");
			//TODO FIX SELECT LOGIC
			SELECT_GREATER_THAN_END_DATE = session.prepare("SELECT * FROM Rooms where endDate < ? ALLOW FILTERING");
			UPDATE_ROOM = session
					//.prepare("UPDATE Rooms SET startDate=?, endDate=?, name=? WHERE roomId=?;");
					.prepare("INSERT INTO Rooms (roomId, startDate, endDate, name, size) VALUES (?, ?, ?, ?, ?)");
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

	public Set<Room> selectAll() throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_ROOMS);

		ResultSet rs = null;
		Set<Room> roomInfo = new HashSet<Room>();
		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			int roomId = row.getInt("roomId");
			LocalDate startDate = row.getDate("startDate");
			LocalDate endDate = row.getDate("endDate");
			String name = row.getString("name");
			int size = row.getInt("size");

			Room room = new Room(roomId, startDate, endDate, name, size);
			roomInfo.add(room);
		}
		return roomInfo;
	}

	public Set<Room> selectGreaterThanEndDate() throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_GREATER_THAN_END_DATE);

		ResultSet rs = null;
		Set<Room> roomInfo = new HashSet<Room>();
		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			int roomId = row.getInt("roomId");
			LocalDate startDate = row.getDate("startDate");
			LocalDate endDate = row.getDate("endDate");
			String name = row.getString("name");
			int size = row.getInt("size");

			Room room = new Room(roomId, startDate, endDate, name, size);
			roomInfo.add(room);
		}
		return roomInfo;
	}

	public boolean isDateBigger(String first, String second) throws IllegalArgumentException{
		String[] splitFirst = first.split("-");
		String[] splitSecond = second.split("-");

		if (splitFirst.length < 3 || splitSecond.length < 3) 
			throw new IllegalArgumentException();
		//Really bad if
		try {
			if (Integer.parseInt(splitFirst[0]) >= Integer.parseInt(splitSecond[0]) &&
			Integer.parseInt(splitFirst[1]) >= Integer.parseInt(splitSecond[1]) &&
			Integer.parseInt(splitFirst[2]) > Integer.parseInt(splitSecond[2]))
				return true;
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
		return false;
	}

	public void reserveRoom(LocalDate startDate, LocalDate endDate, int size, String name) throws BackendException {
		
		int totalSize = 0;
		Set<Room> roomInfo = selectAll();
		Set<Room> freeRooms = new HashSet<Room>();
		HashMap<Integer, Integer> reservedRooms = new HashMap<Integer, Integer>(); 
		
		//TO-DO: figure out how to reserve a room that is currently occupied
		for (Room room: roomInfo) {
			if (room.name == null || (isDateBigger(startDate.toString(), room.endDate.toString()))) {
				freeRooms.add(room);
				System.out.println("Room " + room.roomId + " is free");
			}
		}

		for (Room room: freeRooms) {
			if (totalSize <= size) {
				totalSize += room.size;
				reservedRooms.put(room.roomId, room.size);
				System.out.println("Room " + room.roomId + " reserved");
			}
		}

		for (Map.Entry<Integer, Integer> room: reservedRooms.entrySet()) {
			BoundStatement bs = new BoundStatement(UPDATE_ROOM);
			bs.bind(room.getKey(), startDate, endDate, name, room.getValue());
			try {
				session.execute(bs);
				logger.info("Room " + room.getKey() + " reserved");
			} catch (Exception e) {
				throw new BackendException("Could not perform a reservation. " + e.getMessage() + ".", e);
			}
		}
	}

	public void clearRoom(int roomId) throws BackendException {
		BoundStatement bs = new BoundStatement(UPDATE_ROOM);
		bs.bind("1900-00-01", "1900-00-01", "", roomId);
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
