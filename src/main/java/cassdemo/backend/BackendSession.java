package cassdemo.backend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.sql.*;

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
	private static PreparedStatement INSERT_INTO_ROOM;

	private static final String ROOM_FORMAT = "- %-10s %-10s  %-16s %-10s %-10s\n";
	// private static final SimpleDateFormat df = new
	// SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private void prepareStatements() throws BackendException {
		try {
			SELECT_ALL_FROM_ROOMS = session.prepare("SELECT * FROM Rooms;");
			INSERT_INTO_ROOM = session
					.prepare("INSERT INTO Rooms (startDate, endDate, name) VALUES (?, ?, ?) WHERE roomId=?;");
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

	public String selectAll() throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_ROOMS);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			int roomId = row.getInt("roomId");
			Date startDate = Date.valueOf(row.getString("startDate"));
			Date endDate = Date.valueOf(row.getString("endDate"));
			String name = row.getString("name");
			int size = row.getInt("size");

			builder.append(String.format(ROOM_FORMAT, roomId, startDate, endDate, name, size));
		}

		return builder.toString();
	}

	public void reserveRoom(int roomId, String startDate, String endDate, int size, String name) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_ROOM);
		bs.bind(startDate, endDate, name, roomId);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a reservation. " + e.getMessage() + ".", e);
		}

		logger.info("Room " + roomId + " reserved");
	}

	public void clearRoom(int roomId) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_ROOM);
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
