package cassdemo.backend;

import com.datastax.driver.core.LocalDate;

public class Room {
    int roomId;
	LocalDate startDate;
	LocalDate endDate;
	String name;
    int size;
    
    public Room(int _roomId, LocalDate _startDate, LocalDate _endDate, String _name, int _size) {
        roomId = _roomId;
        startDate = _startDate;
        endDate = _endDate;
        size = _size;
    }
    @Override
    public String toString() {
        return "roomId : " + roomId;
    }
}