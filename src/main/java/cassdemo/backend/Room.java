package cassdemo.backend;

public class Room {
    int roomId;
	String startDate;
	String endDate;
	String name;
    int size;
    
    public Room(int _roomId, String _startDate, String _endDate, String _name, int _size) {
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