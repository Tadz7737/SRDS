package cassdemo.backend;

import java.sql.Date;
import java.util.HashSet;
import com.datastax.driver.core.LocalDate;

import cassdemo.backend.Reservation;

public class Room {
    int roomId;
	String name;
    int size;
    Reservation reservation;

    boolean isRoomFreeAtDate(LocalDate reservDate) {
        Date first = new Date(reservDate.getMillisSinceEpoch());
        Date second = new Date(reservation.startDate.getMillisSinceEpoch());
        if (reservation.startDate.getYear() == 1900) 
            return true;
        if (first.after(second)) {
            return false;
        }
        return true;
    }

    public Room(int _roomId, int _size) {
        roomId = _roomId;
        size = _size;
    }

    @Override
    public String toString() {
        return "roomId : " + roomId;
    }
}