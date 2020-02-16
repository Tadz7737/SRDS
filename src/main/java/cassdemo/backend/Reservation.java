package cassdemo.backend;

import com.datastax.driver.core.LocalDate;

public class Reservation {
    LocalDate startDate;

    public Reservation(LocalDate _startDate,  String _name) {
        startDate = _startDate;
    }
}