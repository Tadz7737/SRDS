package cassdemo.backend;

import com.datastax.driver.core.LocalDate;

public class Reservation {
    LocalDate startDate;
    LocalDate endDate;

    public Reservation(LocalDate _startDate, LocalDate _endDate, String _name) {
        startDate = _startDate;
        endDate = _endDate;
    }
}