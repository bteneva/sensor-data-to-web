package bg.environment.monitoring.model;

import java.time.LocalDateTime;

public class AirDataPoint {
    public LocalDateTime timestamp;
    public double pm1;
    public double pm25;
    public double pm10;
    //public double so2;
    //public double no2;

    public AirDataPoint(LocalDateTime timestamp, double pm1, double pm25, double pm10) {
        this.timestamp = timestamp;
        this.pm1 = pm1;
        this.pm25 = pm25;
        this.pm10 = pm10;
        //this.so2 = so2;
        //this.no2 = no2;
    }
}
