package bg.environment.monitoring.engine;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PhpNodeEngine {
    private static final String DB_URL = System.getenv("RAILWAY_ENVIRONMENT") != null
            ? "jdbc:sqlite:/app/data/air_quality.db"
            : "jdbc:sqlite:air_quality.db";
    private static final ZoneId BG_ZONE = ZoneId.of("Europe/Sofia");

    private record PhpStation(String name, String dataUrl) {}

    private static final List<PhpStation> STATIONS = List.of(
            new PhpStation("Баланово (N330)", "https://meter.ac/gs/nodes/N330/data.raw.php"),
            new PhpStation("Големо село 2 (N321)", "https://meter.ac/gs/nodes/N321/data.raw.php"),
            new PhpStation("Шатрово (N329)", "https://meter.ac/gs/nodes/N329/data.raw.php")
    );

    private final Runnable onNewDataCallback;
    private final HttpClient client = HttpClient.newHttpClient();

    public PhpNodeEngine(Runnable onNewDataCallback) {
        this.onNewDataCallback = onNewDataCallback;
    }

    public void checkAndProcess() {
        String sofiaTime = getSofiaTime();
        boolean anyNew = false;

        // 1. Вземаме ТЕКУЩИЯ вятър само веднъж, преди цикъла
        SensorDataToWeb.WeatherSnapshot currentWind = SensorDataToWeb.fetchCurrentWindAndTemp();
        long nowUnix = Instant.now().getEpochSecond();

        for (PhpStation station : STATIONS) {
            try {
                System.out.println(sofiaTime + " ⏱️  [PhpNodeEngine] Изтегляне на данни за " + station.name() + "...");
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(station.dataUrl()))
                        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                        .GET()
                        .build();

                HttpResponse<Stream<String>> response = client.send(request, HttpResponse.BodyHandlers.ofLines());

                if (response.statusCode() == 200) {
                    List<String> allLines;
                    try (Stream<String> stream = response.body()) {
                        allLines = stream.filter(line -> line != null && !line.trim().isEmpty()
                                        && !line.contains("time") && !line.contains("pm"))
                                .collect(Collectors.toList());
                    }

                    if (!allLines.isEmpty()) {
                        // 2. Отваряме ЕДНА връзка към базата за ВСИЧКИ редове на станцията
                        if (saveStationDataBatch(allLines, station.name(), currentWind, nowUnix)) {
                            anyNew = true;
                        }
                    }
                } else {
                    System.err.println("❌ [PhpNodeEngine] " + station.name() + " върна грешка: " + response.statusCode());
                }
            } catch (Exception e) {
                System.err.println("❌ [PhpNodeEngine] Критична грешка при " + station.name() + ": " + e.getMessage());
                e.printStackTrace();
            }
        }

        if (anyNew && onNewDataCallback != null) {
            onNewDataCallback.run();
        }
    }

    public static String getSofiaTime() {
        String sofiaTime = ZonedDateTime.now(BG_ZONE).format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        return sofiaTime;
    }

    private boolean saveStationDataBatch(List<String> lines, String stationName, SensorDataToWeb.WeatherSnapshot currentWind, long nowUnix) {
        String sql = "INSERT INTO pollution(timestamp, pm1, pm25, pm10, temp, humidity, pressure, station_name, wind_dir, wind_speed, wind_degree) " +
                "VALUES(?,?,?,?,?,?,?,?,?,?,?) " +
                "ON CONFLICT(timestamp, station_name) DO UPDATE SET " +
                "wind_dir = excluded.wind_dir, " +
                "wind_speed = excluded.wind_speed, " +
                "wind_degree = excluded.wind_degree " +
                "WHERE pollution.wind_degree IS NULL";

        boolean insertedAny = false;

        try (Connection conn = DriverManager.getConnection(DB_URL);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);

            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i).trim();
                if (line.isEmpty() || line.contains("Signal") || line.contains("Unix")) continue;

                // Разделяме стриктно по запетая
                String[] words = line.split(",");

                // Според структурата имаме точно 8 елемента (индекси от 0 до 7)
                if (words.length <= 8) continue;

                try {
                    // Извличаме Unix времето от индекс 7 и го зачистваме от интервали
                    long unixTime = Long.parseLong(words[7].trim());
                    LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochSecond(unixTime), BG_ZONE);
                    String formattedTime = time.toString().replace("T", " ");

                    // Парсваме останалите елементи спрямо точните им позиции в CSV-то
                    double pm25     = Double.parseDouble(words[1].trim());
                    double pm10     = Double.parseDouble(words[2].trim());
                    double pressure = Double.parseDouble(words[3].trim());
                    double temp     = Double.parseDouble(words[4].trim());
                    double humidity = Double.parseDouble(words[5].trim());

                    // Запълваме PreparedStatement
                    pstmt.setString(1, formattedTime);
                    pstmt.setDouble(2, 0.0); // Нямаме PM1 в този файл, оставяме 0.0
                    pstmt.setDouble(3, pm25);
                    pstmt.setDouble(4, pm10);
                    pstmt.setDouble(5, temp);
                    pstmt.setDouble(6, humidity);
                    pstmt.setDouble(7, pressure);
                    pstmt.setString(8, stationName);

                    // Записваме вятъра само за текущи записи (до 15 мин) или за най-последния ред
                    boolean isRecent = (nowUnix - unixTime) < 900;
                    boolean isLastLine = (i == lines.size() - 1);

                    if (isRecent || isLastLine) {
                        SensorDataToWeb.bindWind(pstmt, 9, currentWind);
                    } else {
                        pstmt.setNull(9, java.sql.Types.VARCHAR);
                        pstmt.setNull(10, java.sql.Types.DOUBLE);
                        pstmt.setNull(11, java.sql.Types.DOUBLE);
                    }

                    pstmt.addBatch();

                } catch (NumberFormatException nfe) {
                    // Ако има дефектен или празен елемент, прескачаме само този ред
                    continue;
                }
            }

            int[] results = pstmt.executeBatch();
            conn.commit();

            for (int res : results) {
                if (res > 0 || res == PreparedStatement.SUCCESS_NO_INFO) {
                    insertedAny = true;
                }
            }

        } catch (Exception e) {
            System.err.println("❌ [PhpNodeEngine] Критична грешка при запис за " + stationName + ": " + e.getMessage());
        }

        return insertedAny;
    }

}
