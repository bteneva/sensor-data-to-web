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
import java.util.stream.Stream;

public class PhpNodeEngine {
    private static final String DB_URL = "jdbc:sqlite:air_quality.db";
    private static final String DATA_URL = "https://meter.ac/gs/nodes/N321/data.raw.php";
    private static final ZoneId BG_ZONE = ZoneId.of("Europe/Sofia");

    // Референция към метода за експорт на основния клас, за да обновим live.json при нова точка
    private final Runnable onNewDataCallback;

    public PhpNodeEngine(Runnable onNewDataCallback) {
        this.onNewDataCallback = onNewDataCallback;
    }

    public void checkAndProcess() {
        try {
            System.out.println("⏱️ [PhpNodeEngine] Проверка на .raw.php за ново 5-минутно измерване...");
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(DATA_URL))
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                    .GET()
                    .build();

            HttpResponse<Stream<String>> response = client.send(request, HttpResponse.BodyHandlers.ofLines());

            if (response.statusCode() == 200) {
                try (Stream<String> lines = response.body()) {
                    // Хващаме само последната (най-новата) линия от PHP файла
                    String lastLine = lines.reduce((first, second) -> second).orElse(null);

                    if (lastLine != null && !lastLine.trim().isEmpty()) {
                        String[] words = lastLine.trim().split(",|\\s+");

                        // Проверяваме дали редът е валиден и не е заглавие
                        if (words.length >= 9 && !lastLine.contains("time") && !lastLine.contains("pm")) {
                            savePoint(words);
                        }
                    }
                }
            } else {
                System.err.println("❌ [PhpNodeEngine] Сървърът върна грешка: " + response.statusCode());
            }
        } catch (Exception e) {
            System.err.println("❌ [PhpNodeEngine] Критична грешка при обработка: " + e.getMessage());
        }
    }

    private void savePoint(String[] words) {
        String sql = "INSERT OR IGNORE INTO pollution(timestamp, pm1, pm25, pm10, temp, humidity, pressure, station_name, wind_dir) VALUES(?,?,?,?,?,?,?,?,?)";

        try {
            long unixTime = Long.parseLong(words[7].trim());
            LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochSecond(unixTime), BG_ZONE);
            String formattedTime = time.toString().replace("T", " ");

            double pm25 = Double.parseDouble(words[1].trim());
            double pm10 = Double.parseDouble(words[2].trim());
            double pressure = Double.parseDouble(words[3].trim());
            double temp = Double.parseDouble(words[4].trim());
            double humidity = Double.parseDouble(words[5].trim());

            // Взимаме вятъра от новото работещо уеб API
            String currentWind = SensorDataToWeb.fetchCurrentWindDirection();

            try (Connection conn = DriverManager.getConnection(DB_URL);
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {

                pstmt.setString(1, formattedTime);
                pstmt.setDouble(2, 0.0);
                pstmt.setDouble(3, pm25);
                pstmt.setDouble(4, pm10);
                pstmt.setDouble(5, temp);
                pstmt.setDouble(6, humidity);
                pstmt.setDouble(7, pressure);
                pstmt.setString(8, "Големо село 2 (PHP)");
                pstmt.setString(9, currentWind); // Записваме вятъра

                int rowsAffected = pstmt.executeUpdate();
                if (rowsAffected > 0 && onNewDataCallback != null) {
                    onNewDataCallback.run();
                }
            }
        } catch (Exception e) {
            System.err.println("❌ [PhpNodeEngine] Грешка при парсване: " + e.getMessage());
        }
    }



}

