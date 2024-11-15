package kafkaApp;




import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageProcessor {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final String TARGET_TOPIC = "ComputerName"; // Целевой топик
    private static final String SOURCE_TOPIC = "computer"; // Исходный топик
    private static final long DELAY_MS = 500; // Задержка в 500 мс

    @KafkaListener(topics = SOURCE_TOPIC, groupId = "test-group")
    public void listen(String message) {
        System.out.println("Полученное сообщение: " + message);

        try {
            //  задержка перед отправкой сообщения в целевой топик
            Thread.sleep(DELAY_MS);
        } catch (Exception e) {
            System.err.println("Произошла ошибка");
        }

        // новое сообщение с префиксом
        String transformedMessage = "Computer name - " + message;

        // Отправка сообщения в целевой топик
        kafkaTemplate.send(TARGET_TOPIC, transformedMessage);
        System.out.println("Отправка сообщения в топик: " + transformedMessage);
    }
}