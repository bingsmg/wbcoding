package live.clover.serialization;

import live.clover.entity.User;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

/**
 * @author weibb
 */
public class MyProducerSerializer implements Serializer<User> {

    @Override
    public byte[] serialize(String s, User user) {
        return user.toString().getBytes(StandardCharsets.UTF_8);
    }
}
