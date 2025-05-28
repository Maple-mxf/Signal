package signal.mongo;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.List;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MongoDriverPojoTest {

  private MongoClient client;
  private MongoDatabase db;

  @Before
  public void setup() {

    CodecRegistry codecRegistry =
        CodecRegistries.fromRegistries(
            MongoClientSettings.getDefaultCodecRegistry(),
            CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));

    client =
        MongoClients.create(
            MongoClientSettings.builder()
                
                .applyConnectionString(new ConnectionString("mongodb://127.0.0.1:5707"))
                .codecRegistry(codecRegistry)
                .build());

    db = client.getDatabase("signal");
  }

  @After
  public void after() {
    client.close();
  }

  public record Hobby(@BsonProperty("name") String name, @BsonProperty("score") int score) {}

  public record Student(
      @BsonId Integer id,
      @BsonProperty("name") String name,
      @BsonProperty("hobbies") List<Hobby> hobbies) {}

  @Test
  public void testConvert2JavaObject() {

    MongoCollection<Student> coll = db.getCollection("student", Student.class);

    FindIterable<Student> students = coll.find();

    for (Student student : students) {
      System.err.println(student);
    }
  }
}
