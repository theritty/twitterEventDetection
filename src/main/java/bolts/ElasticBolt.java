package bolts;
/*
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;


public class ElasticBolt {

    public static Client client;
    public static Node node;

    public static void createClient( ) throws IOException {
        node = nodeBuilder()
                .settings(Settings
                        .settingsBuilder()
                        .put("http.enabled", false)
                        .put("path.home", "/home/ceren/Desktop/elasticsearch-2.2.0/"))
                .clusterName("elasticsearch")
                .client(true)
                .node();

//        node = nodeBuilder().clusterName("elasticsearch").node();
        client = node.client();
    }

    public static void closeClient( ) throws IOException {
        client.close();
        node.close();
    }
    public static void createIndex(String indexName, String documentType, String documentId) throws IOException {
        //Create Index and set settings and mappings
        IndexResponse response = client.prepareIndex(indexName, documentType, documentId)
                .setSource(jsonBuilder()
                        .startObject()
                        .field("user", "test_user_2")
                        .field("postDate", new Date())
                        .field("message", "dasddasd out Elasticsearch - 2")
                        .endObject()
                )
                .get();
        System.out.println("write " + response);
    }

    public static void getDocument(String indexName, String documentType, String documentId) {
        //Get document
        GetResponse response = client.prepareGet(indexName, documentType, documentId)
                .setOperationThreaded(false)
                .get();
        System.out.println("read " + response);
    }

    public static void main(String[] args)
    {
        try {
            createClient();
            createIndex("twitter", "tweet", "2");
            getDocument( "twitter", "tweet", "2");
            closeClient();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
*/