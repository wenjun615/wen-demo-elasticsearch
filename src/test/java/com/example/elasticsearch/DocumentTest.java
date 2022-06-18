package com.example.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.example.elasticsearch.document.UserDocument;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * <p>
 * 文档测试
 * </p>
 *
 * @author wenjun
 * @since 2022/6/18
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = ElasticsearchApplication.class)
public class DocumentTest {

    @Autowired
    private RestHighLevelClient client;

    /**
     * 添加文档
     */
    @Test
    public void addDocument() throws IOException {
        UserDocument user = new UserDocument();
        user.setId("1");
        user.setName("里斯");
        user.setCity("武汉");
        user.setSex("男");
        user.setAge(20);
        user.setCreateTime(new Date());
        IndexRequest request = new IndexRequest();
        request.id("1");
        request.index("cs_index");
        request.type("_doc");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.source(JSON.toJSONString(user), XContentType.JSON);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 更新文档
     */
    @Test
    public void updateDocument() throws IOException {
        UserDocument user = new UserDocument();
        user.setId("2");
        user.setName("程咬金");
        user.setCreateTime(new Date());
        UpdateRequest request = new UpdateRequest();
        request.id("2");
        request.index("cs_index");
        request.type("_doc");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 删除文档
     */
    @Test
    public void deleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest();
        request.id("1");
        request.index("cs_index");
        request.type("_doc");
        request.timeout(TimeValue.timeValueSeconds(1));
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 查询文档是不是存在
     */
    @Test
    public void exists() throws IOException {
        GetRequest request = new GetRequest();
        request.id("3");
        request.index("cs_index");
        request.type("_doc");
        boolean response = client.exists(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    /**
     * 根据 id 查询指定文档
     */
    @Test
    public void getById() throws IOException {
        GetRequest request = new GetRequest();
        request.id("1");
        request.index("cs_index");
        request.type("_doc");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 批量添加文档
     */
    @Test
    public void batchAddDocument() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout(TimeValue.timeValueSeconds(10));
        List<UserDocument> userArrayList = new ArrayList<>();
        userArrayList.add(new UserDocument("张三", "男", 30, "武汉"));
        userArrayList.add(new UserDocument("里斯", "女", 31, "北京"));
        userArrayList.add(new UserDocument("王五", "男", 32, "武汉"));
        userArrayList.add(new UserDocument("赵六", "女", 33, "长沙"));
        userArrayList.add(new UserDocument("七七", "男", 34, "武汉"));
        TimeValue timeValue = TimeValue.timeValueSeconds(1);
        // 添加请求
        for (int i = 0; i < userArrayList.size(); i++) {
            String id = String.valueOf(i);
            userArrayList.get(i).setId(id);
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.id(id);
            indexRequest.index("cs_index");
            indexRequest.type("_doc");
            indexRequest.timeout(timeValue);
            indexRequest.source(JSON.toJSONString(userArrayList.get(i)), XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        // 执行请求
        BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }
}
