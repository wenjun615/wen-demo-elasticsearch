package com.example.elasticsearch;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

/**
 * <p>
 * 搜索测试
 * </p>
 *
 * @author wenjun
 * @since 2022/6/18
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = ElasticsearchApplication.class)
public class SearchTest {

    @Autowired
    private RestHighLevelClient client;

    /**
     * 单条件精确查询
     */
    @Test
    public void search0() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(QueryBuilders.termsQuery("name", "赵里"));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 多条件精确查询，取并集
     */
    @Test
    public void search1() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(QueryBuilders.termsQuery("name", "张", "陈"));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 范围查询，包括 from、to
     */
    @Test
    public void search2() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(QueryBuilders.rangeQuery("age").from(20).to(32));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 范围查询，不包括 from、to
     */
    @Test
    public void search3() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(QueryBuilders.rangeQuery("age").from(20, false).to(30, false));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 范围查询 lt 小于 gt 大于
     */
    @Test
    public void search4() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(QueryBuilders.rangeQuery("age").lt(30).gt(20));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 模糊查询，支持通配符
     */
    @Test
    public void search5() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(QueryBuilders.wildcardQuery("name", "张三"));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 不使用通配符的模糊查询，左右匹配
     */
    @Test
    public void search6() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(QueryBuilders.queryStringQuery("张三").field("name"));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 多字段模糊查询
     */
    @Test
    public void search7() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(QueryBuilders.multiMatchQuery("长", "name", "city"));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 分页搜索
     */
    @Test
    public void search8() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder().from(0).size(2);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 排序，字段的类型必须是：integer、double、long或者keyword
     */
    @Test
    public void search9() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder().sort("createTime", SortOrder.ASC);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 精确统计筛选文档数，查询性能有所降低
     */
    @Test
    public void search10() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder().trackTotalHits(true);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 设置源字段过滤，第一个参数表示结果集包括哪些字段，第二个参数表示结果集不包括哪些字段
     */
    @Test
    public void search11() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder()
                .fetchSource(new String[]{"name", "age", "city", "createTime"}, new String[]{});
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 根据 id 精确匹配
     */
    @Test
    public void search12() throws IOException {
        String[] ids = new String[]{"1", "2"};
        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(QueryBuilders.termsQuery("_id", ids));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 搜索全部
     */
    @Test
    public void search21() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(QueryBuilders.matchAllQuery());
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * match 搜索匹配
     */
    @Test
    public void search22() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("name", "张王"));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * bool 组合查询
     */
    @Test
    public void search23() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(QueryBuilders.matchQuery("name", "张王"));
        boolQueryBuilder.must(QueryBuilders.rangeQuery("age").lte(30).gte(20));
        builder.query(boolQueryBuilder);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * nested 类型嵌套查询
     */
    @Test
    public void search24() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder mainBool = new BoolQueryBuilder();
        mainBool.must(QueryBuilders.matchQuery("name", "赵六"));
        // nested 类型嵌套查询
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(QueryBuilders.matchQuery("products.brand", "A"));
        boolQueryBuilder.must(QueryBuilders.matchQuery("products.title", "巧克力"));
        NestedQueryBuilder nested = QueryBuilders.nestedQuery("products", boolQueryBuilder, ScoreMode.None);
        mainBool.must(nested);
        builder.query(mainBool);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 多条件查询 + 排序 + 分页
     */
    @Test
    public void search29() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(QueryBuilders.matchQuery("name", "张王"));
        boolQueryBuilder.must(QueryBuilders.rangeQuery("age").lte(30).gte(20));
        builder.query(boolQueryBuilder);
        builder.from(0).size(2);
        builder.sort("createTime", SortOrder.ASC);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 聚合查询 avg
     */
    @Test
    public void search31() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        AggregationBuilder aggregation = AggregationBuilders.avg("avg_age").field("age");
        builder.aggregation(aggregation);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 聚合查询 count
     */
    @Test
    public void search32() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        AggregationBuilder aggregation = AggregationBuilders.count("count_age").field("age");
        builder.aggregation(aggregation);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    /**
     * 聚合查询 分组
     */
    @Test
    public void search33() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        AggregationBuilder aggregation = AggregationBuilders.terms("tag_createTime").field("createTime")
                .subAggregation(AggregationBuilders.count("count_age").field("age"))
                .subAggregation(AggregationBuilders.sum("sum_age").field("age"))
                .subAggregation(AggregationBuilders.avg("avg_age").field("age"));
        builder.aggregation(aggregation);
        // 不输出原始数据
        builder.size(0);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cs_index");
        searchRequest.types("_doc");
        searchRequest.source(builder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

}
