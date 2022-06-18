package com.example.elasticsearch.document;

import java.util.Date;

/**
 * <p>
 * 用户文档
 * </p>
 *
 * @author wenjun
 * @since 2022/6/18
 */
public class UserDocument {

    private String id;

    private String name;

    private String sex;

    private Integer age;

    private String city;

    private Date createTime;

    public UserDocument(String name, String sex, Integer age, String city) {
        this.name = name;
        this.sex = sex;
        this.age = age;
        this.city = city;
    }

    public UserDocument() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }
}
