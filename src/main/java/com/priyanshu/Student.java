package com.priyanshu;

import java.io.Serializable;

public class Student implements Serializable {
    public String id;
    public String name;
    public String marks;

    public Student() {
    }

    public Student(String id, String name, String marks) {
        this.id = id;
        this.name = name;
        this.marks = marks;
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

    public String getMarks() {
        return marks;
    }

    public void setMarks(String marks) {
        this.marks = marks;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", marks='" + marks + '\'' +
                '}';
    }
}
