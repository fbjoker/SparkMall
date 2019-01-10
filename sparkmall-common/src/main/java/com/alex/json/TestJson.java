package com.alex.json;

import com.alibaba.fastjson.JSONArray;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.fastjson.support.config.FastJsonConfig;
import com.alibaba.fastjson.support.spring.FastJsonHttpMessageConverter;
import com.jayway.jsonpath.JsonPath;


import com.alibaba.fastjson.JSONReader;

import javax.xml.stream.events.StartDocument;
import java.util.*;

public class TestJson {

    public static void analysisJson(Object objJson, String path,
                                    Map<String, String> map) {
       /* // 如果obj为json数组
        if (objJson instanceof JSONArray) {
            JSONArray objArray = (JSONArray) objJson;
            for (int i = 0; i < objArray.size(); i++) {
                analysisJson(objArray.get(i), path, map);
            }
        } else if (objJson instanceof JSONObject) {
            JSONObject jsonObject = (JSONObject) objJson;
            Iterator it = jsonObject.keys();
            while (it.hasNext()) {
                String key = it.next().toString();
                Object object = jsonObject.get(key);
                if (path != "" && path != null && !"".equals(path)) {
                    path += "." + key;
                } else {
                    path += key;
                }
                if (object != null && !JSONNull.getInstance().equals(object)) {
                    // 如果得到的是数组
                    if (object instanceof JSONArray) {
                        JSONArray objArray = (JSONArray) object;
                        analysisJson(objArray, path, map);
                        path = modifyPath(path);
                    } else if (object instanceof JSONObject) {
                        analysisJson((JSONObject) object, path, map);
                        path = modifyPath(path);
                    } else {
                        map.put(path, object.toString());
                        // System.out.println("["+path+"]:"+object.toString()+" ");
                        path = modifyPath(path);

                    }
                } else {
                    map.put(path, null);
                    // System.out.println("["+path+"]:"+"null");
                    path = modifyPath(path);
                }
            }
        }*/
    }

    public static void main(String[] args) {
        String data = "[{'id':73,'applyDate':'2018-03-05','reason':'123','auditState':'待审批','editable':false,'student':{'id':1,'sno':'51164500211','name':'张三','nameEn':null,'birthDate':null,'ethnic':{'id':1,'name':'汉族','code':'01'},'gender':{'id':1,'name':'男','code':'1'},'degreeLevel':{'id':1,'name':'硕士研究生','code':'1'},'grade':0,'supervisor':null,'unit':{'id':61,'division':'院系','divisionCode':'01','school':'传播学院','schoolCode':'0123','schoolType':'教学实体','department':'传播学院院部','departmentCode':'012300','departmentType':'教学虚体','since':'2016','until':null,'source':null,'target':null},'discipline':{'id':148,'category':'文学','categoryCode':'05','major':'中国语言文学','majorCode':'0501','minor':'文艺学','minorCode':'050101','originCode':'','degreeLevel':'12','degreeType':'1','area':'p','nature':'gjbzk','since':'2016','until':null,'source':null,'target':null}},'staff':null,'type':{'major':'休学','majorCode':2,'minor':'休学','minorCode':1},'fileNames':[],'applicantUserName':'admin','applicantName':'超级管理员','cTerm':'1','cEarlyGraduationTerm':null,'cStaff':null,'cTransmitDate':null,'cCategory':null,'cMajor':null,'cMinor':null,'cDepartmentCode':null,'cSchoolCode':null,'cReportId':null,'cReportName':null,'cReportLink':null,'cReportDigest':null}]";
        System.out.println(data);
//        JSONObject jsonData = JSONObject.fromObject(data.substring(1,
//                data.length() - 1));

        String data1 = "{'id':73,'applyDate':'2018-03-05','reason':'123','auditState':'待审批','editable':false,'student':{'id':1,'sno':'51164500211','name':'张三','nameEn':null,'birthDate':null,'ethnic':{'id':1,'name':'汉族','code':'01'},'gender':{'id':1,'name':'男','code':'1'},'degreeLevel':{'id':1,'name':'硕士研究生','code':'1'},'grade':0,'supervisor':null,'unit':{'id':61,'division':'院系','divisionCode':'01','school':'传播学院','schoolCode':'0123','schoolType':'教学实体','department':'传播学院院部','departmentCode':'012300','departmentType':'教学虚体','since':'2016','until':null,'source':null,'target':null},'discipline':{'id':148,'category':'文学','categoryCode':'05','major':'中国语言文学','majorCode':'0501','minor':'文艺学','minorCode':'050101','originCode':'','degreeLevel':'12','degreeType':'1','area':'p','nature':'gjbzk','since':'2016','until':null,'source':null,'target':null}},'staff':null,'type':{'major':'休学','majorCode':2,'minor':'休学','minorCode':1},'fileNames':[],'applicantUserName':'admin','applicantName':'超级管理员','cTerm':'1','cEarlyGraduationTerm':null,'cStaff':null,'cTransmitDate':null,'cCategory':null,'cMajor':null,'cMinor':null,'cDepartmentCode':null,'cSchoolCode':null,'cReportId':null,'cReportName':null,'cReportLink':null,'cReportDigest':null}";
        //data和data1的区别就是少了个[]
        //转换为json对象的时候空字符串会丢失,但是key还在,可以在转换jsonstr的时候设置SerializerFeature.WriteMapNullValue 还原
        JSONObject jsonObject = JSONObject.parseObject(data1);
        System.out.println(jsonObject);
        //由于map是无需的,需要转换为有序的要加下面这个参数
        LinkedHashMap linkedHashMap = JSONObject.parseObject(data1, LinkedHashMap.class, Feature.OrderedField);
        System.out.println(linkedHashMap);


        //向要不过滤掉null,需要这样解析json对象
//        QuoteFieldNames———-输出key时是否使用双引号,默认为true
//        WriteMapNullValue——–是否输出值为null的字段,默认为false
//        WriteNullNumberAsZero—-数值字段如果为null,输出为0,而非null
//        WriteNullListAsEmpty—–List字段如果为null,输出为[],而非null
//        WriteNullStringAsEmpty—字符类型字段如果为null,输出为”“,而非null
//        WriteNullBooleanAsFalse–Boolean字段如果为null,输出为false,而非null
        String str = JSONObject.toJSONString(jsonObject, SerializerFeature.WriteMapNullValue);
        System.out.println(str);

        String data3 = "[{'id':73,'applyDate':'2018-03-05','reason':'123','auditState':'待审批','editable':false,'student':{'id':1,'sno':'51164500211','name':'张三','nameEn':null,'birthDate':null,'ethnic':{'id':1,'name':'汉族','code':'01'},'gender':{'id':1,'name':'男','code':'1'},'degreeLevel':{'id':1,'name':'硕士研究生','code':'1'},'grade':0,'supervisor':null,'unit':{'id':61,'division':'院系','divisionCode':'01','school':'传播学院','schoolCode':'0123','schoolType':'教学实体','department':'传播学院院部','departmentCode':'012300','departmentType':'教学虚体','since':'2016','until':null,'source':null,'target':null},'discipline':{'id':148,'category':'文学','categoryCode':'05','major':'中国语言文学','majorCode':'0501','minor':'文艺学','minorCode':'050101','originCode':'','degreeLevel':'12','degreeType':'1','area':'p','nature':'gjbzk','since':'2016','until':null,'source':null,'target':null}},'staff':null,'type':{'major':'休学','majorCode':2,'minor':'休学','minorCode':1},'fileNames':[],'applicantUserName':'admin','applicantName':'超级管理员','cTerm':'1','cEarlyGraduationTerm':null,'cStaff':null,'cTransmitDate':null,'cCategory':null,'cMajor':null,'cMinor':null,'cDepartmentCode':null,'cSchoolCode':null,'cReportId':null,'cReportName':null,'cReportLink':null,'cReportDigest':null}]";

        JSONArray jsonArray = JSONArray.parseArray(data3);
        //System.out.println(jsonArray);

        //获取学生的名字
        String studentname = JsonPath.read(jsonObject, "$.student.name");
        System.out.println(studentname);
        //获取所有的名字
        List<String> allname = JsonPath.read(jsonObject, "$..name");
        System.out.println(allname);




    }

}



