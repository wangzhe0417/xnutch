package org.apache.nutch.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by zhangmingke on 16/6/30.
 */
public class TypeUtils {
    public static HashSet<String> wordList = new HashSet<String>();
    public static HashSet<String> xlsList = new HashSet<String>();
    public static HashSet<String> pptList = new HashSet<String>();
    public static HashSet<String> rtfList = new HashSet<String>();
    public static HashSet<String> pdfList = new HashSet<String>();
    public static HashSet<String> textList = new HashSet<String>();
    public TypeUtils(){

        wordList.add("application/msword");
        wordList.add("application/doc");
        wordList.add("appl/text");
        wordList.add("application/vnd.msword");
        wordList.add("application/vnd.ms-word");
        wordList.add("application/winword");
        wordList.add("application/word");
        wordList.add("application/x-msw6");
        wordList.add("application/x-msword");
        wordList.add("application/vnd.openxmlformats-officedocument.wordprocessingml.document");
        wordList.add("application/vnd.openxmlformats-officedocument.wordprocessingml.template");
        wordList.add("application/vnd.ms-word.document.macroEnabled.12");
        wordList.add("application/vnd.ms-word.template.macroEnabled.12");

        xlsList.add("application/vnd.ms-excel");
        xlsList.add("application/msexcel");
        xlsList.add("application/x-msexcel");
        xlsList.add("application/x-ms-excel");
        xlsList.add("application/vnd.ms-excel");
        xlsList.add("application/x-excel");
        xlsList.add("application/x-dos_ms_excel");
        xlsList.add("application/xls");
        xlsList.add("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
        xlsList.add("application/vnd.openxmlformats-officedocument.spreadsheetml.template");
        xlsList.add("application/vnd.ms-excel.sheet.macroEnabled.12");
        xlsList.add("application/vnd.ms-excel.template.macroEnabled.12");
        xlsList.add("application/vnd.ms-excel.addin.macroEnabled.12");
        xlsList.add("application/vnd.ms-excel.sheet.binary.macroEnabled.12");

        pptList.add("application/vnd.ms-powerpoint");
        pptList.add("application/mspowerpoint");
        pptList.add("application/ms-powerpoint");
        pptList.add("application/mspowerpnt");
        pptList.add("application/vnd-mspowerpoint");
        pptList.add("application/powerpoint");
        pptList.add("application/x-powerpoint");
        pptList.add("application/x-m");

        rtfList.add("application/rtf");
        rtfList.add("application/x-rtf");
        rtfList.add("text/rtf");
        rtfList.add("text/richtext");
        rtfList.add("application/msword");
        rtfList.add("application/doc");
        rtfList.add("application/x-soffice");

        pdfList.add("application/pdf");
        pdfList.add("application/x-pdf");
        pdfList.add("application/acrobat");
        pdfList.add("applications/vnd.pdf");

        textList.add("application/xhtml+xml");
        textList.add("text/html");
        textList.add("text/plain");
        textList.add("text/asp");
        textList.add("application/x-asp");
        textList.add("application/x-asap");
        textList.add("application/x-httpd-php");
        textList.add("text/php");
        textList.add("application/php");
        textList.add("magnus-internal/shellcgi");
        textList.add("application/x-php");
        textList.add("text/xml");
        textList.add("application/xml");
        textList.add("application/x-xml");

    }
    public String getShortType(HashSet<String> type){


        String maxLength = "";
        for(String a : type){
            if(a.length() > maxLength.length())
                maxLength = a;
        }
        if(wordList.contains(maxLength))
            return "word";
        if(xlsList.contains(maxLength))
            return "xls";
        if(pptList.contains(maxLength))
            return "ppt";
        if(rtfList.contains(maxLength))
            return "rtf";
        if(pdfList.contains(maxLength))
            return "pdf";
        return null;
    }

    public Boolean isText(HashSet<String> types){
        for(String a : textList){
            if(types.contains(a))
                return true;
        }
        return false;
    }
}
