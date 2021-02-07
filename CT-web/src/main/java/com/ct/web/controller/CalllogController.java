package com.ct.web.controller;

import com.ct.web.bean.Calllog;
import com.ct.web.service.CalllogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 通话日志控制器对象
 */
@Controller
public class CalllogController {
    @Autowired
    private CalllogService calllogService;

    @RequestMapping("/query")
    public String Query(){
        return "query";
    }

    //@ResponseBody
    @RequestMapping("/view")
    public String view(String tel, String calltime, Model model){
        //查询统计结果
        List<Calllog> logs = calllogService.queryMonthDatas (tel,calltime);
        model.addAttribute("calllogs",logs);
        return "view";
    }
}
