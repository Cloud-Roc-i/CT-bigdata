package com.ct.web.service.impl;

import com.ct.web.bean.Calllog;
import com.ct.web.dao.CalllogDao;
import com.ct.web.service.CalllogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 通话日志服务对象
 */
@Service
public class CalllogServiceImpl implements CalllogService {

    @Autowired
    private CalllogDao calllogDao;

    //查询指定时间的通话统计信息
    @Override
    public List<Calllog> queryMonthDatas(String tel, String calltime) {

        Map<String,Object> paramMap = new HashMap<>();
        paramMap.put("tel",tel);
        if(calltime.length() > 4){
            calltime = calltime.substring(0,4);
        }
        paramMap.put("year",calltime);
        return calllogDao.queryMonthDatas(paramMap);
    }
}
